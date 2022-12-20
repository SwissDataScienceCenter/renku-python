# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Merge strategies."""

import os
import shutil
from json import JSONDecodeError
from pathlib import Path
from tempfile import mkdtemp
from typing import List, NamedTuple, Optional, Union, cast

from BTrees.OOBTree import BTree, Bucket, TreeSet
from deepdiff import DeepDiff
from persistent import Persistent
from persistent.list import PersistentList
from zc.relation.catalog import Catalog

from renku.core import errors
from renku.core.constant import DATABASE_PATH, RENKU_HOME
from renku.core.util import communication
from renku.domain_model.dataset import Dataset, Url
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.plan import AbstractPlan
from renku.infrastructure.database import Database, Index
from renku.infrastructure.repository import Repository
from renku.version import __version__

RemoteEntry = NamedTuple(
    "RemoteEntry", [("reference", str), ("database", Database), ("path", Path), ("repository", Repository)]
)


class GitMerger:
    """Git metadata merger."""

    def merge(self, local: Path, remote: Path, base: Path) -> None:
        """Merge two renku metadata entries together."""
        repository = project_context.repository
        self.remote_entries: List[RemoteEntry] = []

        self._setup_worktrees(repository)

        merged = False
        self.local_database = project_context.database

        try:
            local_object = self.local_database.get_from_path(str(project_context.path / local))
            try:
                base_object: Optional[Persistent] = self.local_database.get_from_path(str(project_context.path / base))
            except (errors.ObjectNotFoundError, JSONDecodeError):
                base_object = None

            for entry in self.remote_entries:
                # NOTE: Loop through all remote merge branches (Octo merge) and try to merge them
                try:
                    self.remote_database = entry.database
                    remote_object = self.remote_database.get_from_path(str(project_context.path / remote))

                    # NOTE: treat merge result as new local for subsequent merges
                    local_object = self.merge_objects(local_object, remote_object, base_object)
                    merged = True
                except errors.ObjectNotFoundError:
                    continue
        finally:
            # NOTE: cleanup worktrees
            for entry in self.remote_entries:
                repository.remove_worktree(entry.path)
                shutil.rmtree(entry.path, ignore_errors=True)

        if not merged:
            raise errors.MetadataMergeError("Couldn't merge metadata: remote object not found in merge branches.")

        self.local_database.persist_to_path(local_object, local)

    def _setup_worktrees(self, repository):
        """Setup git worktrees for the remote branches."""

        # NOTE: Get remote branches
        remote_branches = [os.environ[k] for k in os.environ.keys() if k.startswith("GITHEAD")]

        database_path = Path(RENKU_HOME) / DATABASE_PATH

        for remote_branch in remote_branches:
            # NOTE: Create a new shallow worktree for each remote branch, could be several in case of an octo merge
            worktree_path = Path(mkdtemp())
            repository.create_worktree(worktree_path, reference=remote_branch, checkout=False)
            try:
                remote_repository = Repository(worktree_path)
                remote_repository.checkout(sparse=[database_path])

                self.remote_entries.append(
                    RemoteEntry(
                        remote_branch,
                        Database.from_path(worktree_path / database_path),
                        worktree_path,
                        remote_repository,
                    )
                )
            except Exception:
                # NOTE: cleanup worktree
                try:
                    repository.remove_worktree(worktree_path)
                except Exception:
                    pass
                raise

    def merge_objects(self, local: Persistent, remote: Persistent, base: Optional[Persistent]) -> Persistent:
        """Merge two database objects."""
        if type(local) != type(remote):
            raise errors.MetadataMergeError(f"Cannot merge {local} and {remote}: disparate types.")
        if isinstance(local, (BTree, Index, Bucket)):
            return self.merge_btrees(local, remote)
        elif isinstance(local, TreeSet):
            return self.merge_treesets(local, remote)
        elif isinstance(local, Catalog):
            return self.merge_catalogs(local, remote)
        elif isinstance(local, Project):
            return self.merge_projects(local, remote, cast(Optional[Project], base))
        else:
            raise errors.MetadataMergeError(
                f"Cannot merge {local} and {remote}: type not supported for automated merge."
            )

    def merge_btrees(
        self, local: Union[BTree, Index, Bucket], remote: Union[BTree, Index, Bucket]
    ) -> Union[BTree, Index, Bucket]:
        """Merge two BTrees."""
        local_key_ids = {k: getattr(v, "_p_oid", None) for k, v in local.items()}
        remote_key_ids = {k: getattr(v, "_p_oid", None) for k, v in remote.items()}

        common_modified_keys = [k for k, i in local_key_ids.items() if remote_key_ids.get(k, i) != i]
        new_remote_keys = [k for k in remote_key_ids.keys() if k not in local_key_ids]

        for new_remote in new_remote_keys:
            # NOTE: New entries in remote, we can just copy them over
            local[new_remote] = remote[new_remote]

        for common_key in common_modified_keys:
            # NOTE: Merge conflicts!
            local_object = local[common_key]
            remote_object = remote[common_key]

            comparison = self._compare_objects(local_object, remote_object)

            if comparison is not None:
                local[common_key] = comparison
                continue

            local_object._p_activate()
            remote_object._p_activate()
            diff = DeepDiff(local_object, remote_object, exclude_types=[Database])
            pretty_diff = diff.pretty().replace("Value of root.", "local.")
            pretty_diff = "\n".join(f"\t{line}" for line in pretty_diff.splitlines())
            entry_type = str(type(local_object)).split(".")[-1][:-2]

            action = communication.prompt(
                "Merge conflict detected:\n"
                f"{common_key} ({entry_type}) modified in local and remote branch.\n"
                f"Changes between local and remote:\n{pretty_diff}\n"
                "Which do you want to keep?\n[l]ocal, [r]emote, [a]bort:",
                default="a",
            )

            if action == "r":
                local[common_key] = remote_object
            elif action == "a":
                raise errors.MetadataMergeError("Merge aborted")
            elif action != "l":
                raise errors.MetadataMergeError(f"Invalid merge option selected: {action}")

        return local

    def merge_treesets(self, local: TreeSet, remote: TreeSet) -> TreeSet:
        """Merge two TreeSets."""
        local.update([e for e in remote if e not in local])
        return local

    def merge_indices(self, local: Index, remote: Index) -> Index:
        """Merge two Indices."""
        local_key_ids = {k: getattr(v, "_p_oid", None) for k, v in local.items()}
        remote_key_ids = {k: getattr(v, "_p_oid", None) for k, v in remote.items()}

        common_modified_keys = [k for k, i in local_key_ids.items() if remote_key_ids.get(k, i) != i]
        new_remote_keys = [k for k in remote_key_ids.keys() if k not in local_key_ids]

        for new_remote in new_remote_keys:
            # NOTE: New entries in remote, we can just copy them over
            local.add(remote.get(new_remote))

        for common_key in common_modified_keys:
            # NOTE: Merge conflicts!
            local_object = local.get(common_key)
            remote_object = remote.get(common_key)

            comparison = self._compare_objects(local_object, remote_object)

            if comparison is not None:
                local[common_key] = comparison
                continue

            local_object._p_activate()
            remote_object._p_activate()
            diff = DeepDiff(local_object, remote_object, exclude_types=[Database])
            pretty_diff = diff.pretty().replace("Value of root.", "local.")
            pretty_diff = "\n".join(f"\t{line}" for line in pretty_diff.splitlines())
            entry_type = str(type(local.get(common_key))).split(".")[-1][:-2]

            action = communication.prompt(
                "Merge conflict detected:\n"
                f"{common_key} ({entry_type}) modified in local and remote branch.\n"
                f"Changes between local and remote:\n{pretty_diff}\n"
                "Which do you want to keep?\n[l]ocal, [r]emote, [a]bort:",
                default="a",
            )

            if action == "r":
                local.pop(common_key)
                local.add(remote_object)
            elif action == "a":
                raise errors.MetadataMergeError("Merge aborted")
            elif action != "l":
                raise errors.MetadataMergeError(f"Invalid merge option selected: {action}")

        return local

    def merge_catalogs(self, local: Catalog, remote: Catalog) -> Catalog:
        """Merge two Catalogs."""
        for key, value in remote._EMPTY_name_TO_relcount_relset.items():
            if key not in local._EMPTY_name_TO_relcount_relset:
                local._EMPTY_name_TO_relcount_relset[key] = value

        for key, value in remote._name_TO_mapping.items():
            if key not in local._name_TO_mapping:
                local._name_TO_mapping[key] = value
                continue
            for subkey, subvalue in value.items():
                if subkey not in local._name_TO_mapping[key]:
                    local._name_TO_mapping[key][subkey] = subvalue

        for key, value in remote._reltoken_name_TO_objtokenset.items():
            if key not in local._reltoken_name_TO_objtokenset:
                local._reltoken_name_TO_objtokenset[key] = value

        return local

    def merge_projects(self, local: Project, remote: Project, base: Optional[Project]) -> Project:
        """Merge two Project entries."""

        local_changed = (
            base is None
            or local.keywords != base.keywords
            or local.description != base.description
            or local.annotations != base.annotations
        )
        local_template_changed = base is None or local.template_metadata != base.template_metadata
        remote_changed = (
            base is None
            or remote.keywords != base.keywords
            or remote.description != base.description
            or remote.annotations != base.annotations
        )
        remote_template_changed = base is None or remote.template_metadata != base.template_metadata

        if (local_changed or local_template_changed) and not remote_changed and not remote_template_changed:
            return local
        elif not local_changed and not local_template_changed and (remote_changed or remote_template_changed):
            return remote

        if local_changed or remote_changed:
            # NOTE: Merge keywords
            if local.keywords != remote.keywords:
                if base is None:
                    local.keywords = list(set(local.keywords) | set(remote.keywords))
                elif local.keywords != base.keywords and remote.keywords != base.keywords:
                    removed = (set(base.keywords) - set(local.keywords)) | (set(base.keywords) - set(remote.keywords))
                    added = (set(local.keywords) - set(base.keywords)) | (set(remote.keywords) - set(base.keywords))
                    existing = set(base.keywords) - removed
                    local.keywords = list(added | existing)
                elif remote.keywords != base.keywords:
                    local.keywords = remote.keywords

            # NOTE: Merge description
            if local.description != remote.description:

                if base is None or (local.description != base.description and remote.description != base.description):
                    local.description = communication.prompt(
                        f"Project description was modified in local and remote branch.\n"
                        f"local: {local.description}\nremote: {remote.description}\nEnter merged description: ",
                        default=local.description,
                    )
                elif remote.description != base.description:
                    local.description = remote.description

            # NOTE: Merge annotations
            local.annotations = list(set(local.annotations) | set(remote.annotations))

        # NOTE: Merge versions
        if int(remote.version) > int(local.version):
            local.version = remote.version

        local.agent_version = __version__

        # NOTE: Merge template data
        if local_template_changed and remote_template_changed:
            # NOTE: Merge conflicts!
            action = communication.prompt(
                "Merge conflict detected:\n Project template modified/update in both remote and local branch.\n"
                f"local: {local.template_metadata.template_source}@{local.template_metadata.template_ref}:"
                f"{local.template_metadata.template_id}, "
                "version {local.template_metadata.template_version}\n"
                f"remote: {remote.template_metadata.template_source}@{remote.template_metadata.template_ref}:"
                f"{remote.template_metadata.template_id}, "
                "version {remote.template_metadata.template_version}\n"
                "Which do you want to keep?\n[l]ocal, [r]emote, [a]bort:",
                default="a",
            )

            if action == "r":
                local.template_metadata = remote.template_metadata
            elif action == "a":
                raise errors.MetadataMergeError("Merge aborted")
            elif action != "l":
                raise errors.MetadataMergeError(f"Invalid merge option selected: {action}")
        elif remote_template_changed:
            local.template_metadata = remote.template_metadata

        return local

    def _compare_objects(self, local_object: Persistent, remote_object: Persistent) -> Optional[Persistent]:
        """Compare two objects and return the newer/relevant one, if possible."""
        if local_object == remote_object or getattr(local_object, "id", object()) == getattr(
            remote_object, "id", object()
        ):
            # NOTE: Objects are the same, nothing to do
            return local_object

        if isinstance(local_object, Dataset) and local_object.is_derivation():
            if self._is_dataset_derived_from(local_object, cast(Dataset, remote_object), self.local_database):
                return local_object
        if isinstance(remote_object, Dataset) and remote_object.is_derivation():
            if self._is_dataset_derived_from(remote_object, cast(Dataset, local_object), self.remote_database):
                return remote_object
        if isinstance(local_object, AbstractPlan) and local_object.is_derivation():
            if self._is_plan_derived_from(local_object, cast(AbstractPlan, remote_object), self.local_database):
                return local_object
        if isinstance(remote_object, AbstractPlan) and remote_object.is_derivation():
            if self._is_plan_derived_from(remote_object, cast(AbstractPlan, local_object), self.remote_database):
                return remote_object
        if isinstance(local_object, list) and isinstance(remote_object, list):
            local_object.extend(r for r in remote_object if r not in local_object)
            return local_object
        if isinstance(local_object, PersistentList) and isinstance(remote_object, PersistentList):
            local_object.extend(r for r in remote_object if r not in local_object)
            return local_object

        return None

    def _is_dataset_derived_from(self, potential_child: Dataset, potential_parent: Dataset, database: Database) -> bool:
        """Check if a dataset is a derivation of another dataset."""
        parent = potential_child
        while parent.is_derivation():
            parent = database.get_by_id(cast(Url, parent.derived_from).value)
            if parent.id == potential_parent.id:
                return True

        return False

    def _is_plan_derived_from(
        self, potential_child: AbstractPlan, potential_parent: AbstractPlan, database: Database
    ) -> bool:
        """Check if a dataset is a derivation of another dataset."""
        parent = potential_child
        while parent.is_derivation():
            parent = database.get_by_id(cast(str, parent.derived_from))
            if parent.id == potential_parent.id:
                return True

        return False
