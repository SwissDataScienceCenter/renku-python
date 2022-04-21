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
from pathlib import Path
from tempfile import mkdtemp
from typing import List, NamedTuple, Optional, Union, cast

from BTrees.OOBTree import BTree, TreeSet
from deepdiff import DeepDiff
from persistent import Persistent
from persistent.list import PersistentList
from zc.relation.catalog import Catalog

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management import RENKU_HOME
from renku.core.management.client import LocalClient
from renku.core.util import communication
from renku.domain_model.dataset import Dataset, Url
from renku.domain_model.project import Project
from renku.domain_model.workflow.plan import AbstractPlan
from renku.infrastructure.database import Database, Index
from renku.infrastructure.repository import Repository
from renku.version import __version__

RemoteEntry = NamedTuple(
    "RemoteEntry", [("reference", str), ("database", Database), ("path", Path), ("repository", Repository)]
)


class GitMerger:
    """Git metadata merger."""

    client_dispatcher = inject.attr(IClientDispatcher)
    database_dispatcher = inject.attr(IDatabaseDispatcher)

    def merge(self, local: Path, remote: Path, base: Path) -> None:
        """Merge two renku metadata entries together."""
        client = self.client_dispatcher.current_client
        self.remote_entries: List[RemoteEntry] = []

        self._setup_worktrees(client)

        result = None
        self.local_database = self.database_dispatcher.current_database

        try:
            local_object = self.local_database.get_from_path(str(client.path / local))
            base_object = self.local_database.get_from_path(str(client.path / base))

            for entry in self.remote_entries:
                try:
                    self.remote_database = entry.database
                    remote_object = self.remote_database.get_from_path(str(client.path / remote))
                    result = self.merge_objects(local_object, remote_object, base_object)
                    break
                except errors.ObjectNotFoundError:
                    continue
        finally:
            # NOTE: cleanup worktrees
            for entry in self.remote_entries:
                client.repository.remove_worktree(entry.path)

        if result is None:
            raise errors.MetadataMergeError("Couldn't merge metadata: remote object not found in merge branches.")

        self.local_database.persist_to_path(result, local)

    def _setup_worktrees(self, client):
        """Setup git worktrees for the remote branches."""

        # NOTE: Get remote branches
        remote_branches = [os.environ[k] for k in os.environ.keys() if k.startswith("GITHEAD")]

        for remote_branch in remote_branches:
            # NOTE: Create a new shallow worktree for each remote branch, could be several in case of an octo merge
            worktree_path = Path(mkdtemp())
            client.repository.create_worktree(worktree_path, remote_branch, checkout=False)
            try:
                remote_repository = Repository(worktree_path)
                database_path = Path(RENKU_HOME) / LocalClient.DATABASE_PATH
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
                    client.repository.remove_worktree(worktree_path)
                except Exception:
                    pass
                raise

    def merge_objects(self, local: Persistent, remote: Persistent, base: Persistent) -> Persistent:
        """Merge two database objects."""
        if type(local) != type(remote):
            raise errors.MetadataMergeError(f"Cannot merge {local} and {remote}: disparate types.")
        if isinstance(local, (BTree, Index)):
            return self._merge_btrees(local, remote)
        elif isinstance(local, TreeSet):
            return self._merge_treesets(local, remote)
        elif isinstance(local, Project):
            return self._merge_projects(local, remote, base)
        else:
            raise errors.MetadataMergeError(
                f"Cannot merge {local} and {remote}: type not supported for automated merge."
            )

    def _merge_btrees(self, local: Union[BTree, Index], remote: Union[BTree, Index]) -> Union[BTree, Index]:
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
            diff = DeepDiff(local_object, remote_object)
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

    def _merge_treesets(self, local: TreeSet, remote: TreeSet) -> TreeSet:
        """Merge two TreeSets."""
        local.update([e for e in remote if e not in local])
        return local

    def _merge_indices(self, local: Index, remote: Index) -> Index:
        """Merge two BTrees."""
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
            diff = DeepDiff(local_object, remote_object)
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

    def _merge_catalogs(self, local: Catalog, remote: Catalog) -> Catalog:
        """Merge two Catalogs."""
        raise NotImplementedError()

    def _merge_projects(self, local: Project, remote: Project, base: Project) -> Project:
        """Merge two Project entries."""

        local_changed = (
            local.keywords != base.keywords
            or local.description != base.description
            or local.annotations != base.annotations
        )
        local_template_changed = (
            local.template_id != base.template_id
            or local.template_ref != base.template_ref
            or local.template_source != base.template_source
            or local.template_version != base.template_version
        )
        remote_changed = (
            remote.keywords != base.keywords
            or remote.description != base.description
            or remote.annotations != base.annotations
        )
        remote_template_changed = (
            remote.template_id != base.template_id
            or remote.template_ref != base.template_ref
            or remote.template_source != base.template_source
            or remote.template_version != base.template_version
        )

        if (local_changed or local_template_changed) and not remote_changed and not remote_template_changed:
            return local
        elif not local_changed and not local_template_changed and (remote_changed or remote_template_changed):
            return remote

        if local_changed or remote_changed:
            # NOTE: Merge keywords
            if local.keywords != remote.keywords:
                if local.keywords != base.keywords and remote.keywords != base.keywords:
                    removed = (set(base.keywords) - set(local.keywords)) | (set(base.keywords) - set(remote.keywords))
                    added = (set(local.keywords) - set(base.keywords)) | (set(remote.keywords) - set(base.keywords))
                    existing = set(base.keywords) - removed
                    local.keywords = list(added | existing)
                elif remote.keywords != base.keywords:
                    local.keywords = remote.keywords

            # NOTE: Merge description
            if local.description != remote.description:
                if local.description != base.description and remote.description != base.description:
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
                f"local: {local.template_source}@{local.template_ref}:{local.template_id}, "
                "version {local.template_version}\n"
                f"remote: {remote.template_source}@{remote.template_ref}:{remote.template_id}, "
                "version {remote.template_version}\n"
                "Which do you want to keep?\n[l]ocal, [r]emote, [a]bort:",
                default="a",
            )

            if action == "r":
                local.template_id = remote.template_id
                local.template_ref = remote.template_ref
                local.template_source = remote.template_source
                local.template_version = remote.template_version
                local.template_metadata = remote.template_metadata
                local.immutable_template_files = remote.immutable_template_files
                local.automated_update = remote.automated_update
            elif action == "a":
                raise errors.MetadataMergeError("Merge aborted")
            elif action != "l":
                raise errors.MetadataMergeError(f"Invalid merge option selected: {action}")
        elif remote_template_changed:
            local.template_id = remote.template_id
            local.template_ref = remote.template_ref
            local.template_source = remote.template_source
            local.template_version = remote.template_version
            local.template_metadata = remote.template_metadata
            local.immutable_template_files = remote.immutable_template_files
            local.automated_update = remote.automated_update

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
