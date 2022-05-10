# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Client for handling a data storage."""
import csv
import functools
import itertools
import os
import re
import shlex
import tempfile
from collections import defaultdict
from pathlib import Path
from shutil import move, which
from subprocess import PIPE, STDOUT, check_output, run
from typing import List, Optional, Tuple

import attr
import pathspec
from werkzeug.utils import cached_property

from renku.core import errors
from renku.core.util import communication
from renku.core.util.file_size import parse_file_size
from renku.core.util.git import run_command
from renku.domain_model.entity import Entity
from renku.domain_model.provenance.activity import Collection

from .git import _expand_directories
from .repository import RepositoryApiMixin  # type: ignore


class RenkuGitWildMatchPattern(pathspec.patterns.GitWildMatchPattern):
    """Custom GitWildMatchPattern matcher."""

    __slots__ = ("pattern",)

    def __init__(self, pattern, include=None):
        """Initialize RenkuRegexPattern."""
        super().__init__(pattern, include)
        self.pattern = pattern


pathspec.util.register_pattern("renku_gitwildmatch", RenkuGitWildMatchPattern)


def check_external_storage_wrapper(fn):
    """Check availability of external storage on methods that need it.

    Raises:
        ``errors.ExternalStorageNotInstalled``: If external storage isn't installed.
        ``errors.ExternalStorageDisabled``: If external storage isn't enabled.
    """
    # noqa
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self.check_external_storage():
            pass
        else:
            return fn(self, *args, **kwargs)

    return wrapper


@attr.s
class StorageApiMixin(RepositoryApiMixin):
    """Client for handling a data storage."""

    external_storage_requested = attr.ib(default=True)
    """External storage (e.g. LFS) requested for Renku command."""

    RENKU_LFS_IGNORE_PATH = ".renkulfsignore"
    """.gitignore like file specifying paths that are not tracked in LFS."""

    _CMD_STORAGE_INSTALL = ["git", "lfs", "install", "--local"]

    _CMD_STORAGE_TRACK = ["git", "lfs", "track", "--"]

    _CMD_STORAGE_UNTRACK = ["git", "lfs", "untrack", "--"]

    _CMD_STORAGE_CLEAN = ["git", "lfs", "clean"]

    _CMD_STORAGE_CHECKOUT = ["git", "lfs", "checkout"]

    _CMD_STORAGE_PULL = ["git", "lfs", "pull", "-I"]

    _CMD_STORAGE_MIGRATE_IMPORT = ["git", "lfs", "migrate", "import"]

    _CMD_STORAGE_MIGRATE_INFO = ["git", "lfs", "migrate", "info", "--top", "42000"]

    _CMD_STORAGE_LIST = ["git", "lfs", "ls-files", "-n"]

    _CMD_STORAGE_STATUS = ["git", "lfs", "status"]

    _LFS_HEADER = "version https://git-lfs.github.com/spec/"

    @cached_property
    def storage_installed(self):
        """Verify that git-lfs is installed and on system PATH."""
        return bool(which("git-lfs"))

    @cached_property
    def storage_installed_locally(self):
        """Verify that git-lfs is installed for the project."""
        repo_config = self.repository.get_configuration(scope="local")
        return repo_config.has_section('filter "lfs"')

    def check_external_storage(self):
        """Check if repository has external storage enabled.

        Raises:
            ``errors.ExternalStorageNotInstalled``: If external storage isn't installed.
            ``errors.ExternalStorageDisabled``: If external storage isn't enabled.
        """
        storage_installed = self.storage_installed_locally and self.storage_installed
        if self.external_storage_requested and not storage_installed:
            raise errors.ExternalStorageDisabled(self.repository)

        if self.storage_installed_locally and not self.storage_installed:
            raise errors.ExternalStorageNotInstalled(self.repository)

        return storage_installed

    @cached_property
    def renku_lfs_ignore(self):
        """Gets pathspec for files to not add to LFS."""
        ignore_path = self.path / self.RENKU_LFS_IGNORE_PATH

        if not os.path.exists(ignore_path):
            return pathspec.PathSpec.from_lines("renku_gitwildmatch", self.RENKU_PROTECTED_PATHS)
        with ignore_path.open("r") as f:
            # NOTE: Append `renku_protected_paths` at the end to give it the highest priority
            lines = itertools.chain(f, self.RENKU_PROTECTED_PATHS)
            return pathspec.PathSpec.from_lines("renku_gitwildmatch", lines)

    @property
    def minimum_lfs_file_size(self):
        """The minimum size of a file in bytes to be added to lfs."""
        size = self.get_value("renku", "lfs_threshold")

        return parse_file_size(size)

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        try:
            result = run(
                self._CMD_STORAGE_INSTALL + (["--force"] if force else []),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=self.path,
                universal_newlines=True,
            )

            if result.returncode != 0:
                raise errors.GitLFSError(f"Error executing 'git lfs install: \n {result.stdout}")
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")

    def init_repository(self, force=False, user=None, initial_branch=None):
        """Initialize a local Renku repository."""
        result = super().init_repository(force=force, user=user, initial_branch=initial_branch)

        # initialize LFS if it is requested and installed
        if self.external_storage_requested and self.storage_installed:
            self.init_external_storage(force=force)

        return result

    @check_external_storage_wrapper
    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if not self.external_storage_requested:
            return

        # Calculate which paths can be tracked in lfs
        track_paths = []
        attrs = self.repository.get_attributes(*paths)

        for path in paths:
            path = Path(path)

            # Do not track symlinks in LFS
            if path.is_symlink():
                continue

            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(str(path), {}).get("filter") == "lfs" or not (self.path / path).exists():
                continue

            relative_path = Path(path).relative_to(self.path) if path.is_absolute() else path

            if (
                path.is_dir()
                and not self.renku_lfs_ignore.match_file(relative_path)
                and not any(self.renku_lfs_ignore.match_tree(str(relative_path)))
            ):
                track_paths.append(str(path / "**"))
            elif not self.renku_lfs_ignore.match_file(str(relative_path)):
                file_size = os.path.getsize(str(os.path.relpath(self.path / path, os.getcwd())))
                if file_size >= self.minimum_lfs_file_size:
                    track_paths.append(str(relative_path))

        if track_paths:
            try:
                result = run_command(
                    self._CMD_STORAGE_TRACK,
                    *track_paths,
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=self.path,
                    universal_newlines=True,
                )

                if result.returncode != 0:
                    raise errors.GitLFSError(f"Error executing 'git lfs track: \n {result.stdout}")
            except (KeyboardInterrupt, OSError) as e:
                raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")

        show_message = self.get_value("renku", "show_lfs_message")
        if track_paths and (show_message is None or show_message == "True"):
            files_list = "\n\t".join(track_paths)
            communication.info(
                f"Adding these files to Git LFS:\n\t{files_list}"
                "\nTo disable this message in the future, run:\n\trenku config set show_lfs_message False"
            )

        return track_paths

    @check_external_storage_wrapper
    def untrack_paths_from_storage(self, *paths):
        """Untrack paths from the external storage."""
        try:
            result = run_command(
                self._CMD_STORAGE_UNTRACK, *paths, stdout=PIPE, stderr=STDOUT, cwd=self.path, universal_newlines=True
            )

            if result.returncode != 0:
                raise errors.GitLFSError(f"Error executing 'git lfs untrack: \n {result.stdout}")
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")

    @check_external_storage_wrapper
    def list_tracked_paths(self, client=None):
        """List paths tracked in lfs for a client."""
        client = client or self
        try:
            files = check_output(self._CMD_STORAGE_LIST, cwd=client.path, encoding="UTF-8")
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(f"Couldn't run 'git lfs ls-files':\n{e}")
        files = [client.path / f for f in files.splitlines()]
        return files

    @check_external_storage_wrapper
    def list_unpushed_lfs_paths(self, client=None):
        """List paths tracked in lfs for a client."""
        client = client or self

        if len(client.repository.remotes) < 1 or not client.repository.active_branch.remote_branch:
            raise errors.GitConfigurationError(
                f"No git remote is configured for {client.path} branch {client.repository.active_branch.name}."
                + "Cleaning the storage cache would lead to a loss of data as "
                + "it is not on a server. Please see "
                + "https://www.atlassian.com/git/tutorials/syncing for "
                + "information on how to sync with a remote."
            )
        try:
            status = check_output(self._CMD_STORAGE_STATUS, cwd=client.path, encoding="UTF-8")
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(f"Couldn't run 'git lfs status':\n{e}")

        files = status.split("Objects to be committed:")[0].splitlines()[2:]
        files = [client.path / f.rsplit("(", 1)[0].strip() for f in files if f.strip()]
        return files

    @check_external_storage_wrapper
    def pull_paths_from_storage(self, *paths):
        """Pull paths from LFS."""
        client_dict = defaultdict(list)

        for path in _expand_directories(paths):
            client, commit, path = self.get_in_submodules(self.repository.head.commit, path)
            try:
                absolute_path = Path(path).resolve()
                relative_path = absolute_path.relative_to(client.path)
            except ValueError:  # An external file
                absolute_path = Path(os.path.abspath(path))
                relative_path = absolute_path.relative_to(client.path)
            client_dict[client.path].append(shlex.quote(str(relative_path)))

        for client_path, paths in client_dict.items():
            result = run_command(
                self._CMD_STORAGE_PULL,
                *paths,
                separator=",",
                cwd=client_path,
                stdout=PIPE,
                stderr=STDOUT,
                universal_newlines=True,
            )

            if result.returncode != 0:
                raise errors.GitLFSError(f"Cannot pull LFS objects from server:\n {result.stdout}")

    @check_external_storage_wrapper
    def clean_storage_cache(self, *paths):
        """Remove paths from lfs cache."""
        client_dict = defaultdict(list)
        clients = {}
        tracked_paths = {}
        unpushed_paths = {}
        untracked_paths = []
        local_only_paths = []

        for path in _expand_directories(paths):
            client, _, path = self.get_in_submodules(self.repository.head.commit, path)
            try:
                absolute_path = Path(path).resolve()
                relative_path = absolute_path.relative_to(client.path)
            except ValueError:  # An external file
                absolute_path = Path(os.path.abspath(path))
                relative_path = absolute_path.relative_to(client.path)

            if client.path not in tracked_paths:
                tracked_paths[client.path] = self.list_tracked_paths(client)

            if client.path not in unpushed_paths:
                u_paths = self.list_unpushed_lfs_paths(client)
                unpushed_paths[client.path] = u_paths

            if absolute_path in unpushed_paths[client.path]:
                local_only_paths.append(str(relative_path))
            elif absolute_path not in tracked_paths[client.path]:
                untracked_paths.append(str(relative_path))
            else:
                client_dict[client.path].append(str(relative_path))
                clients[client.path] = client

        for client_path, paths in client_dict.items():
            client = clients[client_path]

            for path in paths:
                with open(path, "r") as tracked_file:
                    try:
                        header = tracked_file.read(len(self._LFS_HEADER))
                        if header == self._LFS_HEADER:
                            # file is not pulled
                            continue
                    except UnicodeDecodeError:
                        # likely a binary file, not lfs pointer file
                        pass
                with tempfile.NamedTemporaryFile(mode="w+t", encoding="utf-8", delete=False) as tmp, open(
                    path, "r+t"
                ) as input_file:
                    result = run(
                        self._CMD_STORAGE_CLEAN, cwd=client_path, stdin=input_file, stdout=tmp, universal_newlines=True
                    )

                    if result.returncode != 0:
                        raise errors.GitLFSError(f"Error executing 'git lfs clean: \n {result.stdout}")

                    tmp_path = tmp.name
                move(tmp_path, path)

                # get lfs sha hash
                old_pointer = client.repository.get_raw_content(path=path, revision="HEAD")
                old_pointer = old_pointer.splitlines()[1]
                old_pointer = old_pointer.split(" ")[1].split(":")[1]

                prefix1 = old_pointer[:2]
                prefix2 = old_pointer[2:4]

                # remove from lfs cache
                object_path = client.path / ".git" / "lfs" / "objects" / prefix1 / prefix2 / old_pointer
                object_path.unlink()

            # add paths so they don't show as modified
            client.repository.add(*paths)

        return untracked_paths, local_only_paths

    @check_external_storage_wrapper
    def checkout_paths_from_storage(self, *paths):
        """Checkout a paths from LFS."""
        result = run_command(
            self._CMD_STORAGE_CHECKOUT, *paths, cwd=self.path, stdout=PIPE, stderr=STDOUT, universal_newlines=True
        )

        if result.returncode != 0:
            raise errors.GitLFSError(f"Error executing 'git lfs checkout: \n {result.stdout}")

    def check_requires_tracking(self, *paths):
        """Check paths and return a list of those that must be tracked."""

        if not self.external_storage_requested:
            return

        attrs = self.repository.get_attributes(*paths)
        track_paths = []

        for path in paths:
            absolute_path = Path(os.path.abspath(self.path / path))
            path = str(path)

            # Do not track symlinks in LFS
            if absolute_path.is_symlink():
                continue

            # Do not add files with filter=lfs in .gitattributes
            if attrs.get(path, {}).get("filter") == "lfs":
                continue

            if not absolute_path.is_dir():
                if self.renku_lfs_ignore.match_file(path):
                    continue
                if os.path.getsize(absolute_path) < self.minimum_lfs_file_size:
                    continue

                track_paths.append(path)

        return track_paths

    def get_lfs_migrate_filters(self) -> Tuple[List[str], List[str]]:
        """Gets include, exclude and above filters for lfs migrate."""

        def add_migrate_pattern(pattern, collection):
            if pattern in self.RENKU_PROTECTED_PATHS:
                return
            pattern = pattern.strip()
            if pattern.endswith("*"):
                return
            pattern = pattern.rstrip("/")
            collection.append(f"{pattern}/**")

        includes = []
        excludes = []
        for p in self.renku_lfs_ignore.patterns:
            if p.regex is None:
                continue

            pattern = p.pattern.replace(os.linesep, "").replace("\n", "")
            if pattern.startswith("!"):
                pattern = pattern.replace("!", "", 1)

            if p.include:  # File ignored by LFS
                excludes.append(pattern)
                add_migrate_pattern(pattern, excludes)
            else:
                includes.append(pattern)
                add_migrate_pattern(pattern, includes)

        if excludes:
            excludes = ["--exclude", ",".join(excludes)]
        if includes:
            includes = ["--include", ",".join(includes)]

        return includes, excludes

    def check_lfs_migrate_info(self, everything=False, use_size_filter=True):
        """Return list of file groups in history should be in LFS."""
        ref = ["--everything"] if everything else ["--include-ref", self.repository.active_branch.name]

        includes, excludes = self.get_lfs_migrate_filters()

        ignore_pointers = ["--pointers", "ignore"]

        command = self._CMD_STORAGE_MIGRATE_INFO + ref + includes + excludes

        # NOTE: ``lfs migrate info`` supports ``--above`` while ``lfs migrate import`` doesn't.
        if use_size_filter:
            above = ["--above", str(self.minimum_lfs_file_size)]
            command += above

        try:
            lfs_output = run(
                command + ignore_pointers, stdout=PIPE, stderr=STDOUT, cwd=self.path, universal_newlines=True
            )
        except (KeyboardInterrupt, OSError) as e:
            raise errors.GitError(f"Couldn't run 'git lfs migrate info':\n{e}")

        if lfs_output.returncode != 0:
            # NOTE: try running without --pointers (old versions of git lfs)
            try:
                lfs_output = run(command, stdout=PIPE, stderr=STDOUT, cwd=self.path, universal_newlines=True)
            except (KeyboardInterrupt, OSError) as e:
                raise errors.GitError(f"Couldn't run 'git lfs migrate info':\n{e}")

            if lfs_output.returncode != 0:
                raise errors.GitLFSError(f"Error executing 'git lfs migrate info: \n {lfs_output.stdout}")

        groups = []
        files_re = re.compile(r"(.*\s+[\d.]+\s+\S+).*")

        for line in lfs_output.stdout.split("\n"):
            match = files_re.match(line)
            if match:
                groups.append(match.groups()[0])

        if groups and use_size_filter:
            # NOTE: Since there are some large files, remove the size filter so that users get list of all files that
            # will be moved to LFS.
            return self.check_lfs_migrate_info(everything=everything, use_size_filter=False)

        return groups

    def migrate_files_to_lfs(self, paths):
        """Migrate files to Git LFS."""
        if paths:
            includes = ["--include", ",".join(paths)]
            excludes = []
        else:
            includes, excludes = self.get_lfs_migrate_filters()

        tempdir = Path(tempfile.mkdtemp())
        map_path = tempdir / "objectmap.csv"
        object_map = [f"--object-map={map_path}"]

        command = self._CMD_STORAGE_MIGRATE_IMPORT + includes + excludes + object_map

        try:
            lfs_output = run(command, stdout=PIPE, stderr=STDOUT, cwd=self.path, universal_newlines=True)
        except (KeyboardInterrupt, OSError) as e:
            raise errors.GitError(f"Couldn't run 'git lfs migrate import':\n{e}")

        if lfs_output.returncode != 0:
            raise errors.GitLFSError(f"Error executing 'git lfs migrate import: \n {lfs_output.stdout}")

        with open(map_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")

            commit_sha_mapping = [(r[0], r[1]) for r in reader]

        os.remove(map_path)

        sha_mapping = dict()

        repo_root = Path(".")

        for old_commit_sha, new_commit_sha in commit_sha_mapping:
            old_commit = self.repository.get_commit(old_commit_sha)
            new_commit = self.repository.get_commit(new_commit_sha)
            processed = set()

            for diff in old_commit.get_changes():
                path_obj = Path(diff.b_path)

                # NOTE: Get git object hash mapping for files and parent folders
                while path_obj != repo_root:
                    if path_obj in processed:
                        break

                    path_str = str(path_obj)
                    old_sha = old_commit.tree[path_str].hexsha
                    new_sha = new_commit.tree[path_str].hexsha

                    sha_mapping[old_sha] = new_sha

                    processed.add(path_obj)
                    path_obj = path_obj.parent

        def _map_checksum(entity, checksum_mapping) -> Optional[Entity]:
            """Update the checksum and id of an entity based on a mapping."""
            if entity.checksum not in checksum_mapping:
                return None

            new_checksum = checksum_mapping[entity.checksum]

            if isinstance(entity, Collection) and entity.members:
                members = []
                for member in entity.members:
                    new_member = _map_checksum(member, checksum_mapping)
                    if new_member:
                        members.append(new_member)
                    else:
                        members.append(member)
                new_entity: Entity = Collection(checksum=new_checksum, path=entity.path, members=members)
            else:
                new_entity = Entity(checksum=new_checksum, path=entity.path)

            return new_entity

        def _map_checksum_old(entity, checksum_mapping):
            """Update the checksum and id of an entity based on a mapping."""
            # TODO: Remove this method once moved to Entity with 'id' field
            from renku.domain_model.provenance.activity import Collection

            if entity.checksum not in checksum_mapping:
                return

            new_checksum = checksum_mapping[entity.checksum]

            entity._id = entity._id.replace(entity.checksum, new_checksum)
            entity.checksum = new_checksum

            if isinstance(entity, Collection) and entity.members:
                for member in entity.members:
                    _map_checksum_old(member, checksum_mapping)

        # NOTE: Update workflow provenance
        # provenance_graph = ProvenanceGraph.from_database(database)

        # TODO: Update activities
        # for activity in provenance_graph.activities:
        #     # NOTE: This is a valid use-case since history will be re-written
        #     activity.unfreeze()
        #     if activity.generations:
        #         generations = []
        #         for generation in activity.generations:
        #             new_entity = _map_checksum(generation.entity, sha_mapping)
        #             if new_entity:
        #                 new_generation = Generation(id=generation.id, entity=new_entity)
        #                 generations.append(new_generation)
        #             else:
        #                 generations.append(generation)
        #         activity.generations = generations

        #     if activity.usages:
        #         usages = []
        #         for usage in activity.usages:
        #             new_entity = _map_checksum(usage.entity, sha_mapping)
        #             if new_entity:
        #                 new_usage = Usage(id=usage.id, entity=new_entity)
        #                 usages.append(new_usage)
        #             else:
        #                 usages.append(usage)
        #         activity.usages = usages

        #     if activity.invalidations:
        #         invalidations = []
        #         for entity in activity.invalidations:
        #             new_entity = _map_checksum(entity, sha_mapping)
        #             if new_entity:
        #                 invalidations.append(new_entity)
        #             else:
        #                 invalidations.append(entity)
        #         activity.invalidations = invalidations

        #     activity.freeze()

        # NOTE: Update datasets provenance
        # TODO: Fix dataset provenance
        # for dataset in datasets_provenance.datasets:
        #     for file_ in dataset.files:
        #         _map_checksum_old(file_.entity, sha_mapping)
