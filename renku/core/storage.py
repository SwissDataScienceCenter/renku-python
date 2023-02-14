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
"""Logic for handling a data storage."""

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
from typing import TYPE_CHECKING, List, Optional, Tuple

import pathspec

from renku.core import errors
from renku.core.config import get_value
from renku.core.constant import RENKU_LFS_IGNORE_PATH, RENKU_PROTECTED_PATHS
from renku.core.util import communication
from renku.core.util.file_size import parse_file_size
from renku.core.util.git import get_in_submodules, run_command
from renku.core.util.os import expand_directories
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.domain_model.entity import Entity  # type: ignore
    from renku.infrastructure.repository import Repository


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
    def wrapper(*args, **kwargs):
        if not check_external_storage():
            pass
        else:
            return fn(*args, **kwargs)

    return wrapper


@functools.lru_cache()
def storage_installed():
    """Verify that git-lfs is installed and on system PATH."""
    return bool(which("git-lfs"))


def storage_installed_locally():
    """Verify that git-lfs is installed for the project."""
    repo_config = project_context.repository.get_configuration(scope="local")
    return repo_config.has_section('filter "lfs"')


def check_external_storage():
    """Check if repository has external storage enabled.

    Raises:
        ``errors.ExternalStorageNotInstalled``: If external storage isn't installed.
        ``errors.ExternalStorageDisabled``: If external storage isn't enabled.
    """
    installed_locally = storage_installed_locally()
    is_storage_installed = installed_locally and storage_installed()

    if project_context.external_storage_requested and not is_storage_installed:
        raise errors.ExternalStorageDisabled()

    if installed_locally and not storage_installed():
        raise errors.ExternalStorageNotInstalled()

    return is_storage_installed


def renku_lfs_ignore():
    """Gets pathspec for files to not add to LFS."""
    ignore_path = project_context.path / RENKU_LFS_IGNORE_PATH

    if not os.path.exists(ignore_path):
        return pathspec.PathSpec.from_lines("renku_gitwildmatch", RENKU_PROTECTED_PATHS)
    with ignore_path.open("r") as f:
        # NOTE: Append `renku_protected_paths` at the end to give it the highest priority
        lines = itertools.chain(f, RENKU_PROTECTED_PATHS)
        return pathspec.PathSpec.from_lines("renku_gitwildmatch", lines)


def get_minimum_lfs_file_size():
    """The minimum size of a file in bytes to be added to lfs."""
    size = get_value("renku", "lfs_threshold")

    return parse_file_size(size)


def init_external_storage(force=False):
    """Initialize the external storage for data."""
    try:
        result = run(
            _CMD_STORAGE_INSTALL + (["--force"] if force else []),
            stdout=PIPE,
            stderr=STDOUT,
            cwd=project_context.path,
            universal_newlines=True,
        )

        if result.returncode != 0:
            raise errors.GitLFSError(f"Error executing 'git lfs install: \n {result.stdout}")
    except (KeyboardInterrupt, OSError) as e:
        raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")


@check_external_storage_wrapper
def track_paths_in_storage(*paths):
    """Track paths in the external storage."""
    if not project_context.external_storage_requested or not check_external_storage():
        return

    # Calculate which paths can be tracked in lfs
    track_paths = []
    attrs = project_context.repository.get_attributes(*paths)

    for path in paths:
        path = Path(path)

        # Do not track symlinks in LFS
        if path.is_symlink():
            continue

        # Do not add files with filter=lfs in .gitattributes
        if attrs.get(str(path), {}).get("filter") == "lfs" or not (project_context.path / path).exists():
            continue

        relative_path = Path(path).relative_to(project_context.path) if path.is_absolute() else path

        if (
            path.is_dir()
            and not renku_lfs_ignore().match_file(relative_path)
            and not any(renku_lfs_ignore().match_tree(str(relative_path)))
        ):
            track_paths.append(str(path / "**"))
        elif not renku_lfs_ignore().match_file(str(relative_path)):
            file_size = os.path.getsize(str(os.path.relpath(project_context.path / path, os.getcwd())))
            if file_size >= get_minimum_lfs_file_size():
                track_paths.append(str(relative_path))

    if track_paths:
        try:
            result = run_command(
                _CMD_STORAGE_TRACK,
                *track_paths,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=project_context.path,
                universal_newlines=True,
            )

            if result.returncode != 0:
                raise errors.GitLFSError(f"Error executing 'git lfs track: \n {result.stdout}")
        except (KeyboardInterrupt, OSError) as e:
            raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")

    show_message = get_value("renku", "show_lfs_message")
    if track_paths and (show_message is None or show_message == "True"):
        files_list = "\n\t".join(track_paths)
        communication.info(
            f"Adding these files to Git LFS:\n\t{files_list}"
            "\nTo disable this message in the future, run:\n\trenku config set show_lfs_message False"
        )

    return track_paths


@check_external_storage_wrapper
def untrack_paths_from_storage(*paths):
    """Untrack paths from the external storage."""
    try:
        result = run_command(
            _CMD_STORAGE_UNTRACK,
            *paths,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=project_context.path,
            universal_newlines=True,
        )

        if result.returncode != 0:
            raise errors.GitLFSError(f"Error executing 'git lfs untrack: \n {result.stdout}")
    except (KeyboardInterrupt, OSError) as e:
        raise errors.ParameterError(f"Couldn't run 'git lfs':\n{e}")


@check_external_storage_wrapper
def list_tracked_paths():
    """List paths tracked in lfs."""
    try:
        files = check_output(_CMD_STORAGE_LIST, cwd=project_context.path, encoding="UTF-8")
    except (KeyboardInterrupt, OSError) as e:
        raise errors.ParameterError(f"Couldn't run 'git lfs ls-files':\n{e}")
    files_split = [project_context.path / f for f in files.splitlines()]
    return files_split


@check_external_storage_wrapper
def list_unpushed_lfs_paths(repository: "Repository"):
    """List paths tracked in lfs for a repository."""

    if len(repository.remotes) < 1 or (repository.active_branch and not repository.active_branch.remote_branch):
        raise errors.GitConfigurationError(
            f"No git remote is configured for {project_context.path} branch "
            + f"{repository.active_branch.name}."  # type: ignore
            + "Cleaning the storage cache would lead to a loss of data as "
            + "it is not on a server. Please see "
            + "https://www.atlassian.com/git/tutorials/syncing for "
            + "information on how to sync with a remote."
        )
    try:
        status = check_output(_CMD_STORAGE_STATUS, cwd=project_context.path, encoding="UTF-8")
    except (KeyboardInterrupt, OSError) as e:
        raise errors.ParameterError(f"Couldn't run 'git lfs status':\n{e}")

    files = status.split("Objects to be committed:")[0].splitlines()[2:]
    return [project_context.path / f.rsplit("(", 1)[0].strip() for f in files if f.strip()]


@check_external_storage_wrapper
def pull_paths_from_storage(repository: "Repository", *paths):
    """Pull paths from LFS."""
    project_dict = defaultdict(list)

    for path in expand_directories(paths):
        sub_repository, _, path = get_in_submodules(repository, repository.head.commit, path)
        try:
            absolute_path = Path(path).resolve()
            relative_path = absolute_path.relative_to(project_context.path)
        except ValueError:  # An external file
            absolute_path = Path(os.path.abspath(path))
            relative_path = absolute_path.relative_to(project_context.path)
        project_dict[sub_repository.path].append(shlex.quote(str(relative_path)))

    for project_path, file_paths in project_dict.items():
        result = run_command(
            _CMD_STORAGE_PULL,
            *file_paths,
            separator=",",
            cwd=project_path,
            stdout=PIPE,
            stderr=STDOUT,
            universal_newlines=True,
        )

        if result.returncode != 0:
            raise errors.GitLFSError(f"Cannot pull LFS objects from server:\n {result.stdout}")


@check_external_storage_wrapper
def clean_storage_cache(*check_paths):
    """Remove paths from lfs cache."""
    project_dict = defaultdict(list)
    repositories = {}
    tracked_paths = {}
    unpushed_paths = {}
    untracked_paths = []
    local_only_paths = []

    repository = project_context.repository

    for path in expand_directories(check_paths):
        current_repository, _, path = get_in_submodules(repository=repository, commit=repository.head.commit, path=path)
        try:
            absolute_path = Path(path).resolve()
            relative_path = absolute_path.relative_to(project_context.path)
        except ValueError:  # An external file
            absolute_path = Path(os.path.abspath(path))
            relative_path = absolute_path.relative_to(project_context.path)

        if project_context.path not in tracked_paths:
            tracked_paths[project_context.path] = list_tracked_paths()

        if project_context.path not in unpushed_paths:
            u_paths = list_unpushed_lfs_paths(current_repository)
            unpushed_paths[project_context.path] = u_paths

        if absolute_path in unpushed_paths[project_context.path]:
            local_only_paths.append(str(relative_path))
        elif absolute_path not in tracked_paths[project_context.path]:
            untracked_paths.append(str(relative_path))
        else:
            project_dict[project_context.path].append(str(relative_path))
            repositories[project_context.path] = current_repository

    for project_path, paths in project_dict.items():
        current_repository = repositories[project_path]

        for path in paths:
            with open(path, "r") as tracked_file:
                try:
                    header = tracked_file.read(len(_LFS_HEADER))
                    if header == _LFS_HEADER:
                        # file is not pulled
                        continue
                except UnicodeDecodeError:
                    # likely a binary file, not lfs pointer file
                    pass
            with tempfile.NamedTemporaryFile(mode="w+t", encoding="utf-8", delete=False) as tmp, open(
                path, "r+t"
            ) as input_file:
                result = run(
                    _CMD_STORAGE_CLEAN, cwd=project_path, stdin=input_file, stdout=tmp, universal_newlines=True
                )

                if result.returncode != 0:
                    raise errors.GitLFSError(f"Error executing 'git lfs clean: \n {result.stdout}")

                tmp_path = tmp.name
            move(tmp_path, path)

            # get lfs sha hash
            old_pointer = current_repository.get_raw_content(path=path, revision="HEAD")
            old_pointer = old_pointer.splitlines()[1]
            old_pointer = old_pointer.split(" ")[1].split(":")[1]

            prefix1 = old_pointer[:2]
            prefix2 = old_pointer[2:4]

            # remove from lfs cache
            object_path = project_context.path / ".git" / "lfs" / "objects" / prefix1 / prefix2 / old_pointer
            object_path.unlink()

        # add paths so they don't show as modified
        current_repository.add(*paths)

    return untracked_paths, local_only_paths


@check_external_storage_wrapper
def checkout_paths_from_storage(*paths):
    """Checkout a paths from LFS."""
    result = run_command(
        _CMD_STORAGE_CHECKOUT,
        *paths,
        cwd=project_context.path,
        stdout=PIPE,
        stderr=STDOUT,
        universal_newlines=True,
    )

    if result.returncode != 0:
        raise errors.GitLFSError(f"Error executing 'git lfs checkout: \n {result.stdout}")


def check_requires_tracking(*paths):
    """Check paths and return a list of those that must be tracked."""

    if not project_context.external_storage_requested:
        return

    attrs = project_context.repository.get_attributes(*paths)
    track_paths = []

    for path in paths:
        absolute_path = Path(os.path.abspath(project_context.path / path))
        path = str(path)

        # Do not track symlinks in LFS
        if absolute_path.is_symlink():
            continue

        # Do not add files with filter=lfs in .gitattributes
        if attrs.get(path, {}).get("filter") == "lfs":
            continue

        if not absolute_path.is_dir():
            if renku_lfs_ignore().match_file(path):
                continue
            if os.path.getsize(absolute_path) < get_minimum_lfs_file_size():
                continue

            track_paths.append(path)

    return track_paths


def get_lfs_migrate_filters() -> Tuple[List[str], List[str]]:
    """Gets include, exclude and above filters for lfs migrate."""

    def add_migrate_pattern(pattern, collection):
        if pattern in RENKU_PROTECTED_PATHS:
            return
        pattern = pattern.strip()
        if pattern.endswith("*"):
            return
        pattern = pattern.rstrip("/")
        collection.append(f"{pattern}/**")

    includes = []
    excludes = []
    for p in renku_lfs_ignore().patterns:
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


def check_lfs_migrate_info(everything=False, use_size_filter=True):
    """Return list of file groups in history should be in LFS."""
    ref = (
        ["--everything"]
        if everything or not project_context.repository.active_branch
        else ["--include-ref", project_context.repository.active_branch.name]
    )

    includes, excludes = get_lfs_migrate_filters()

    ignore_pointers = ["--pointers", "ignore"]

    command = _CMD_STORAGE_MIGRATE_INFO + ref + includes + excludes

    # NOTE: ``lfs migrate info`` supports ``--above`` while ``lfs migrate import`` doesn't.
    if use_size_filter:
        above = ["--above", str(get_minimum_lfs_file_size())]
        command += above

    try:
        lfs_output = run(
            command + ignore_pointers,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=project_context.path,
            universal_newlines=True,
        )
    except (KeyboardInterrupt, OSError) as e:
        raise errors.GitError(f"Couldn't run 'git lfs migrate info':\n{e}")

    if lfs_output.returncode != 0:
        # NOTE: try running without --pointers (old versions of git lfs)
        try:
            lfs_output = run(command, stdout=PIPE, stderr=STDOUT, cwd=project_context.path, universal_newlines=True)
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
        return check_lfs_migrate_info(everything=everything, use_size_filter=False)

    return groups


def migrate_files_to_lfs(paths):
    """Migrate files to Git LFS."""
    if paths:
        includes: List[str] = ["--include", ",".join(paths)]
        excludes: List[str] = []
    else:
        includes, excludes = get_lfs_migrate_filters()

    tempdir = Path(tempfile.mkdtemp())
    map_path = tempdir / "objectmap.csv"
    object_map = [f"--object-map={map_path}"]

    command = _CMD_STORAGE_MIGRATE_IMPORT + includes + excludes + object_map

    try:
        lfs_output = run(command, stdout=PIPE, stderr=STDOUT, cwd=project_context.path, universal_newlines=True)
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
    repository = project_context.repository

    for old_commit_sha, new_commit_sha in commit_sha_mapping:
        old_commit = repository.get_commit(old_commit_sha)
        new_commit = repository.get_commit(new_commit_sha)
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

    def _map_checksum(entity, checksum_mapping) -> Optional["Entity"]:
        """Update the checksum and id of an entity based on a mapping."""
        from renku.domain_model.entity import Entity
        from renku.domain_model.provenance.activity import Collection

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
