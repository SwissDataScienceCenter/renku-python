# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Wrap Git client."""

import glob
import itertools
import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import attr

from renku.core import errors
from renku.core.utils.os import get_absolute_path
from renku.core.utils.scm import shorten_message
from renku.core.utils.urls import remove_credentials

COMMIT_DIFF_STRATEGY = "DIFF"
STARTED_AT = int(time.time() * 1e3)


def prepare_commit(
    client,
    commit_only=None,
    skip_dirty_checks=False,
):
    """Gather information about repo needed for committing later on."""
    diff_before = set()

    if commit_only == COMMIT_DIFF_STRATEGY:
        if len(client.repository.staged_changes) > 0 or len(client.repository.unstaged_changes) > 0:
            client.repository.reset()

        # Exclude files created by pipes.
        diff_before = {
            file
            for file in client.repository.untracked_files
            if STARTED_AT - int(Path(file).stat().st_ctime * 1e3) >= 1e3
        }

    if isinstance(commit_only, list) and not skip_dirty_checks:
        for path_ in commit_only:
            client.ensure_untracked(str(path_))
            client.ensure_unstaged(str(path_))

    return diff_before


def finalize_commit(
    client,
    diff_before,
    commit_only=None,
    commit_empty=True,
    raise_if_empty=False,
    commit_message=None,
    abbreviate_message=True,
):
    """Commit modified/added paths."""
    from renku.core.metadata.repository import Actor
    from renku.version import __version__, version_url

    committer = Actor(name=f"renku {__version__}", email=version_url)

    change_types = {item.a_path: item.change_type for item in client.repository.unstaged_changes}

    if commit_only == COMMIT_DIFF_STRATEGY:
        # Get diff generated in command.
        staged_after = set(change_types.keys())

        modified_after_change_types = {item.a_path: item.change_type for item in client.repository.staged_changes}

        modified_after = set(modified_after_change_types.keys())

        change_types.update(modified_after_change_types)

        diff_after = set(client.repository.untracked_files).union(staged_after).union(modified_after)

        # Remove files not touched in command.
        commit_only = list(diff_after - diff_before)

    if isinstance(commit_only, list):
        for path_ in commit_only:
            p = client.path / path_
            if p.exists() or change_types.get(str(path_)) == "D":
                client.repository.add(path_)

    if not commit_only:
        client.repository.add(all=True)

    try:
        diffs = [d.a_path for d in client.repository.staged_changes]
    except errors.GitError:
        diffs = []

    if not commit_empty and not diffs:
        if raise_if_empty:
            raise errors.NothingToCommit()
        return

    if commit_message and not isinstance(commit_message, str):
        raise errors.CommitMessageEmpty()

    elif not commit_message:
        argv = [os.path.basename(sys.argv[0])] + [remove_credentials(arg) for arg in sys.argv[1:]]

        commit_message = " ".join(argv)

    if abbreviate_message:
        commit_message = shorten_message(commit_message)

    # Ignore pre-commit hooks since we have already done everything.
    client.repository.commit(commit_message + client.transaction_id, committer=committer, no_verify=True)


def prepare_worktree(
    original_client,
    path=None,
    branch_name=None,
    commit=None,
):
    """Set up a Git worktree to provide isolation."""
    from renku.core.metadata.repository import NULL_TREE
    from renku.core.utils.contexts import Isolation

    path = path or tempfile.mkdtemp()
    branch_name = branch_name or "renku/run/isolation/" + uuid.uuid4().hex

    # TODO sys.argv

    if commit is NULL_TREE:
        args = ["add", "--detach", path]
        original_client.repository.run_git_command("worktree", *args)
        client = attr.evolve(original_client, path=path)
        client.repository.run_git_command("checkout", "--orphan", branch_name)
        client.repository.remove("*", recursive=True, force=True)
    else:
        args = ["add", "-b", branch_name, path]
        if commit:
            args.append(commit.hexsha)
        original_client.repository.run_git_command("worktree", *args)
        client = attr.evolve(original_client, path=path)

    client.repository.get_configuration = original_client.repository.get_configuration

    # Keep current directory relative to repository root.
    relative = Path(os.path.relpath(Path(".").resolve(), original_client.path))

    # Reroute standard streams
    original_mapped_std = get_mapped_std_streams(original_client.candidate_paths)
    mapped_std = {}
    for name, stream in original_mapped_std.items():
        stream_path = Path(path) / (Path(stream).relative_to(original_client.path))
        stream_path = stream_path.absolute()

        if not stream_path.exists():
            stream_path.parent.mkdir(parents=True, exist_ok=True)
            stream_path.touch()

        mapped_std[name] = stream_path

    _clean_streams(original_client.repository, original_mapped_std)

    new_cwd = Path(path) / relative
    new_cwd.mkdir(parents=True, exist_ok=True)

    isolation = Isolation(cwd=str(new_cwd), **mapped_std)
    isolation.__enter__()
    return client, isolation, path, branch_name


def finalize_worktree(
    client, isolation, path, branch_name, delete, new_branch, merge_args=("--ff-only",), exception=None
):
    """Cleanup and merge a previously created Git worktree."""
    exc_info = (None, None, None)

    if exception:
        exc_info = (type(exception), exception, exception.__traceback__)

    isolation.__exit__(*exc_info)

    try:
        client.repository.run_git_command("merge", branch_name, *merge_args)
    except errors.GitCommandError:
        raise errors.FailedMerge(client.repository, branch_name, merge_args)

    if delete:
        client.repository.run_git_command("worktree", "remove", path)

        if new_branch:
            # delete the created temporary branch
            client.repository.branches.remove(branch_name)
    if client.external_storage_requested:
        client.checkout_paths_from_storage()


def get_mapped_std_streams(lookup_paths, streams=("stdin", "stdout", "stderr")):
    """Get a mapping of standard streams to given paths."""
    # FIXME add device number too
    standard_inos = {}
    for stream in streams:
        try:
            stream_stat = os.fstat(getattr(sys, stream).fileno())
            key = stream_stat.st_dev, stream_stat.st_ino
            standard_inos[key] = stream
        except Exception:  # FIXME UnsupportedOperation
            pass
        # FIXME if not getattr(sys, stream).istty()

    def stream_inos(paths):
        """Yield tuples with stats and path."""
        for path in paths:
            try:
                stat = os.stat(path)
                key = (stat.st_dev, stat.st_ino)
                if key in standard_inos:
                    yield standard_inos[key], path
            except FileNotFoundError:  # pragma: no cover
                pass

        return []

    return dict(stream_inos(lookup_paths)) if standard_inos else {}


def _clean_streams(repository, mapped_streams):
    """Clean mapped standard streams."""
    for stream_name in ("stdout", "stderr"):
        stream = mapped_streams.get(stream_name)
        if not stream:
            continue

        absolute_path = get_absolute_path(stream, repository.path)
        path = os.path.relpath(absolute_path, start=repository.path)
        if path not in repository.files:
            os.remove(absolute_path)
        else:
            with open(path, "wb") as output_file:
                checksum = repository.get_object_hash(path=absolute_path, revision="HEAD")
                repository.copy_content_to_file(path=absolute_path, checksum=checksum, output_file=output_file)


def _expand_directories(paths):
    """Expand directory with all files it contains."""
    processed_paths = set()
    for path in paths:
        for matched_path in glob.iglob(str(path), recursive=True):
            if matched_path in processed_paths:
                continue
            path_ = Path(matched_path)
            if path_.is_dir():
                for expanded in path_.rglob("*"):
                    processed_paths.add(str(expanded))
                    yield str(expanded)
            else:
                processed_paths.add(matched_path)
                yield matched_path


@attr.s
class GitCore:
    """Wrap Git client."""

    repository = attr.ib(init=False, default=None)

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        from renku.core.metadata.repository import Repository

        #: Create an instance of a Git repository for the given path.
        try:
            self.repository = Repository(self.path)
        except errors.GitError:
            self.repository = None

    @property
    def modified_paths(self):
        """Return paths of modified files."""
        return [item.b_path for item in self.repository.unstaged_changes if item.b_path]

    @property
    def dirty_paths(self):
        """Get paths of dirty files in the repository."""
        repo_path = self.repository.path
        staged_files = [d.a_path for d in self.repository.staged_changes] if self.repository.head.is_valid() else []
        return {
            os.path.join(repo_path, p) for p in self.repository.untracked_files + self.modified_paths + staged_files
        }

    @property
    def candidate_paths(self):
        """Return all paths in the index and untracked files."""
        return [
            os.path.join(self.repository.path, path)
            for path in itertools.chain(self.repository.files, self.repository.untracked_files)
        ]

    def find_ignored_paths(self, *paths):
        """Return ignored paths matching ``.gitignore`` file."""
        return self.repository.get_ignored_paths(*paths)

    def remove_unmodified(self, paths, autocommit=True):
        """Remove unmodified paths and return their names."""
        tested_paths = set(_expand_directories(paths))

        # Keep only unchanged files in the output paths.
        tracked_paths = {
            diff.b_path
            for diff in self.repo.index.diff(None)
            if diff.change_type in {"A", "R", "M", "T"} and diff.b_path in tested_paths
        }
        unchanged_paths = tested_paths - tracked_paths

        return unchanged_paths

    def ensure_clean(self, ignore_std_streams=False):
        """Make sure the repository is clean."""
        dirty_paths = self.dirty_paths
        mapped_streams = get_mapped_std_streams(dirty_paths)

        if ignore_std_streams:
            if dirty_paths - set(mapped_streams.values()):
                _clean_streams(self.repository, mapped_streams)
                raise errors.DirtyRepository(self.repository)

        elif self.repository.is_dirty():
            _clean_streams(self.repository, mapped_streams)
            raise errors.DirtyRepository(self.repository)

    def ensure_untracked(self, path):
        """Ensure that path is not part of git untracked files."""
        untracked = self.repository.untracked_files

        for file_path in untracked:
            is_parent = (self.path / file_path).parent == (self.path / path)
            is_equal = path == file_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(self.repository)

    def ensure_unstaged(self, path):
        """Ensure that path is not part of git staged files."""
        staged = self.repository.staged_changes

        for file_path in staged:
            is_parent = str(file_path.a_path).startswith(path)
            is_equal = path == file_path.a_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(self.repository)

    def setup_credential_helper(self):
        """Setup git credential helper to ``cache`` if not set already."""
        credential_helper = self.repository.get_configuration().get_value("credential", "helper", "")

        if not credential_helper:
            with self.repository.get_configuration(writable=True) as w:
                w.set_value("credential", "helper", "cache")

    @contextmanager
    def commit(
        self,
        commit_only=None,
        commit_empty=True,
        raise_if_empty=False,
        commit_message=None,
        abbreviate_message=True,
        skip_dirty_checks=False,
    ):
        """Automatic commit."""

        diff_before = prepare_commit(self, commit_only=commit_only, skip_dirty_checks=skip_dirty_checks)

        yield

        finalize_commit(
            self,
            diff_before,
            commit_only=commit_only,
            commit_empty=commit_empty,
            raise_if_empty=raise_if_empty,
            commit_message=commit_message,
            abbreviate_message=abbreviate_message,
        )

    @contextmanager
    def worktree(self, path=None, branch_name=None, commit=None, merge_args=("--ff-only",)):
        """Create new worktree."""
        from renku.core.metadata.repository import NULL_TREE

        delete = branch_name is None
        new_branch = commit is not NULL_TREE

        new_client, isolation, path, branch_name = prepare_worktree(self, path, branch_name, commit)
        try:
            yield
        except (Exception, BaseException) as e:
            finalize_worktree(new_client, isolation, path, branch_name, delete, new_branch, merge_args, exception=e)
            raise
        else:
            finalize_worktree(new_client, isolation, path, branch_name, delete, new_branch, merge_args)
