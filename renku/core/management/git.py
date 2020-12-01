# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
from collections import defaultdict
from contextlib import contextmanager
from itertools import zip_longest
from pathlib import Path

import attr
import git

from renku.core import errors
from renku.core.utils.scm import git_unicode_unescape, shorten_message
from renku.core.utils.urls import remove_credentials

COMMIT_DIFF_STRATEGY = "DIFF"
STARTED_AT = int(time.time() * 1e3)


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


def _clean_streams(repo, mapped_streams):
    """Clean mapped standard streams."""
    for stream_name in ("stdout", "stderr"):
        stream = mapped_streams.get(stream_name)
        if not stream:
            continue

        path = os.path.relpath(stream, start=repo.working_dir)
        if (path, 0) not in repo.index.entries:
            os.remove(stream)
        else:
            blob = repo.index.entries[(path, 0)].to_blob(repo)
            with open(path, "wb") as fp:
                fp.write(blob.data_stream.read())


def _expand_directories(paths):
    """Expand directory with all files it contains."""
    processed_paths = set()
    for path in paths:
        for matched_path in glob.glob(path, recursive=True):
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

    repo = attr.ib(init=False)
    """Store an instance of the Git repository."""

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        from git import InvalidGitRepositoryError, Repo

        #: Create an instance of a Git repository for the given path.
        try:
            self.repo = Repo(str(self.path))
        except InvalidGitRepositoryError:
            self.repo = None

    @property
    def modified_paths(self):
        """Return paths of modified files."""
        return [item.b_path for item in self.repo.index.diff(None) if item.b_path]

    @property
    def dirty_paths(self):
        """Get paths of dirty files in the repository."""
        repo_path = self.repo.working_dir
        staged_files = self.repo.index.diff("HEAD") if self.repo.head.is_valid() else []
        return {os.path.join(repo_path, p) for p in self.repo.untracked_files + self.modified_paths + staged_files}

    @property
    def candidate_paths(self):
        """Return all paths in the index and untracked files."""
        repo_path = self.repo.working_dir
        return [
            os.path.join(repo_path, path)
            for path in itertools.chain((x[0] for x in self.repo.index.entries), self.repo.untracked_files,)
        ]

    def find_ignored_paths(self, *paths):
        """Return ignored paths matching ``.gitignore`` file."""
        from git.exc import GitCommandError

        try:
            return self.repo.git.check_ignore(*paths).split()
        except GitCommandError:
            pass

    def find_attr(self, *paths):
        """Return map with path and its attributes."""
        from git.exc import GitCommandError

        attrs = defaultdict(dict)
        try:
            data = self.repo.git.check_attr("-z", "-a", "--", *paths)
            for file, name, value in zip_longest(*[iter(data.strip("\0").split("\0"))] * 3):
                if file:
                    attrs[file][name] = value
        except GitCommandError:
            pass

        return attrs

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

        # Fix tracking of unchanged files by removing them first.
        if autocommit and unchanged_paths:
            self.repo.index.remove(unchanged_paths, cached=True, r=True, ignore_unmatch=True)
            self.repo.index.commit("renku: automatic removal of unchanged files")
            self.repo.index.add(unchanged_paths)

        return unchanged_paths

    def ensure_clean(self, ignore_std_streams=False):
        """Make sure the repository is clean."""
        dirty_paths = self.dirty_paths
        mapped_streams = get_mapped_std_streams(dirty_paths)

        if ignore_std_streams:
            if dirty_paths - set(mapped_streams.values()):
                _clean_streams(self.repo, mapped_streams)
                raise errors.DirtyRepository(self.repo)

        elif self.repo.is_dirty(untracked_files=True):
            _clean_streams(self.repo, mapped_streams)
            raise errors.DirtyRepository(self.repo)

    def ensure_untracked(self, path):
        """Ensure that path is not part of git untracked files."""
        untracked = self.repo.untracked_files

        for file_path in untracked:
            is_parent = str(file_path).startswith(path)
            is_equal = path == file_path

            if is_parent or is_equal:
                raise errors.DirtyRenkuDirectory(self.repo)

    def ensure_unstaged(self, path):
        """Ensure that path is not part of git staged files."""
        try:
            staged = self.repo.index.diff("HEAD")

            for file_path in staged:
                unescaped_path = git_unicode_unescape(file_path.a_path)
                is_parent = str(unescaped_path).startswith(path)
                is_equal = path == unescaped_path

                if is_parent or is_equal:
                    raise errors.DirtyRenkuDirectory(self.repo)

        except git.exc.BadName:
            pass

    def setup_credential_helper(self):
        """Setup git credential helper to ``cache`` if not set already."""
        credential_helper = self.repo.config_reader().get_value("credential", "helper", "")

        if not credential_helper:
            with self.repo.config_writer() as w:
                w.set_value("credential", "helper", "cache")

    @contextmanager
    def commit(
        self, commit_only=None, commit_empty=True, raise_if_empty=False, commit_message=None, abbreviate_message=True
    ):
        """Automatic commit."""
        from git import Actor

        from renku.version import __version__, version_url

        diff_before = set()

        if commit_only == COMMIT_DIFF_STRATEGY:
            staged = {git_unicode_unescape(item.a_path) for item in self.repo.index.diff(None)}

            modified = {git_unicode_unescape(item.a_path) for item in self.repo.index.diff("HEAD")}

            if staged or modified:
                self.repo.git.reset()

            # Exclude files created by pipes.
            diff_before = {
                file_
                for file_ in self.repo.untracked_files
                if STARTED_AT - int(Path(file_).stat().st_ctime * 1e3) >= 1e3
            }

        if isinstance(commit_only, list):
            for path_ in commit_only:
                self.ensure_untracked(str(path_))
                self.ensure_unstaged(str(path_))

        project_metadata_path = str(self.renku_metadata_path)

        yield

        committer = Actor("renku {0}".format(__version__), version_url)

        change_types = {}

        if commit_only == COMMIT_DIFF_STRATEGY:
            # Get diff generated in command.
            change_types = {git_unicode_unescape(item.a_path): item.change_type for item in self.repo.index.diff(None)}
            staged_after = set(change_types.keys())

            modified_after_change_types = {
                git_unicode_unescape(item.a_path): item.change_type for item in self.repo.index.diff("HEAD")
            }

            modified_after = set(modified_after_change_types.keys())

            change_types.update(modified_after_change_types)

            diff_after = set(self.repo.untracked_files).union(staged_after).union(modified_after)

            # Remove files not touched in command.
            commit_only = list(diff_after - diff_before)

        if isinstance(commit_only, list):
            for path_ in commit_only:
                p = self.path / path_
                if p.exists() or change_types.get(path_) == "D":
                    self.repo.git.add(path_)

        if not commit_only:
            self.repo.git.add("--all")

        diffs = []
        try:
            diffs = [git_unicode_unescape(d.a_path) for d in self.repo.index.diff("HEAD")]
            if project_metadata_path in diffs:
                diffs.remove(project_metadata_path)
        except git.exc.BadName:
            pass

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

        try:
            project = self.project
            if project:
                project.to_yaml()
                self.repo.index.add(project_metadata_path)
        except ValueError:
            pass

        # Ignore pre-commit hooks since we have already done everything.
        self.repo.index.commit(
            commit_message, committer=committer, skip_hooks=True,
        )

    @contextmanager
    def transaction(
        self,
        clean=True,
        commit=True,
        commit_empty=True,
        commit_message=None,
        commit_only=None,
        ignore_std_streams=False,
        raise_if_empty=False,
    ):
        """Perform Git checks and operations."""
        if clean:
            self.ensure_clean(ignore_std_streams=ignore_std_streams)

        if commit:
            with self.commit(
                commit_empty=commit_empty,
                commit_message=commit_message,
                commit_only=commit_only,
                raise_if_empty=raise_if_empty,
            ):
                yield self
        else:
            yield self

    @contextmanager
    def worktree(
        self, path=None, branch_name=None, commit=None, merge_args=("--ff-only",),
    ):
        """Create new worktree."""
        from git import NULL_TREE, GitCommandError

        from renku.core.utils.contexts import Isolation

        delete = path is None
        path = path or tempfile.mkdtemp()
        branch_name = branch_name or "renku/run/isolation/" + uuid.uuid4().hex

        # TODO sys.argv

        new_branch = False

        if commit is NULL_TREE:
            args = ["add", "--detach", path]
            self.repo.git.worktree(*args)
            client = attr.evolve(self, path=path)
            client.repo.git.checkout("--orphan", branch_name)
            client.repo.git.rm("-rf", "*")
        else:
            args = ["add", "-b", branch_name, path]
            if commit:
                args.append(commit)
            self.repo.git.worktree(*args)
            client = attr.evolve(self, path=path)
            new_branch = True

        client.repo.config_reader = self.repo.config_reader

        # Keep current directory relative to repository root.
        relative = Path(".").resolve().relative_to(self.path)

        # Reroute standard streams
        original_mapped_std = get_mapped_std_streams(self.candidate_paths)
        mapped_std = {}
        for name, stream in original_mapped_std.items():
            stream_path = Path(path) / (Path(stream).relative_to(self.path))
            stream_path = stream_path.absolute()

            if not stream_path.exists():
                stream_path.parent.mkdir(parents=True, exist_ok=True)
                stream_path.touch()

            mapped_std[name] = stream_path

        _clean_streams(self.repo, original_mapped_std)

        new_cwd = Path(path) / relative
        new_cwd.mkdir(parents=True, exist_ok=True)

        with Isolation(cwd=str(new_cwd), **mapped_std):
            yield client

        try:
            self.repo.git.merge(branch_name, *merge_args)
        except GitCommandError:
            raise errors.FailedMerge(self.repo, branch_name, merge_args)

        if delete:
            self.repo.git.worktree("remove", path)

            if new_branch:
                # delete the created temporary branch
                self.repo.git.branch("-d", branch_name)

        self.checkout_paths_from_storage()
