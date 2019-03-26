# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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

import itertools
import os
import shutil
import sys
import tempfile
import uuid
from collections import defaultdict
from contextlib import contextmanager
from email.utils import formatdate
from itertools import zip_longest

import attr

from renku import errors
from renku._compat import Path


def _mapped_std_streams(lookup_paths, streams=('stdin', 'stdout', 'stderr')):
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

    return dict(stream_inos(lookup_paths)) if standard_inos else {}


def _clean_streams(repo, mapped_streams):
    """Clean mapped standard streams."""
    for stream_name in ('stdout', 'stderr'):
        stream = mapped_streams.get(stream_name)
        if not stream:
            continue

        path = os.path.relpath(stream, start=repo.working_dir)
        if (path, 0) not in repo.index.entries:
            os.remove(stream)
        else:
            blob = repo.index.entries[(path, 0)].to_blob(repo)
            with open(path, 'wb') as fp:
                fp.write(blob.data_stream.read())


def _expand_directories(paths):
    """Expand directory with all files it contains."""
    for path in paths:
        path_ = Path(path)
        if path_.is_dir():
            for expanded in path_.rglob('*'):
                yield str(expanded)
        else:
            yield path


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
        return [
            item.b_path for item in self.repo.index.diff(None) if item.b_path
        ]

    @property
    def dirty_paths(self):
        """Get paths of dirty files in the repository."""
        repo_path = self.repo.working_dir
        return {
            os.path.join(repo_path, p)
            for p in self.repo.untracked_files + self.modified_paths
        }

    @property
    def candidate_paths(self):
        """Return all paths in the index and untracked files."""
        repo_path = self.repo.working_dir
        return [
            os.path.join(repo_path, path) for path in itertools.chain(
                (x[0] for x in self.repo.index.entries),
                self.repo.untracked_files,
            )
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
            data = self.repo.git.check_attr('-z', '-a', '--', *paths)
            for file, name, value in zip_longest(
                *[iter(data.strip('\0').split('\0'))] * 3
            ):
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
            if diff.change_type in {'A', 'R', 'M', 'T'} and
            diff.b_path in tested_paths
        }
        unchanged_paths = tested_paths - tracked_paths

        # Fix tracking of unchanged files by removing them first.
        if autocommit and unchanged_paths:
            self.repo.index.remove(
                unchanged_paths, cached=True, r=True, ignore_unmatch=True
            )
            self.repo.index.commit(
                'renku: automatic removal of unchanged files'
            )
            self.repo.index.add(unchanged_paths)

        return unchanged_paths

    def ensure_clean(self, ignore_std_streams=False):
        """Make sure the repository is clean."""
        dirty_paths = self.dirty_paths
        mapped_streams = _mapped_std_streams(dirty_paths)

        if ignore_std_streams:
            if dirty_paths - set(mapped_streams.values()):
                _clean_streams(self.repo, mapped_streams)
                raise errors.DirtyRepository(self.repo)

        elif self.repo.is_dirty(untracked_files=True):
            _clean_streams(self.repo, mapped_streams)
            raise errors.DirtyRepository(self.repo)

    @contextmanager
    def commit(self, author_date=None):
        """Automatic commit."""
        from git import Actor
        from renku.version import __version__

        author_date = author_date or formatdate(localtime=True)

        yield

        committer = Actor(
            'renku {0}'.format(__version__),
            'renku+{0}@datascience.ch'.format(__version__),
        )

        self.repo.git.add('--all')
        argv = [os.path.basename(sys.argv[0])] + sys.argv[1:]
        # Ignore pre-commit hooks since we have already done everything.
        self.repo.index.commit(
            ' '.join(argv),
            author_date=author_date,
            committer=committer,
            skip_hooks=True,
        )

    @contextmanager
    def transaction(
        self,
        clean=True,
        up_to_date=False,
        commit=True,
        ignore_std_streams=False
    ):
        """Perform Git checks and operations."""
        if clean:
            self.ensure_clean(ignore_std_streams=ignore_std_streams)

        if up_to_date:
            # TODO
            # Fetch origin/master
            # is_ancestor('origin/master', 'HEAD')
            pass

        if commit:
            with self.commit():
                yield self
        else:
            yield self

    @contextmanager
    def worktree(
        self,
        path=None,
        branch_name=None,
        commit=None,
        merge_args=('--ff-only', ),
    ):
        """Create new worktree."""
        from git import GitCommandError, NULL_TREE
        from renku._contexts import Isolation

        delete = path is None
        path = path or tempfile.mkdtemp()
        branch_name = branch_name or 'renku/run/isolation/' + uuid.uuid4().hex

        # TODO sys.argv

        if commit is NULL_TREE:
            args = ['add', '--detach', path]
            self.repo.git.worktree(*args)
            client = attr.evolve(self, path=path)
            client.repo.git.checkout('--orphan', branch_name)
            client.repo.git.rm('-rf', '*')
        else:
            args = ['add', '-b', branch_name, path]
            if commit:
                args.append(commit)
            self.repo.git.worktree(*args)
            client = attr.evolve(self, path=path)

        client.repo.config_reader = self.repo.config_reader

        # Keep current directory relative to repository root.
        relative = Path('.').resolve().relative_to(self.path)

        # Reroute standard streams
        original_mapped_std = _mapped_std_streams(self.candidate_paths)
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
            raise errors.FailedMerge(self.repo)

        if delete:
            shutil.rmtree(path)
            self.repo.git.worktree('prune')

        self.checkout_paths_from_storage()
