# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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
"""Utility functions for managing the underling Git repository."""

import os
import sys
from contextlib import contextmanager

import click
from git import Repo

from renku import errors

GIT_KEY = 'renku.git'


def set_git_home(value):
    """Set Git path."""
    ctx = click.get_current_context()
    ctx.meta[GIT_KEY] = value


def get_git_home():
    """Get Git path from current context."""
    ctx = click.get_current_context(silent=True)
    if ctx and GIT_KEY in ctx.meta:
        return ctx.meta[GIT_KEY]
    return Repo('.', search_parent_directories=True).working_dir


def _dirty_paths(repo):
    """Get paths of dirty files in the repository."""
    repo_path = repo.working_dir
    return {
        os.path.join(repo_path, p)
        for p in repo.untracked_files +
        [item.a_path for item in repo.index.diff(None)]
    }


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

    lookup_inos = {(stat.st_dev, stat.st_ino): path
                   for stat, path in ((os.stat(path), path)
                                      for path in lookup_paths)}

    return {
        standard_inos[key]: lookup_inos[key]
        for key in set(standard_inos.keys()) & set(lookup_inos.keys())
    }


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


@contextmanager
def with_git(
    clean=True, up_to_date=False, commit=True, ignore_std_streams=False
):
    """Perform Git checks and operations."""
    repo_path = get_git_home()
    current_dir = os.getcwd()

    if clean:
        try:
            os.chdir(repo_path)
            repo = Repo(repo_path)

            dirty_paths = _dirty_paths(repo)
            mapped_streams = _mapped_std_streams(dirty_paths)

            if ignore_std_streams:
                if dirty_paths - set(mapped_streams.values()):
                    _clean_streams(repo, mapped_streams)
                    raise errors.DirtyRepository(repo)

            elif repo.is_dirty(untracked_files=True):
                _clean_streams(repo, mapped_streams)
                raise errors.DirtyRepository(repo)
        finally:
            os.chdir(current_dir)

    if up_to_date:
        # TODO
        # Fetch origin/master
        # is_ancestor('origin/master', 'HEAD')
        pass

    yield

    if commit:
        try:
            os.chdir(repo_path)
            repo = Repo(get_git_home())
            repo.git.add('--all')
            argv = [os.path.basename(sys.argv[0])] + sys.argv[1:]
            # Ignore pre-commit hooks since we have already done everything.
            repo.index.commit(' '.join(argv), skip_hooks=True)
        finally:
            os.chdir(current_dir)


def _safe_issue_checkout(repo, issue=None):
    """Safely checkout branch for the issue."""
    branch_name = str(issue) if issue else 'master'
    if branch_name not in repo.heads:
        branch = repo.create_head(branch_name)
    else:
        branch = repo.heads[branch_name]
    branch.checkout()
