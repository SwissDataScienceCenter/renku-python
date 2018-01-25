# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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

GIT_KEY = 'renga.git'


def set_git_home(value):
    """Set Git path."""
    ctx = click.get_current_context()
    ctx.meta[GIT_KEY] = value


def get_git_home():
    """Get Git path from current context."""
    return click.get_current_context().meta.get(GIT_KEY, '.')


@contextmanager
def with_git(clean=True, up_to_date=False, commit=True, ignore_fileno=None):
    """Perform Git checks and operations."""
    repo_path = get_git_home()
    current_dir = os.getcwd()

    if clean:  # pragma: no cover
        try:
            os.chdir(repo_path)
            repo = Repo(repo_path)

            if ignore_fileno:
                dirty_paths = repo.untracked_files + \
                    [item.a_path for item in repo.index.diff(None)]

                ignore_inos = {os.fstat(fileno).st_ino for fileno in ignore_fileno}
                dirty_inos = {os.stat(path).st_ino for path in dirty_paths}

                if dirty_inos - ignore_inos:
                    raise RuntimeError('The repository is dirty.')

            elif repo.is_dirty(untracked_files=True):
                raise RuntimeError('The repository is dirty.')

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
            repo.index.commit(' '.join(sys.argv))
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
