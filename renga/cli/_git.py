
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

from dulwich.repo import Repo
from dulwich import porcelain as git


GIT_KEY = 'renga.git'


def set_git_home(value):
    """Set Git path."""
    ctx = click.get_current_context()
    ctx.meta[GIT_KEY] = value


def get_git_home():
    """Get Git path from current context."""
    return click.get_current_context().meta.get(GIT_KEY, '.')


@contextmanager
def with_git(clean=True, up_to_date=False, commit=True):
    """Perform Git checks and operations."""
    repo_path = get_git_home()
    current_dir = os.getcwd()

    if clean:  # pragma: no cover
        try:
            os.chdir(repo_path)
            status = git.status()

            if any(status.staged.values()):
                raise RuntimeError('Uncommited changes')
            elif status.unstaged:
                raise RuntimeError('Modified files')

        finally:
            os.chdir(current_dir)

    if up_to_date:
        # TODO
        pass

    yield

    if commit:
        try:
            os.chdir(repo_path)
            git.add()
            ref = git.commit(
                message=b' '.join((a.encode('utf-8') for a in sys.argv)))
            repo = Repo('.')
            current_branch_ref = repo.refs.follow(b'HEAD')[0][1]
            repo[current_branch_ref] = ref
        finally:
            os.chdir(current_dir)


def _safe_issue_checkout(repo, issue=None):
    """Safely checkout branch for the issue."""
    branch = '{0}'.format(issue).encode('utf-8') if issue else b'master'
    branch_ref = b'refs/heads/' + branch

    if branch_ref in repo:
        repo.reset_index(repo[branch_ref].tree)
    else:
        git.branch_create(repo, branch, objectish=b'refs/heads/master')

    repo.refs.set_symbolic_ref(b'HEAD', branch_ref)
