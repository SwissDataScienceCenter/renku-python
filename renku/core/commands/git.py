# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

import click

GIT_KEY = "renku.git"
GIT_ISOLATION = "renku.worktree"


def set_git_home(value):
    """Set Git path."""
    ctx = click.get_current_context()
    ctx.meta[GIT_KEY] = value


def get_git_home(path="."):
    """Get Git path from the current context."""
    ctx = click.get_current_context(silent=True)
    if ctx and GIT_KEY in ctx.meta:
        return ctx.meta[GIT_KEY]

    from git import Repo

    return Repo(path, search_parent_directories=True).working_dir


def set_git_isolation(value):
    """Set Git isolation."""
    ctx = click.get_current_context()
    ctx.meta[GIT_ISOLATION] = value


def get_git_isolation():
    """Get Git isolation from the current context."""
    ctx = click.get_current_context(silent=True)
    if ctx and GIT_ISOLATION in ctx.meta:
        return ctx.meta[GIT_ISOLATION]


def _safe_issue_checkout(repo, issue=None):
    """Safely checkout branch for the issue."""
    branch_name = str(issue) if issue else "master"
    if branch_name not in repo.heads:
        branch = repo.create_head(branch_name)
    else:
        branch = repo.heads[branch_name]
    branch.checkout()
