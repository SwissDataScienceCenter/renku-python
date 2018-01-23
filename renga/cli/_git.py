
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
def with_git(clean=True, commit=True):
    """Perform Git checks and operations."""
    repo_path = get_git_home()
    current_dir = os.getcwd()

    if clean:  # pragma: no cover
        try:
            raise NotImplemented()
            os.chdir(repo_path)
            changed = git.status()
        finally:
            os.chdir(current_dir)

    yield

    try:
        os.chdir(repo_path)
        git.add()
        if commit:
            git.commit(message=b' '.join((a.encode('utf-8') for a in sys.argv)))
    finally:
        os.chdir(current_dir)
