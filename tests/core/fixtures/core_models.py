# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku fixtures for models."""

import os
import urllib

import pytest

from renku.infrastructure.repository import Repository
from tests.fixtures.config import IT_PROTECTED_REMOTE_REPO_URL, IT_REMOTE_NON_RENKU_REPO_URL
from tests.utils import clone_compressed_repository


@pytest.fixture
def git_repository(tmp_path):
    """A Git repository.

    Commits:

        * 8853e0c (HEAD -> master, origin/master, origin/HEAD) changes
        * 9c4c680 G
        *   2b98350 Merge branch 'feature'
        |\
        | * 556dc54 F
        | * b5689b7 E
        * | a150977 D
        |/
        * 1a5d1c7 C
        * d44be07 A B

    Changes to files:

        8853e0c (HEAD -> master, origin/master, origin/HEAD) changes
            M       A
            D       B
            R100    C       data/X
        9c4c680 G
            A       G
        2b98350 Merge branch 'feature'
        556dc54 F
            A       F
        b5689b7 E
            A       E
        a150977 D
            A       D
        1a5d1c7 C
            A       C
        d44be07 A B
            A       A
            A       B
    """
    from renku.core.util.contexts import chdir

    name = "git-repository.git"
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.path):
        yield repository


@pytest.fixture
def git_repository_with_remote(git_repository):
    """A Git repository with remote."""
    git_repository.run_git_command("remote", "set-url", "origin", IT_PROTECTED_REMOTE_REPO_URL)

    yield git_repository


@pytest.fixture
def git_repository_with_multiple_remotes(git_repository_with_remote):
    """A Git repository with remote."""
    git_repository_with_remote.run_git_command("remote", "add", "second-remote", IT_REMOTE_NON_RENKU_REPO_URL)

    yield git_repository_with_remote


@pytest.fixture
def protected_git_repository(tmp_path):
    """A Git repository with remote."""
    parsed_url = urllib.parse.urlparse(IT_PROTECTED_REMOTE_REPO_URL)

    url = f"oauth2:{os.getenv('IT_OAUTH_GIT_TOKEN')}@{parsed_url.netloc}"
    parsed_url = parsed_url._replace(netloc=url).geturl()

    repository = Repository.clone_from(url=parsed_url, path=tmp_path)

    with repository.get_configuration(writable=True) as config:
        config.set_value("pull", "rebase", "false")

    yield repository
