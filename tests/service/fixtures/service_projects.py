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
"""Renku service fixtures for project management."""
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def project_metadata(project):
    """Create project with metadata."""
    metadata = {
        "project_id": uuid.uuid4().hex,
        "name": Path(project).name,
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "git@gitlab.com",
        "initialized": True,
    }

    yield project, metadata


@pytest.fixture(scope="module")
def it_git_access_token():
    """Returns a git access token for a testing run."""
    from tests.fixtures.config import IT_GIT_ACCESS_TOKEN

    return IT_GIT_ACCESS_TOKEN


@pytest.fixture(scope="module")
def it_remote_repo_url():
    """Returns a remote path to integration test repository."""
    from tests.fixtures.config import IT_REMOTE_REPO_URL

    return IT_REMOTE_REPO_URL


@pytest.fixture(scope="module")
def it_non_renku_repo_url():
    """Returns a remote path to integration test repository."""
    from tests.fixtures.config import IT_REMOTE_NON_RENKU_REPO_URL

    return IT_REMOTE_NON_RENKU_REPO_URL


@pytest.fixture(scope="module")
def it_no_commit_repo_url():
    """Returns a remote path to integration test repository."""
    from tests.fixtures.config import IT_REMOTE_NO_COMMITS_REPO_URL

    return IT_REMOTE_NO_COMMITS_REPO_URL


@pytest.fixture(scope="module")
def it_protected_repo_url():
    """Returns a protected repository url."""
    from tests.fixtures.config import IT_PROTECTED_REMOTE_REPO_URL

    return IT_PROTECTED_REMOTE_REPO_URL
