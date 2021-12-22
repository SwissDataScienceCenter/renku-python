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
"""Renku common configurations."""
import os
from pathlib import Path

import pytest

IT_PROTECTED_REMOTE_REPO_URL = os.getenv(
    "IT_PROTECTED_REMOTE_REPO", "https://dev.renku.ch/gitlab/renku-python-integration-tests/core-it-protected.git"
)

IT_REMOTE_REPO_URL = os.getenv(
    "IT_REMOTE_REPOSITORY", "https://dev.renku.ch/gitlab/renku-python-integration-tests/core-integration-test"
)
IT_REMOTE_NON_RENKU_REPO_URL = os.getenv(
    "IT_REMOTE_NON_RENKU_REPO_URL", "https://dev.renku.ch/gitlab/renku-python-integration-tests/core-it-non-renku"
)
IT_REMOTE_NO_COMMITS_REPO_URL = os.getenv(
    "IT_REMOTE_NO_COMMITS_REPO_URL", "https://dev.renku.ch/gitlab/renku-python-integration-tests/core-it-no-commits"
)
IT_GIT_ACCESS_TOKEN = os.getenv("IT_OAUTH_GIT_TOKEN")


@pytest.fixture
def global_config_dir(monkeypatch, tmpdir):
    """Create a temporary renku config directory."""
    from renku.core.management.config import ConfigManagerMixin

    with monkeypatch.context() as m:
        home_dir = tmpdir.mkdir("fake_home").strpath
        m.setattr(ConfigManagerMixin, "global_config_dir", home_dir)

        yield m


@pytest.fixture(scope="session")
def vcr_config():
    """Common configuration for all vcr tests."""
    return {"filter_headers": ["authorization"], "ignore_localhost": True, "record_mode": "once"}


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    """Base directory to store cassettes for each test file (module)."""
    relative_path = Path(request.node.fspath).relative_to(os.getcwd())
    parts = Path(relative_path).parts
    # cassettes/test-file-path-in-tests-directory/test-file-name-with-no-extension/
    return os.path.join("cassettes", os.sep.join(parts[1:-1]), relative_path.stem)
