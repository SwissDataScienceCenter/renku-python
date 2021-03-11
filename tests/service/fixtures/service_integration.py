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
"""Renku service fixtures for integration testing."""
import contextlib
import json
import uuid
from copy import deepcopy

import pytest
from git import GitCommandError


def integration_repo_path(headers, project_id, url_components):
    """Constructs integration repo path."""
    from renku.service.serializers.headers import RequiredIdentityHeaders
    from renku.service.utils import make_project_path

    user = RequiredIdentityHeaders().load(headers)
    project = {
        "project_id": project_id,
        "owner": url_components.owner,
        "name": url_components.name,
    }

    project_path = make_project_path(user, project)
    return project_path


@contextlib.contextmanager
def integration_repo(headers, project_id, url_components):
    """With integration repo helper."""
    from git import Repo

    from renku.core.utils.contexts import chdir

    repo_path = integration_repo_path(headers, project_id, url_components)
    with chdir(repo_path):
        repo = Repo(repo_path)
        repo.heads.master.checkout()

        yield repo

        if integration_repo_path(headers, project_id, url_components).exists():
            repo.git.reset("--hard")
            repo.heads.master.checkout()
            repo.git.reset("--hard")
            repo.git.clean("-xdf")


@pytest.fixture(scope="module")
def integration_lifecycle(svc_client, mock_redis, identity_headers, it_remote_repo_url):
    """Setup and teardown steps for integration tests."""
    from renku.core.models.git import GitURL

    url_components = GitURL.parse(it_remote_repo_url)

    payload = {"git_url": it_remote_repo_url, "depth": 0}

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert {"result"} == set(response.json.keys())

    project_id = response.json["result"]["project_id"]
    assert isinstance(uuid.UUID(project_id), uuid.UUID)

    yield svc_client, identity_headers, project_id, url_components

    # Teardown step: Delete all branches except master (if needed).
    if integration_repo_path(identity_headers, project_id, url_components).exists():
        with integration_repo(identity_headers, project_id, url_components) as repo:
            try:
                repo.remote().push(refspec=(":{0}".format(repo.active_branch.name)))
            except GitCommandError:
                pass


@pytest.fixture
def svc_client_setup(integration_lifecycle):
    """Service client setup."""
    svc_client, headers, project_id, url_components = integration_lifecycle

    with integration_repo(headers, project_id, url_components) as repo:
        repo.git.checkout("master")

        new_branch = uuid.uuid4().hex
        current = repo.create_head(new_branch)
        current.checkout()

        yield svc_client, deepcopy(headers), project_id, url_components

        if integration_repo_path(headers, project_id, url_components).exists():
            # NOTE: Some tests delete the repo
            repo.git.checkout("master")
            repo.git.branch("-D", current)


@pytest.fixture
def svc_client_with_repo(svc_client_setup):
    """Service client with a remote repository."""
    svc_client, headers, project_id, url_components = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert response.json["result"]

    yield svc_client, deepcopy(headers), project_id, url_components


@pytest.fixture
def svc_protected_repo(svc_client, identity_headers, it_protected_repo_url):
    """Service client with migrated remote protected repository."""
    payload = {
        "git_url": it_protected_repo_url,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    data = {
        "project_id": response.json["result"]["project_id"],
        "skip_template_update": True,
        "skip_docker_update": True,
    }
    svc_client.post("/cache.migrate", data=json.dumps(data), headers=identity_headers)

    yield svc_client, identity_headers, payload, response


@pytest.fixture
def svc_protected_old_repo(svc_synced_client, it_protected_repo_url):
    """Service client with remote protected repository."""
    svc_client, identity_headers, cache, user = svc_synced_client

    payload = {
        "git_url": it_protected_repo_url,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    project_id = response.json["result"]["project_id"]

    yield svc_client, identity_headers, project_id, cache, user
