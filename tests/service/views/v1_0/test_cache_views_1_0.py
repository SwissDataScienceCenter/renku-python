# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Renku service cache view tests."""
import json
import uuid

import pytest

from renku.core.metadata.repository import Repository
from tests.utils import retry_failed


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_1_0(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.post(
        "/1.0/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )
    assert "warnings" not in response.json["result"]
    assert "errors" not in response.json["result"]


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_job(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, is_delayed=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_remote(svc_client, identity_headers, it_remote_repo_url):
    """Check execution of all migrations."""

    response = svc_client.post(
        "/cache.migrate",
        data=json.dumps(dict(git_url=it_remote_repo_url, skip_docker_update=True)),
        headers=identity_headers,
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local(svc_client_setup):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.get("/1.1/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]
    assert "template_source" in response.json["result"]["template_status"]
    assert "template_ref" in response.json["result"]["template_status"]
    assert "template_id" in response.json["result"]["template_status"]
    assert "automated_template_update" in response.json["result"]["template_status"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote(svc_client, identity_headers, it_remote_repo_url):
    """Check if migrations are required for a remote project."""
    response = svc_client.get(
        "/1.1/cache.migrations_check", query_string=dict(git_url=it_remote_repo_url), headers=identity_headers
    )

    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]


@pytest.mark.service
@pytest.mark.integration
def test_check_no_migrations(svc_client_with_repo):
    """Check if migrations are not required."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    response = svc_client.get("/1.1/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

    assert 200 == response.status_code

    assert not response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_cache_is_reset_after_failing_push(svc_protected_old_repo):
    """Check cache state is reset after pushing to a protected branch fails."""
    svc_client, headers, project_id, cache, user = svc_protected_old_repo

    project = cache.get_project(user, project_id)
    repository = Repository(path=project.abs_path)
    commit_sha_before = repository.head.commit.hexsha
    active_branch_before = repository.active_branch.name

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]

    project = cache.get_project(user, project_id)
    repository = Repository(path=project.abs_path)

    assert commit_sha_before == repository.head.commit.hexsha
    assert active_branch_before == repository.active_branch.name


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_migrating_protected_branch(svc_protected_old_repo):
    """Check migrating on a protected branch does not change cache state."""
    svc_client, headers, project_id, _, _ = svc_protected_old_repo

    response = svc_client.get("/1.1/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code
    assert response.json["result"]["core_compatibility_status"]["migration_required"]

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )

    response = svc_client.get("/1.1/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code
    assert response.json["result"]["core_compatibility_status"]["migration_required"]


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_cache_gets_synchronized(
    local_remote_repository, directory_tree, quick_cache_synchronization, client_database_injection_manager
):
    """Test that the cache stays synchronized with the remote repository."""
    from renku.core.management.client import LocalClient
    from renku.core.models.provenance.agent import Person

    svc_client, identity_headers, project_id, remote_repo, remote_repo_checkout = local_remote_repository

    client = LocalClient(remote_repo_checkout.path)

    with client_database_injection_manager(client):
        with client.commit(commit_message="Create dataset"):
            with client.with_dataset(name="my_dataset", create=True, commit_database=True) as dataset:
                dataset.creators = [Person(name="me", email="me@example.com", id="me_id")]

    remote_repo_checkout.push()
    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params, headers=identity_headers)
    assert response
    assert 200 == response.status_code

    assert {"datasets"} == set(response.json["result"].keys()), response.json
    assert 1 == len(response.json["result"]["datasets"])

    payload = {
        "project_id": project_id,
        "name": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert 200 == response.status_code
    assert {"name", "remote_branch"} == set(response.json["result"].keys())

    remote_repo_checkout.pull()

    with client_database_injection_manager(client):
        datasets = client.datasets.values()
        assert 2 == len(datasets)

    assert any(d.name == "my_dataset" for d in datasets)
    assert any(d.name == payload["name"] for d in datasets)


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote_anonymous(svc_client, it_remote_public_repo_url):
    """Test anonymous users can check for migration of public projects."""
    response = svc_client.get(
        "/1.1/cache.migrations_check", query_string={"git_url": it_remote_public_repo_url}, headers={}
    )

    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"] is True
