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

import pytest


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local_0_9(svc_client_setup):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.get("/v0.9/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_version"]
    assert response.json["result"]["latest_version"]
    assert "template_source" in response.json["result"]
    assert "template_ref" in response.json["result"]
    assert "template_id" in response.json["result"]
    assert "automated_template_update" in response.json["result"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote_0_9(svc_client, identity_headers, it_remote_repo_url):
    """Check if migrations are required for a remote project."""
    response = svc_client.get(
        "/v0.9/cache.migrations_check", query_string=dict(git_url=it_remote_repo_url), headers=identity_headers
    )

    assert 200 == response.status_code

    assert response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_version"]
    assert response.json["result"]["latest_version"]


@pytest.mark.service
@pytest.mark.integration
def test_check_no_migrations_0_9(svc_client_with_repo):
    """Check if migrations are not required."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    response = svc_client.get("/v0.9/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

    assert 200 == response.status_code
    assert not response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]
