# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
"""Renku service config view tests."""

import json

import pytest

from renku.service.errors import IntermittentSettingExistsError, ProgramProjectCorruptError, UserNonRenkuProjectError
from tests.utils import retry_failed


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_show(svc_client_with_repo):
    """Check config show view."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/config.show", query_string=params, headers=headers)

    assert {"result"} == set(response.json.keys())
    keys = {"interactive.default_url", "renku.autocommit_lfs", "renku.lfs_threshold"}
    assert keys == set(response.json["result"]["config"].keys())
    assert keys == set(response.json["result"]["default"].keys())
    assert 200 == response.status_code


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_show_errors(svc_client_with_user, it_non_renku_repo_url):
    """Check config show view."""
    svc_client, headers, _, _ = svc_client_with_user
    params = {"git_url": it_non_renku_repo_url}

    response = svc_client.get("/config.show", query_string=params, headers=headers)

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserNonRenkuProjectError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_show_remote(svc_client_with_repo, it_remote_repo_url):
    """Check config show view."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = dict(git_url=it_remote_repo_url)

    response = svc_client.get("/config.show", query_string=params, headers=headers)

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    keys = {"interactive.default_url", "renku.autocommit_lfs", "renku.lfs_threshold"}
    assert keys == set(response.json["result"]["config"].keys())
    assert keys == set(response.json["result"]["default"].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_set(svc_client_with_repo):
    """Check config set view."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "config": {
            "lfs_threshold": "1b",
            "renku.autocommit_lfs": "true",
            "interactive.default_url": "/not_lab",
            "interactive.dummy": "dummy-value",
        },
    }

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers)
    assert 200 == response.status_code
    assert {"error"} != set(response.json.keys())

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/config.show", query_string=params, headers=headers)

    assert {"result"} == set(response.json.keys())
    assert "1b" == response.json["result"]["config"]["renku.lfs_threshold"]
    assert "true" == response.json["result"]["config"]["renku.autocommit_lfs"]
    assert "/not_lab" == response.json["result"]["config"]["interactive.default_url"]
    assert "dummy-value" == response.json["result"]["config"]["interactive.dummy"]
    assert 200 == response.status_code

    payload = {
        "project_id": project_id,
        "config": {"lfs_threshold": None, "interactive.default_url": "/still_not_lab", "interactive.dummy": None},
    }

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers)
    assert 200 == response.status_code
    assert {"error"} != set(response.json.keys())

    response = svc_client.get("/config.show", query_string=params, headers=headers)

    assert {"result"} == set(response.json.keys())
    assert "100kb" == response.json["result"]["config"]["renku.lfs_threshold"]
    assert "/still_not_lab" == response.json["result"]["config"]["interactive.default_url"]
    assert "interactive.dummy" not in response.json["result"]["config"].keys()
    assert 200 == response.status_code


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_set_failures(svc_client_with_repo):
    """Check errors triggered while invoking config set."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    # NOTE: remove a non existing value
    non_existing_param = "NON_EXISTING"
    payload = {
        "project_id": project_id,
        "config": {
            non_existing_param: None,
        },
    }

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers)

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert IntermittentSettingExistsError.code == response.json["error"]["code"]
    assert non_existing_param in response.json["error"]["devMessage"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_config_view_set_and_show_failures(svc_client_with_repo):
    """Check errors triggered while invoking config set."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    # NOTE: use sections with wrong chars introduces a readin error. Should we handle it at write time?
    payload = {
        "project_id": project_id,
        "config": {".NON_EXISTING": "test"},
    }

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers)

    assert 200 == response.status_code
    assert {"error"} != set(response.json.keys())

    response = svc_client.get("/config.show", query_string={"project_id": project_id}, headers=headers)

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert ProgramProjectCorruptError.code == response.json["error"]["code"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_config_set_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed config set."""
    config = {
        "lfs_threshold": "1b",
        "renku.autocommit_lfs": "true",
        "interactive.default_url": "/not_lab",
        "interactive.dummy": "dummy-value",
    }

    response = svc_client.post(
        "/config.set",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True, config=config)),
        headers=identity_headers,
    )

    assert 200 == response.status_code
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]
