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
from flaky import flaky


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_config_view_show(svc_client_with_repo):
    """Check config show view."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/config.show", query_string=params, headers=headers,)

    assert {"result"} == set(response.json.keys())
    keys = {"interactive.default_url", "renku.autocommit_lfs", "renku.lfs_threshold"}
    assert keys == set(response.json["result"]["config"].keys())
    assert keys == set(response.json["result"]["default"].keys())
    assert 200 == response.status_code


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_config_view_show_remote(svc_client_with_repo, it_remote_repo):
    """Check config show view."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = dict(git_url=it_remote_repo)

    response = svc_client.get("/config.show", query_string=params, headers=headers,)

    assert {"result"} == set(response.json.keys())
    keys = {"interactive.default_url", "renku.autocommit_lfs", "renku.lfs_threshold"}
    assert keys == set(response.json["result"]["config"].keys())
    assert keys == set(response.json["result"]["default"].keys())
    assert 200 == response.status_code


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
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

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers,)
    assert 200 == response.status_code
    assert {"error"} != set(response.json.keys())

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/config.show", query_string=params, headers=headers,)

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

    response = svc_client.post("/config.set", data=json.dumps(payload), headers=headers,)
    assert 200 == response.status_code
    assert {"error"} != set(response.json.keys())

    response = svc_client.get("/config.show", query_string=params, headers=headers,)

    assert {"result"} == set(response.json.keys())
    assert "100kb" == response.json["result"]["config"]["renku.lfs_threshold"]
    assert "/still_not_lab" == response.json["result"]["config"]["interactive.default_url"]
    assert "interactive.dummy" not in response.json["result"]["config"].keys()
    assert 200 == response.status_code
