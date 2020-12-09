# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Check exceptions raised on views."""
import json
import uuid

import pytest
from flaky import flaky

from renku.service.config import INVALID_HEADERS_ERROR_CODE, INVALID_PARAMS_ERROR_CODE


@pytest.mark.service
def test_allowed_methods_exc(service_allowed_endpoint):
    """Check allowed methods for every endpoint."""
    methods, request, svc_client = service_allowed_endpoint

    method = request["allowed_method"]
    if method == "GET":  # if GET remove sister method HEAD
        methods.pop(method)
        methods.pop("HEAD")
    else:
        methods.pop(method)

    for method, fn in methods.items():
        response = fn(request["url"])
        assert 405 == response.status_code


@pytest.mark.service
def test_auth_headers_exc(service_allowed_endpoint):
    """Check correct headers for every endpoint."""
    methods, request, svc_client = service_allowed_endpoint

    method = request["allowed_method"]
    if method == "GET":  # if GET remove sister method HEAD
        client_method = methods.pop(method)
        methods.pop("HEAD")
    else:
        client_method = methods.pop(method)

    response = client_method(request["url"], headers=request["headers"],)

    assert 200 == response.status_code
    assert response.json["error"]["code"] in [INVALID_HEADERS_ERROR_CODE, INVALID_PARAMS_ERROR_CODE]
    assert response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_migration_required_flag(svc_client_setup):
    """Check migration required failure."""
    svc_client, headers, project_id, _ = svc_client_setup

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response.json["error"]["migration_required"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_project_uninitialized(svc_client, it_remote_non_renku_repo, identity_headers):
    """Check migration required failure."""
    payload = {"git_url": it_remote_non_renku_repo}

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert "result" in response.json
    assert "error" not in response.json

    project_id = response.json["result"]["project_id"]
    initialized = response.json["result"]["initialized"]

    assert not initialized

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert "error" in response.json
    assert "project_initialization_required" in response.json["error"]
    assert response.json["error"]["project_initialization_required"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_project_no_commits(svc_client, it_remote_no_commits_repo, identity_headers):
    """Check migration required failure."""
    payload = {"git_url": it_remote_no_commits_repo}

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert "result" in response.json
    assert "error" not in response.json

    project_id = response.json["result"]["project_id"]
    initialized = response.json["result"]["initialized"]

    assert not initialized

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert "error" in response.json
    assert "project_initialization_required" in response.json["error"]
    assert response.json["error"]["project_initialization_required"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    "git_url,expected",
    [
        ("https://github.com", {"error": {"code": -32602, "reason": "Validation error: `schema` - Invalid `git_url`"}}),
        (
            "https://github.com/SwissDataScienceCenter",
            {"error": {"code": -32602, "reason": "Validation error: `schema` - Invalid `git_url`"}},
        ),
        ("https://test.com/test2/test3", {"error": {"code": -32001, "reason": "Repository could not be found"}}),
        ("https://www.test.com/test2/test3", {"error": {"code": -32001, "reason": "Repository could not be found"}}),
    ],
)
def test_invalid_git_remote(git_url, expected, svc_client_with_templates):
    """Check reading manifest template."""
    svc_client, headers, template_params = svc_client_with_templates
    template_params["url"] = git_url
    response = svc_client.get("/templates.read_manifest", query_string=template_params, headers=headers)

    assert response
    assert expected == response.json
