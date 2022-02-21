# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 -Swiss Data Science Center (SDSC)
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

from renku.service.config import SVC_ERROR_PROGRAMMING
from renku.service.errors import (
    IntermittentProjectIdError,
    ProgramContentTypeError,
    UserAnonymousError,
    UserNonRenkuProjectError,
    UserOutdatedProjectError,
    UserRepoNoAccessError,
    UserRepoUrlInvalidError,
)
from tests.fixtures.config import IT_PROTECTED_REMOTE_REPO_URL
from tests.service.views.test_dataset_views import assert_rpc_response
from tests.utils import retry_failed


@pytest.mark.service
def test_allowed_methods_exc(service_allowed_endpoint):
    """Check invalid methods amongst the allowed service methods for some endpoints."""
    methods, request, svc_client = service_allowed_endpoint

    method = request["allowed_method"]
    methods.pop(method)

    for method, fn in methods.items():
        response = fn(request["url"])
        assert_rpc_response(response, "error")
        assert SVC_ERROR_PROGRAMMING + 405 == response.json["error"]["code"]


@pytest.mark.service
def test_unallowed_methods_exc(service_unallowed_endpoint):
    """Check unallowed methods for some endpoint."""
    methods, request, svc_client = service_unallowed_endpoint

    for method, fn in methods.items():
        response = fn(request["url"], content_type="application/json")
        assert_rpc_response(response, "error")
        assert SVC_ERROR_PROGRAMMING + 405 == response.json["error"]["code"]


@pytest.mark.service
def test_auth_headers_exc(service_allowed_endpoint):
    """Check correct headers for every endpoint."""
    methods, request, svc_client = service_allowed_endpoint

    method = request["allowed_method"]
    client_method = methods.pop(method)

    response = client_method(request["url"], headers=request["headers"])
    assert_rpc_response(response, "error")
    assert UserAnonymousError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_content_type_headers_exc(svc_client_with_repo):
    """Verify exceptions are triggered when missing data."""

    svc_client, headers, project_id, _ = svc_client_with_repo
    headers["Content-Type"] = "Fake"

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
    assert_rpc_response(response, "error")
    assert ProgramContentTypeError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_migration_required_flag(svc_client_setup):
    """Check migration required failure."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    payload = {
        "project_id": project_id,
        "name": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert UserOutdatedProjectError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize(
    "url,verb,code",
    [
        ("/this_endpoint_does_not_exists", "POST", SVC_ERROR_PROGRAMMING + 404),
        ("/this_endpoint_does_not_exists", "GET", SVC_ERROR_PROGRAMMING + 404),
        ("/version", "POST", SVC_ERROR_PROGRAMMING + 405),
        ("/version", "WRONG_VERB", SVC_ERROR_PROGRAMMING + 405),
        ("/this_endpoint_does_not_exists", "WRONG_VERB", SVC_ERROR_PROGRAMMING + 404),
    ],
)
def test_http_common_errors(url, verb, code, svc_client):
    """Check migration required failure."""
    # NOTE: the service always uses http error codes with no payload in case of unsupported verbs
    if verb == "POST":
        fn = svc_client.post
    elif verb == "GET":
        fn = svc_client.get
    elif verb == "WRONG_VERB":
        fn = svc_client.options

    response = fn(url)
    assert_rpc_response(response, "error")
    assert response.json["error"]["code"] == code


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_project_uninitialized(svc_client, it_non_renku_repo_url, identity_headers):
    """Check migration required failure."""
    payload = {"git_url": it_non_renku_repo_url}

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert "result" in response.json
    assert "error" not in response.json

    project_id = response.json["result"]["project_id"]
    initialized = response.json["result"]["initialized"]

    assert not initialized

    payload = {
        "project_id": project_id,
        "name": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers)

    assert_rpc_response(response, "error")
    assert UserNonRenkuProjectError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_project_no_commits(svc_client, it_no_commit_repo_url, identity_headers):
    """Check migration required failure."""
    payload = {"git_url": it_no_commit_repo_url}
    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert_rpc_response(response)
    project_id = response.json["result"]["project_id"]
    initialized = response.json["result"]["initialized"]
    assert not initialized

    payload = {
        "project_id": project_id,
        "name": uuid.uuid4().hex,
    }
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers)

    assert_rpc_response(response, "error")
    assert UserNonRenkuProjectError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize(
    "git_url",
    [
        "https://github.com",
        "https://github.com/SwissDataScienceCenter",
        "https://test.com/test2/test3",
        "https://www.test.com/test2/test3",
    ],
)
def test_invalid_git_remote(git_url, svc_client_with_templates):
    """Test error on invalid repository URL while reading template manifest file."""
    svc_client, headers, template_params = svc_client_with_templates
    template_params["url"] = git_url
    response = svc_client.get("/templates.read_manifest", query_string=template_params, headers=headers)

    assert_rpc_response(response, "error")
    assert UserRepoUrlInvalidError().code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_invalid_project_id(svc_client_with_repo):
    """Test error on wrong project_id while showing project metadata."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    show_payload = {
        "project_id": project_id + "12345",
    }
    response = svc_client.post("/project.show", data=json.dumps(show_payload), headers=headers)

    assert_rpc_response(response, "error")
    assert IntermittentProjectIdError().code == response.json["error"]["code"]


@pytest.mark.integration
@pytest.mark.service
def test_user_without_permissons(svc_client_with_user):
    """Test getting lock status for a locked project."""
    svc_client, headers, _, _ = svc_client_with_user
    headers["Authorization"] = "Bearer 123abc"

    response = svc_client.post(
        "/project.show",
        data=json.dumps({"migrate_project": True, "git_url": IT_PROTECTED_REMOTE_REPO_URL}),
        headers=headers,
    )

    assert_rpc_response(response, "error")
    assert UserRepoNoAccessError().code == response.json["error"]["code"]
