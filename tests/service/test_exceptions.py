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
"""Renku service exception tests for all endpoints."""
import json
import uuid

import pytest
from flaky import flaky

from renku.service.config import INVALID_HEADERS_ERROR_CODE


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
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    err_message = "user identification is incorrect or missing"
    assert err_message == response.json["error"]["reason"]


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
