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
"""Renku service fixtures for endpoint testing."""
import json

import pytest


@pytest.fixture(
    params=[
        {
            "url": "/cache.files_list",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {"url": "/cache.files_upload", "allowed_method": "POST", "headers": {}},
        {
            "url": "/cache.project_clone",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {
            "url": "/cache.project_list",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {
            "url": "/datasets.add",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {
            "url": "/datasets.create",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {
            "url": "/templates.read_manifest",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
        {
            "url": "/templates.create_project",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json"},
        },
    ]
)
def service_allowed_endpoint(request, svc_client, mock_redis):
    """Ensure allowed methods and correct headers."""
    methods = {
        "GET": svc_client.get,
        "POST": svc_client.post,
        "HEAD": svc_client.head,
        "PUT": svc_client.put,
        "DELETE": svc_client.delete,
        "OPTIONS": svc_client.options,
        "TRACE": svc_client.trace,
        "PATCH": svc_client.patch,
    }

    yield methods, request.param, svc_client


@pytest.fixture
def unlink_file_setup(svc_client_with_repo):
    """Setup for testing of unlinking of a file."""
    from tests.utils import make_dataset_add_payload

    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = make_dataset_add_payload(project_id, [("file_path", "README.md")])
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert 200 == response.status_code

    unlink_payload = {
        "project_id": project_id,
        "name": response.json["result"]["name"],
        "include_filters": [response.json["result"]["files"][0]["file_path"]],
    }

    yield svc_client, headers, unlink_payload
