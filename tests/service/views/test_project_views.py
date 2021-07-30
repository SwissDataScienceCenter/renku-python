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
"""Renku service project view tests."""
import json
import re

import pytest
from flaky import flaky


def assert_rpc_response(response, with_key="result"):
    """Check rpc result in response."""
    assert response and 200 == response.status_code

    response_text = re.sub(r"http\S+", "", json.dumps(response.json))
    assert with_key in response_text


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_edit_project_view(svc_client_with_repo):
    """Test editing project metadata."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    edit_payload = {
        "project_id": project_id,
        "description": "my new title",
        "creator": {"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"},
    }
    response = svc_client.post("/project.edit", data=json.dumps(edit_payload), headers=headers)

    assert response
    assert_rpc_response(response)

    assert {"warning", "edited", "remote_branch"} == set(response.json["result"])
    assert {
        "description": "my new title",
        "creator": {"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"},
    } == response.json["result"]["edited"]


@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=10, min_passes=1)
def test_remote_edit_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed edit."""
    response = svc_client.post(
        "/project.edit",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True)),
        headers=identity_headers,
    )

    assert 200 == response.status_code
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]
