# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Renku service dataset view tests."""

import pytest

from tests.utils import assert_rpc_response, retry_failed


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_workflow_plans_view(svc_workflow_repo):
    """Check listing of plans."""
    svc_client, headers, project_id = svc_workflow_repo

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/workflow_plans.list", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {"plans"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["plans"])
    assert {
        "children",
        "created",
        "creators",
        "description",
        "id",
        "keywords",
        "name",
        "number_of_executions",
        "touches_existing_files",
        "type",
        "last_executed",
    } == set(response.json["result"]["plans"][0].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_show_workflow_plans_view(svc_workflow_repo):
    """Check showing of plans."""
    svc_client, headers, project_id = svc_workflow_repo

    params = {"project_id": project_id, "plan_id": "/plans/3398f0a970774e2d82b1791190de85d0"}

    response = svc_client.get("/workflow_plans.show", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {
        "annotations",
        "creators",
        "created",
        "description",
        "full_command",
        "id",
        "inputs",
        "keywords",
        "latest",
        "name",
        "outputs",
        "parameters",
        "success_codes",
        "touches_existing_files",
        "number_of_executions",
        "last_executed",
        "type",
    } == set(response.json["result"].keys())

    params = {"project_id": project_id, "plan_id": "/plans/3f05dcdf8d584396af8d12ad711cc59e"}

    response = svc_client.get("/workflow_plans.show", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {
        "annotations",
        "creators",
        "created",
        "description",
        "id",
        "mappings",
        "keywords",
        "latest",
        "name",
        "links",
        "plans",
        "touches_existing_files",
        "type",
    } == set(response.json["result"].keys())
