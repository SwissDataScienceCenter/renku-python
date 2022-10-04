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


@pytest.mark.remote_repo("workflow")
@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_workflow_plans_view(svc_client_with_repo):
    """Check listing of plans."""
    svc_client, headers, project_id, _ = svc_client_with_repo

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

    reexecuted = next(p for p in response.json["result"]["plans"] if p["name"] == "some-step")
    assert 2 == reexecuted["number_of_executions"]
    assert reexecuted["touches_existing_files"]
    assert "2022-10-04T13:05:44+02:00" == reexecuted["last_executed"]

    non_recent = next(p for p in response.json["result"]["plans"] if p["name"] == "deleted-outputs")
    assert not non_recent["touches_existing_files"]
    assert 2 == non_recent["number_of_executions"]

    composite = next(p for p in response.json["result"]["plans"] if p["name"] == "composite1")

    assert not composite["number_of_executions"]
    assert composite["touches_existing_files"]
    assert not composite["last_executed"]


@pytest.mark.parametrize(
    "plan_id,expected_fields,executions,touches_files,latest",
    [
        (
            "/plans/6943ee7f620c4f2d8f75b657e9d9e765",
            {
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
            },
            None,
            True,
            "/plans/6943ee7f620c4f2d8f75b657e9d9e765",
        ),
        (
            "/plans/56b3149fc21e43bea9b73b887934e084",
            {
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
            },
            1,
            True,
            "/plans/56b3149fc21e43bea9b73b887934e084",
        ),
        (
            "/plans/7c4e51b1ac7143c287b5b0001d843310",
            {
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
            },
            2,
            False,
            "/plans/6fee5bb01de449f6bc39d7e7cd23f4c2",
        ),
    ],
)
@pytest.mark.remote_repo("workflow")
@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_show_workflow_plans_view(plan_id, expected_fields, executions, touches_files, latest, svc_client_with_repo):
    """Check showing of plans."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = {"project_id": project_id, "plan_id": plan_id}

    response = svc_client.get("/workflow_plans.show", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert expected_fields == set(response.json["result"].keys())

    if executions is not None:
        assert executions == response.json["result"]["number_of_executions"]
    else:
        assert "number_of_executions" not in response.json["result"]

    assert touches_files == response.json["result"]["touches_existing_files"]
    assert latest == response.json["result"]["latest"]
