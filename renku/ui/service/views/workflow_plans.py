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
"""Renku service workflow plans view."""
from flask import request

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.workflow_plans_export import WorkflowPlansExportCtrl
from renku.ui.service.controllers.workflow_plans_list import WorkflowPlansListCtrl
from renku.ui.service.controllers.workflow_plans_show import WorkflowPlansShowCtrl
from renku.ui.service.views.api_versions import V1_5, VERSIONS_FROM_V1_4, VersionedBlueprint
from renku.ui.service.views.decorators import optional_identity, requires_cache
from renku.ui.service.views.error_handlers import handle_common_except, handle_workflow_errors

PLAN_BLUEPRINT_TAG = "workflow plans"
workflow_plans_blueprint = VersionedBlueprint(PLAN_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@workflow_plans_blueprint.route(
    "/workflow_plans.list", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_4
)
@handle_common_except
@handle_workflow_errors
@requires_cache
@optional_identity
def list_plans_view(user_data, cache):
    """
    List all plans in a project.

    ---
    get:
      description: List all plans in a project.
      parameters:
        - in: query
          schema: WorkflowPlansListRequest
      responses:
        200:
          description: Listing of all plans in a project.
          content:
            application/json:
              schema: WorkflowPlansListResponseRPC
      tags:
        - workflow plans
    """
    return WorkflowPlansListCtrl(cache, user_data, dict(request.args)).to_response()


@workflow_plans_blueprint.route(
    "/workflow_plans.show", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_4
)
@handle_common_except
@handle_workflow_errors
@requires_cache
@optional_identity
def show_plan_view(user_data, cache):
    """
    Show details of a plan.

    ---
    get:
      description: Show details of a plan.
      parameters:
        - in: query
          schema: WorkflowPlansShowRequest
      responses:
        200:
          description: The details of the plan.
          content:
            application/json:
              schema:
                WorkflowPlansShowResponseRPC
      tags:
        - workflow plans
    """
    return WorkflowPlansShowCtrl(cache, user_data, dict(request.args)).to_response()


@workflow_plans_blueprint.route(
    "/workflow_plans.export", methods=["POST"], provide_automatic_options=False, versions=[V1_5]
)
@handle_common_except
@handle_workflow_errors
@requires_cache
@optional_identity
def export_plan_view(user_data, cache):
    """
    Export a workflow.

    ---
    post:
      description: Export a workflow to a specific format.
      requestBody:
        content:
          application/json:
            schema: WorkflowPlansExportRequest
      responses:
        200:
          description: The exported plan.
          content:
            application/json:
              schema: WorkflowPlansExportResponseRPC
      tags:
        - workflow plans
    """
    return WorkflowPlansExportCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore
