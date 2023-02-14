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
"""Renku service project view."""
from flask import request

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.project_edit import ProjectEditCtrl
from renku.ui.service.controllers.project_lock_status import ProjectLockStatusCtrl
from renku.ui.service.controllers.project_show import ProjectShowCtrl
from renku.ui.service.views.api_versions import VERSIONS_FROM_V1_0, VersionedBlueprint
from renku.ui.service.views.decorators import accepts_json, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import handle_common_except, handle_project_write_errors

PROJECT_BLUEPRINT_TAG = "project"
project_blueprint = VersionedBlueprint(PROJECT_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@project_blueprint.route(
    "/project.show", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_0
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def show_project_view(user_data, cache):
    """
    Show project metadata view.

    ---
    post:
      description: Show project metadata.
      requestBody:
        content:
          application/json:
            schema: ProjectShowRequest
      responses:
        200:
          description: Metadata of the project.
          content:
            application/json:
              schema: ProjectShowResponseRPC
      tags:
        - project
    """
    return ProjectShowCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@project_blueprint.route(
    "/project.edit", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_0
)
@handle_common_except
@handle_project_write_errors
@accepts_json
@requires_cache
@requires_identity
def edit_project_view(user_data, cache):
    """
    Edit project metadata view.

    Not passing a field leaves it unchanged.

    ---
    post:
      description: Edit project metadata.
      requestBody:
        content:
          application/json:
            schema: ProjectEditRequest
      responses:
        200:
          description: Status of the requested project edits.
          content:
            application/json:
              schema: ProjectEditResponseRPC
      tags:
        - project
    """
    return ProjectEditCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@project_blueprint.route(
    "/project.lock_status", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_0
)
@handle_common_except
@requires_cache
@requires_identity
def get_project_lock_status(user_data, cache):
    """
    Check whether a project is locked for writing or not.

    ---
    get:
      description: Get project write-lock status.
      parameters:
        - in: query
          schema: ProjectLockStatusRequest
      responses:
        200:
          description: Status of the project write-lock.
          content:
            application/json:
              schema: ProjectLockStatusResponseRPC
      tags:
        - project
    """
    return ProjectLockStatusCtrl(cache, user_data, dict(request.args)).to_response()
