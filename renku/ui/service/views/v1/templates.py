# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Renku service templates view."""
from flask import request

from renku.ui.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl
from renku.ui.service.controllers.v1.templates import TemplatesCreateProjectCtrl_v2_2, TemplatesReadManifestCtrl_v2_2
from renku.ui.service.serializers.v1.templates import ManifestTemplatesResponseRPC_1_5
from renku.ui.service.views.api_versions import V2_0, V2_1, V2_2, VERSIONS_BEFORE_2_0
from renku.ui.service.views.decorators import accepts_json, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import (
    handle_common_except,
    handle_templates_create_errors,
    handle_templates_read_errors,
)


@handle_common_except
@handle_templates_read_errors
@requires_cache
@requires_identity
def read_manifest_from_template_1_5(user_data, cache):
    """
    Read templates from the manifest file of a template repository.

    ---
    get:
      description: Read templates from the manifest file of a template repository.
      parameters:
        - in: query
          name: url
          required: true
          schema:
            type: string
        - in: query
          name: ref
          schema:
            type: string
        - in: query
          name: depth
          schema:
            type: string
      responses:
        200:
          description: Listing of templates in the repository.
          content:
            application/json:
              schema: ManifestTemplatesResponseRPC
      tags:
        - templates
    """
    ctrl = TemplatesReadManifestCtrl(cache, user_data, dict(request.args))
    ctrl.RESPONSE_SERIALIZER = ManifestTemplatesResponseRPC_1_5()  # type: ignore

    return ctrl.to_response()


@handle_common_except
@handle_templates_read_errors
@requires_cache
@requires_identity
def read_manifest_from_template_v2_2(user_data, cache):
    """
    Read templates from the manifest file of a template repository.

    ---
    get:
      description: Read templates from the manifest file of a template repository.
      parameters:
        - in: query
          name: url
          required: true
          schema:
            type: string
        - in: query
          name: ref
          schema:
            type: string
        - in: query
          name: depth
          schema:
            type: string
      responses:
        200:
          description: Listing of templates in the repository.
          content:
            application/json:
              schema: ManifestTemplatesResponseRPC
      tags:
        - templates
    """
    ctrl = TemplatesReadManifestCtrl_v2_2(cache, user_data, dict(request.args))
    return ctrl.to_response()


@handle_common_except
@handle_templates_create_errors
@accepts_json
@requires_cache
@requires_identity
def create_project_from_template_v2_2(user_data, cache):
    """
    Create a new project starting using a remote template.

    ---
    post:
      description: Create a new project using a remote template.
      requestBody:
        content:
          application/json:
            schema: ProjectTemplateRequest
      responses:
        200:
          description: Details of the created project.
          content:
            application/json:
              schema: ProjectTemplateResponseRPC
      tags:
        - templates
    """
    ctrl = TemplatesCreateProjectCtrl_v2_2(cache, user_data, dict(request.json))  # type: ignore

    return ctrl.to_response()


def add_v1_specific_template_endpoints(templates_blueprint):
    """Add v1 only endpoints to blueprint."""
    templates_blueprint.route(
        "/templates.read_manifest", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_0
    )(read_manifest_from_template_1_5)
    return templates_blueprint


def add_v2_specific_template_endpoints(templates_blueprint):
    """Add old v2 endpoints to blueprint."""
    templates_blueprint.route(
        "/templates.read_manifest", methods=["GET"], provide_automatic_options=False, versions=[V2_0, V2_1, V2_2]
    )(read_manifest_from_template_v2_2)
    templates_blueprint.route(
        "/templates.create_project", methods=["POST"], provide_automatic_options=False, versions=[V2_0, V2_1, V2_2]
    )(create_project_from_template_v2_2)
    return templates_blueprint
