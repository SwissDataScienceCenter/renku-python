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
"""Renku service templates view."""
from flask import request

from renku.ui.service.controllers.templates_read_manifest import TemplatesReadManifestCtrl
from renku.ui.service.serializers.v1.templates import ManifestTemplatesResponseRPC_1_5
from renku.ui.service.views.api_versions import V1_0, V1_1, V1_2, V1_3, V1_4, V1_5
from renku.ui.service.views.decorators import requires_cache, requires_identity
from renku.ui.service.views.error_handlers import handle_common_except, handle_templates_read_errors


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


def add_v1_specific_endpoints(templates_blueprint):
    """Add v1 only endpoints to blueprint."""
    templates_blueprint.route(
        "/templates.read_manifest",
        methods=["GET"],
        provide_automatic_options=False,
        versions=[V1_0, V1_1, V1_2, V1_3, V1_4, V1_5],
    )(read_manifest_from_template_1_5)
    return templates_blueprint
