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
"""Renku service cache views."""
from flask import request

from renku.ui.service.controllers.cache_migrations_check import MigrationsCheckCtrl
from renku.ui.service.gateways.gitlab_api_provider import GitlabAPIProvider
from renku.ui.service.serializers.v0_9.cache import ProjectMigrationCheckResponseRPC_0_9
from renku.ui.service.views.api_versions import V0_9
from renku.ui.service.views.decorators import optional_identity, requires_cache
from renku.ui.service.views.error_handlers import handle_common_except


@handle_common_except
@requires_cache
@optional_identity
def migration_check_project_view_0_9(user_data, cache):
    """
    Retrieve migration information for a project.

    ---
    get:
      description: Retrieve migration information for a project.
      parameters:
        - in: query
          schema: ProjectMigrationCheckRequest
      responses:
        200:
          description: Information about required migrations for the project.
          content:
            application/json:
              schema: ProjectMigrationCheckResponseRPC_0_9
      tags:
        - cache
    """
    ctrl = MigrationsCheckCtrl(cache, user_data, dict(request.args), GitlabAPIProvider())
    ctrl.RESPONSE_SERIALIZER = ProjectMigrationCheckResponseRPC_0_9()  # type: ignore
    return ctrl.to_response()


def add_v0_9_specific_endpoints(cache_blueprint):
    """Add v0.9 only endpoints to blueprint."""
    cache_blueprint.route("/cache.migrations_check", methods=["GET"], provide_automatic_options=False, versions=[V0_9])(
        migration_check_project_view_0_9
    )
    return cache_blueprint
