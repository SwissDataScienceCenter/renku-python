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
"""Renku service cache views for v1."""
from dataclasses import asdict

from flask import request

from renku.core.errors import AuthenticationError, ProjectNotFound
from renku.infrastructure.gitlab_api_provider import GitlabAPIProvider
from renku.ui.service.controllers.cache_migrate_project import MigrateProjectCtrl
from renku.ui.service.controllers.cache_migrations_check import MigrationsCheckCtrl
from renku.ui.service.serializers.v1.cache import ProjectMigrateResponseRPC_1_0, ProjectMigrationCheckResponseRPC_1_5
from renku.ui.service.views import result_response
from renku.ui.service.views.api_versions import VERSIONS_BEFORE_1_1, VERSIONS_BEFORE_2_0
from renku.ui.service.views.decorators import accepts_json, optional_identity, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import (
    handle_common_except,
    handle_migration_read_errors,
    handle_migration_write_errors,
)


@handle_common_except
@handle_migration_write_errors
@accepts_json
@requires_cache
@requires_identity
def migrate_project_view_1_0(user_data, cache):
    """
    Migrate a project.

    ---
    post:
      description: Migrate a project.
      requestBody:
        content:
          application/json:
            schema: ProjectMigrateRequest
      responses:
        200:
          description: Migration status.
          content:
            application/json:
              schema: ProjectMigrateResponseRPC_1_0
      tags:
        - cache
    """
    ctrl = MigrateProjectCtrl(cache, user_data, dict(request.json))  # type: ignore
    ctrl.RESPONSE_SERIALIZER = ProjectMigrateResponseRPC_1_0()  # type: ignore
    return ctrl.to_response()


@handle_common_except
@handle_migration_read_errors
@requires_cache
@optional_identity
def migration_check_project_view_1_5(user_data, cache):
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
              schema: ProjectMigrationCheckResponseRPC_1_5
      tags:
        - cache
    """

    from flask import jsonify

    from renku.ui.service.serializers.rpc import JsonRPCResponse
    from renku.ui.service.views.error_handlers import pretty_print_error

    ctrl = MigrationsCheckCtrl(cache, user_data, dict(request.args), GitlabAPIProvider)

    if "project_id" in ctrl.context:  # type: ignore
        result = ctrl.execute_op()
    else:
        # NOTE: use quick flow but fallback to regular flow in case of unexpected exceptions
        try:
            result = ctrl._fast_op_without_cache()
        except (AuthenticationError, ProjectNotFound):
            raise
        except BaseException:
            result = ctrl.execute_op()

    if isinstance(result.core_compatibility_status, Exception):
        return jsonify(JsonRPCResponse().dump({"error": pretty_print_error(result.core_compatibility_status)}))

    if isinstance(result.template_status, Exception):
        return jsonify(JsonRPCResponse().dump({"error": pretty_print_error(result.template_status)}))

    if isinstance(result.dockerfile_renku_status, Exception):
        return jsonify(JsonRPCResponse().dump({"error": pretty_print_error(result.dockerfile_renku_status)}))

    return result_response(ProjectMigrationCheckResponseRPC_1_5(), asdict(result))


def add_v1_specific_cache_endpoints(cache_blueprint):
    """Add v1 only endpoints to blueprint."""
    cache_blueprint.route(
        "/cache.migrate", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_1_1
    )(migrate_project_view_1_0)
    cache_blueprint.route(
        "/cache.migrations_check", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_0
    )(migration_check_project_view_1_5)
    return cache_blueprint
