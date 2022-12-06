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

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.cache_files_delete_chunks import DeleteFileChunksCtrl
from renku.ui.service.controllers.cache_files_upload import UploadFilesCtrl
from renku.ui.service.controllers.cache_list_projects import ListProjectsCtrl
from renku.ui.service.controllers.cache_list_uploaded import ListUploadedFilesCtrl
from renku.ui.service.controllers.cache_migrate_project import MigrateProjectCtrl
from renku.ui.service.controllers.cache_migrations_check import MigrationsCheckCtrl
from renku.ui.service.controllers.cache_project_clone import ProjectCloneCtrl
from renku.ui.service.gateways.gitlab_api_provider import GitlabAPIProvider
from renku.ui.service.views.api_versions import ALL_VERSIONS, VERSIONS_FROM_V1_0, VERSIONS_FROM_V1_1, VersionedBlueprint
from renku.ui.service.views.decorators import accepts_json, optional_identity, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import (
    handle_common_except,
    handle_migration_read_errors,
    handle_migration_write_errors,
)
from renku.ui.service.views.v0_9.cache import add_v0_9_specific_endpoints
from renku.ui.service.views.v1_0.cache import add_v1_0_specific_endpoints

CACHE_BLUEPRINT_TAG = "cache"
cache_blueprint = VersionedBlueprint("cache", __name__, url_prefix=SERVICE_PREFIX)


@cache_blueprint.route("/cache.files_list", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@requires_identity
def list_uploaded_files_view(user_data, cache):
    """
    List uploaded files.

    ---
    get:
      description: List uploaded files ready to be added to projects.
      responses:
        200:
          description: "Return a list of files."
          content:
            application/json:
              schema: FileListResponseRPC
      tags:
        - cache
    """
    return ListUploadedFilesCtrl(cache, user_data).to_response()


@cache_blueprint.route("/cache.files_upload", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@requires_identity
def upload_file_view(user_data, cache):
    """
    Upload a file or archive of files.

    ---
    post:
      description: Upload a file or archive of files.
      parameters:
        - in: query
          schema: FileUploadRequest
      requestBody:
        content:
          multipart/form-data:
            schema:
              type: object
              properties:
                file:
                  type: string
                  format: binary
      responses:
        200:
          description: List of uploaded files.
          content:
            application/json:
              schema: FileUploadResponseRPC
      tags:
        - cache
    """
    return UploadFilesCtrl(cache, user_data, request).to_response()


@cache_blueprint.route(
    "/cache.files_delete_chunks", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def delete_file_chunks_view(user_data, cache):
    """
    Delete chunks from a chunked upload.

    ---
    post:
      description: Delete chunks from a chunked upload.
      parameters:
        - in: query
          schema: FileChunksDeleteRequest
      responses:
        200:
          description: Status of deletion request.
          content:
            application/json:
              schema: FileChunksDeleteResponseRPC
      tags:
        - cache
    """
    return DeleteFileChunksCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@cache_blueprint.route("/cache.project_clone", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def project_clone_view(user_data, cache):
    """
    Clone a remote project.

    ---
    post:
      description: Clone a remote project. If the project is cached already,
        a new clone operation will override the old cache state.
      requestBody:
        content:
          application/json:
            schema: RepositoryCloneRequest
      responses:
        200:
          description: Cloned project.
          content:
            application/json:
              schema: ProjectCloneResponseRPC
      tags:
        - cache
    """
    return ProjectCloneCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@cache_blueprint.route("/cache.project_list", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@requires_identity
def list_projects_view(user_data, cache):
    """
    List cached projects.

    ---
    get:
      description: List cached projects.
      responses:
        200:
          description: List of cached projects.
          content:
            application/json:
              schema: ProjectListResponseRPC
      tags:
        - cache
    """
    return ListProjectsCtrl(cache, user_data).to_response()


@cache_blueprint.route("/cache.migrate", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_1)
@handle_common_except
@handle_migration_write_errors
@accepts_json
@requires_cache
@requires_identity
def migrate_project_view(user_data, cache):
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
              schema: ProjectMigrateResponseRPC
      tags:
        - cache
    """
    return MigrateProjectCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@cache_blueprint.route(
    "/cache.migrations_check", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_0
)
@handle_common_except
@handle_migration_read_errors
@requires_cache
@optional_identity
def migration_check_project_view(user_data, cache):
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
              schema: ProjectMigrationCheckResponseRPC
      tags:
        - cache
    """
    return MigrationsCheckCtrl(cache, user_data, dict(request.args), GitlabAPIProvider()).to_response()


cache_blueprint = add_v0_9_specific_endpoints(cache_blueprint)
cache_blueprint = add_v1_0_specific_endpoints(cache_blueprint)
