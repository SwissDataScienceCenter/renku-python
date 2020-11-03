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
from flask import Blueprint, request
from flask_apispec import marshal_with, use_kwargs

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.cache_files_upload import UploadFilesCtrl
from renku.service.controllers.cache_list_projects import ListProjectsCtrl
from renku.service.controllers.cache_list_uploaded import ListUploadedFilesCtrl
from renku.service.controllers.cache_migrate_project import MigrateProjectCtrl
from renku.service.controllers.cache_migrations_check import MigrationsCheckCtrl
from renku.service.controllers.cache_project_clone import ProjectCloneCtrl
from renku.service.serializers.cache import (
    FileListResponseRPC,
    FileUploadRequest,
    FileUploadResponseRPC,
    ProjectCloneRequest,
    ProjectCloneResponseRPC,
    ProjectListResponseRPC,
    ProjectMigrateRequest,
    ProjectMigrateResponseRPC,
    ProjectMigrationCheckRequest,
    ProjectMigrationCheckResponseRPC,
)
from renku.service.views.decorators import (
    accepts_json,
    handle_common_except,
    handle_migration_except,
    header_doc,
    requires_cache,
    requires_identity,
)

CACHE_BLUEPRINT_TAG = "cache"
cache_blueprint = Blueprint("cache", __name__, url_prefix=SERVICE_PREFIX)


@marshal_with(FileListResponseRPC)
@header_doc(description="List uploaded files.", tags=(CACHE_BLUEPRINT_TAG,))
@cache_blueprint.route(
    "/cache.files_list", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
def list_uploaded_files_view(user_data, cache):
    """List uploaded files ready to be added to projects."""
    return ListUploadedFilesCtrl(cache, user_data).to_response()


@use_kwargs(FileUploadRequest)
@marshal_with(FileUploadResponseRPC)
@header_doc(
    description="Upload file or archive of files.", tags=(CACHE_BLUEPRINT_TAG,),
)
@cache_blueprint.route(
    "/cache.files_upload", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
def upload_file_view(user_data, cache):
    """Upload file or archive of files."""
    return UploadFilesCtrl(cache, user_data, request).to_response()


@use_kwargs(ProjectCloneRequest)
@marshal_with(ProjectCloneResponseRPC)
@header_doc(
    "Clone a remote project. If the project is cached already, "
    "new clone operation will override the old cache state.",
    tags=(CACHE_BLUEPRINT_TAG,),
)
@cache_blueprint.route(
    "/cache.project_clone", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def project_clone_view(user_data, cache):
    """Clone a remote repository."""
    return ProjectCloneCtrl(cache, user_data, dict(request.json)).to_response()


@marshal_with(ProjectListResponseRPC)
@header_doc(
    "List cached projects.", tags=(CACHE_BLUEPRINT_TAG,),
)
@cache_blueprint.route(
    "/cache.project_list", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
def list_projects_view(user_data, cache):
    """List cached projects."""
    return ListProjectsCtrl(cache, user_data).to_response()


@use_kwargs(ProjectMigrateRequest)
@marshal_with(ProjectMigrateResponseRPC)
@header_doc(
    "Migrate project to the latest version.", tags=(CACHE_BLUEPRINT_TAG,),
)
@cache_blueprint.route(
    "/cache.migrate", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@handle_migration_except
@accepts_json
@requires_cache
@requires_identity
def migrate_project_view(user_data, cache):
    """Migrate specified project."""
    return MigrateProjectCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(ProjectMigrationCheckRequest)
@marshal_with(ProjectMigrationCheckResponseRPC)
@header_doc(
    "Check if project requires migration.", tags=(CACHE_BLUEPRINT_TAG,),
)
@cache_blueprint.route(
    "/cache.migrations_check", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@requires_identity
@accepts_json
def migration_check_project_view(user_data, cache):
    """Migrate specified project."""
    return MigrationsCheckCtrl(cache, user_data, dict(request.args)).to_response()
