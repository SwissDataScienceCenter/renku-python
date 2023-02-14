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
"""Renku service datasets view."""
from flask import request

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.datasets_add_file import DatasetsAddFileCtrl
from renku.ui.service.controllers.datasets_create import DatasetsCreateCtrl
from renku.ui.service.controllers.datasets_edit import DatasetsEditCtrl
from renku.ui.service.controllers.datasets_files_list import DatasetsFilesListCtrl
from renku.ui.service.controllers.datasets_import import DatasetsImportCtrl
from renku.ui.service.controllers.datasets_list import DatasetsListCtrl
from renku.ui.service.controllers.datasets_remove import DatasetsRemoveCtrl
from renku.ui.service.controllers.datasets_unlink import DatasetsUnlinkCtrl
from renku.ui.service.views.api_versions import ALL_VERSIONS, VersionedBlueprint
from renku.ui.service.views.decorators import accepts_json, optional_identity, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import (
    handle_common_except,
    handle_datasets_unlink_errors,
    handle_datasets_write_errors,
)

DATASET_BLUEPRINT_TAG = "datasets"
dataset_blueprint = VersionedBlueprint(DATASET_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@dataset_blueprint.route("/datasets.list", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@requires_cache
@optional_identity
def list_datasets_view(user_data, cache):
    """
    List all datasets in a project.

    ---
    get:
      description: List all datasets in a project.
      parameters:
        - in: query
          schema: DatasetListRequest
      responses:
        200:
          description: Listing of all datasets in a project.
          content:
            application/json:
              schema: DatasetListResponseRPC
      tags:
        - datasets
    """
    return DatasetsListCtrl(cache, user_data, dict(request.args)).to_response()


@dataset_blueprint.route(
    "/datasets.files_list", methods=["GET"], provide_automatic_options=False, versions=ALL_VERSIONS
)
@handle_common_except
@requires_cache
@optional_identity
def list_dataset_files_view(user_data, cache):
    """
    List files in a dataset.

    ---
    get:
      description: List files in a dataset.
      parameters:
        - in: query
          schema: DatasetFilesListRequest
      responses:
        200:
          description: Listing of all files in a dataset.
          content:
            application/json:
              schema: DatasetFilesListResponseRPC
      tags:
        - datasets
    """
    return DatasetsFilesListCtrl(cache, user_data, dict(request.args)).to_response()


@dataset_blueprint.route("/datasets.add", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def add_file_to_dataset_view(user_data, cache):
    """
    Add the uploaded file to a cloned repository.

    ---
    post:
      description: Add the uploaded file to a cloned repository.
      requestBody:
        content:
          application/json:
            schema: DatasetAddRequest
      responses:
        200:
          description: Details of the added files.
          content:
            application/json:
              schema: DatasetAddResponseRPC
      tags:
        - datasets
    """
    return DatasetsAddFileCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@dataset_blueprint.route("/datasets.create", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def create_dataset_view(user_data, cache):
    """
    Create a new dataset in a project.

    ---
    post:
      description: Create a new dataset in a project.
      requestBody:
        content:
          application/json:
            schema: DatasetCreateRequest
      responses:
        200:
          description: Properties of the created dataset.
          content:
            application/json:
              schema: DatasetCreateResponseRPC
      tags:
        - datasets
    """
    return DatasetsCreateCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@dataset_blueprint.route("/datasets.remove", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def remove_dataset_view(user_data, cache):
    """
    Remove a dataset from a project.

    ---
    post:
      description: Remove a dataset from a project.
      requestBody:
        content:
          application/json:
            schema: DatasetRemoveRequest
      responses:
        200:
          description: Details of the removed dataset.
          content:
            application/json:
              schema: DatasetRemoveResponseRPC
      tags:
        - datasets
    """
    return DatasetsRemoveCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@dataset_blueprint.route("/datasets.import", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def import_dataset_view(user_data, cache):
    """
    Import a dataset view.

    ---
    post:
      description: Import a dataset into a project.
      requestBody:
        content:
          application/json:
            schema: DatasetImportRequest
      responses:
        200:
          description: Details of the dispatched import dataset job.
          content:
            application/json:
              schema: DatasetImportResponseRPC
      tags:
        - datasets
    """
    return DatasetsImportCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@dataset_blueprint.route("/datasets.edit", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def edit_dataset_view(user_data, cache):
    """
    Edit dataset metadata view.

    Not passing a field leaves it unchanged.

    ---
    post:
      description: Edit dataset metadata.
      requestBody:
        content:
          application/json:
            schema: DatasetEditRequest
      responses:
        200:
          description: Status of the requested dataset edits.
          content:
            application/json:
              schema: DatasetEditResponseRPC
      tags:
        - datasets
    """
    return DatasetsEditCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore


@dataset_blueprint.route("/datasets.unlink", methods=["POST"], provide_automatic_options=False, versions=ALL_VERSIONS)
@handle_common_except
@handle_datasets_unlink_errors
@accepts_json
@requires_cache
@requires_identity
def unlink_file_view(user_data, cache):
    """
    Unlink a file from a dataset view.

    ---
    post:
      description: Unlink a file from a dataset.
      requestBody:
        content:
          application/json:
            schema: DatasetUnlinkRequest
      responses:
        200:
          description: Details of the unlinked files.
          content:
            application/json:
              schema: DatasetUnlinkResponseRPC
      tags:
        - datasets
    """
    return DatasetsUnlinkCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore
