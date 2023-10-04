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
"""Renku service datasets view."""
from flask import request

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.v1.datasets_add_file import DatasetsAddFileCtrl_2_1
from renku.ui.service.controllers.v1.datasets_create import DatasetsCreateCtrl_2_1
from renku.ui.service.controllers.v1.datasets_edit import DatasetsEditCtrl_2_1
from renku.ui.service.controllers.v1.datasets_files_list import DatasetsFilesListCtrl_2_1
from renku.ui.service.controllers.v1.datasets_import import DatasetsImportCtrl_2_1
from renku.ui.service.controllers.v1.datasets_list import DatasetsListCtrl_2_1
from renku.ui.service.controllers.v1.datasets_remove import DatasetsRemoveCtrl_2_1
from renku.ui.service.controllers.v1.datasets_unlink import DatasetsUnlinkCtrl_2_1
from renku.ui.service.views.api_versions import VERSIONS_BEFORE_2_2, VersionedBlueprint
from renku.ui.service.views.decorators import accepts_json, optional_identity, requires_cache, requires_identity
from renku.ui.service.views.error_handlers import (
    handle_common_except,
    handle_datasets_unlink_errors,
    handle_datasets_write_errors,
)

DATASET_BLUEPRINT_TAG = "datasets"
dataset_blueprint = VersionedBlueprint(DATASET_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@handle_common_except
@requires_cache
@optional_identity
def list_datasets_view_2_1(user_data, cache):
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
              schema: DatasetListResponseRPC_2_1
      tags:
        - datasets
    """
    return DatasetsListCtrl_2_1(cache, user_data, dict(request.args)).to_response()


@handle_common_except
@requires_cache
@optional_identity
def list_dataset_files_view_2_1(user_data, cache):
    """
    List files in a dataset.

    ---
    get:
      description: List files in a dataset.
      parameters:
        - in: query
          schema: DatasetFilesListRequest_2_1
      responses:
        200:
          description: Listing of all files in a dataset.
          content:
            application/json:
              schema: DatasetFilesListResponseRPC_2_1
      tags:
        - datasets
    """
    return DatasetsFilesListCtrl_2_1(cache, user_data, dict(request.args)).to_response()


@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def add_file_to_dataset_view_2_1(user_data, cache):
    """
    Add the uploaded file to a cloned repository.

    ---
    post:
      description: Add the uploaded file to a cloned repository.
      requestBody:
        content:
          application/json:
            schema: DatasetAddRequest_2_1
      responses:
        200:
          description: Details of the added files.
          content:
            application/json:
              schema: DatasetAddResponseRPC_2_1
      tags:
        - datasets
    """
    return DatasetsAddFileCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def create_dataset_view_2_1(user_data, cache):
    """
    Create a new dataset in a project.

    ---
    post:
      description: Create a new dataset in a project.
      requestBody:
        content:
          application/json:
            schema: DatasetCreateRequest_2_1
      responses:
        200:
          description: Properties of the created dataset.
          content:
            application/json:
              schema: DatasetCreateResponseRPC_2_1
      tags:
        - datasets
    """
    return DatasetsCreateCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def remove_dataset_view_2_1(user_data, cache):
    """
    Remove a dataset from a project.

    ---
    post:
      description: Remove a dataset from a project.
      requestBody:
        content:
          application/json:
            schema: DatasetRemoveRequest_2_1
      responses:
        200:
          description: Details of the removed dataset.
          content:
            application/json:
              schema: DatasetRemoveResponseRPC_2_1
      tags:
        - datasets
    """
    return DatasetsRemoveCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def import_dataset_view_2_1(user_data, cache):
    """
    Import a dataset view.

    ---
    post:
      description: Import a dataset into a project.
      requestBody:
        content:
          application/json:
            schema: DatasetImportRequest_2_1
      responses:
        200:
          description: Details of the dispatched import dataset job.
          content:
            application/json:
              schema: DatasetImportResponseRPC
      tags:
        - datasets
    """
    return DatasetsImportCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


@handle_common_except
@handle_datasets_write_errors
@accepts_json
@requires_cache
@requires_identity
def edit_dataset_view_2_1(user_data, cache):
    """
    Edit dataset metadata view.

    Not passing a field leaves it unchanged.

    ---
    post:
      description: Edit dataset metadata.
      requestBody:
        content:
          application/json:
            schema: DatasetEditRequest_2_1
      responses:
        200:
          description: Status of the requested dataset edits.
          content:
            application/json:
              schema: DatasetEditResponseRPC
      tags:
        - datasets
    """
    return DatasetsEditCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


@handle_common_except
@handle_datasets_unlink_errors
@accepts_json
@requires_cache
@requires_identity
def unlink_file_view_2_1(user_data, cache):
    """
    Unlink a file from a dataset view.

    ---
    post:
      description: Unlink a file from a dataset.
      requestBody:
        content:
          application/json:
            schema: DatasetUnlinkRequest_2_1
      responses:
        200:
          description: Details of the unlinked files.
          content:
            application/json:
              schema: DatasetUnlinkResponseRPC
      tags:
        - datasets
    """
    return DatasetsUnlinkCtrl_2_1(cache, user_data, dict(request.json)).to_response()  # type: ignore


def add_v1_specific_dataset_endpoints(dataset_blueprint):
    """Add v1 only endpoints to blueprint."""
    dataset_blueprint.route(
        "/datasets.list", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(list_datasets_view_2_1)
    dataset_blueprint.route(
        "/datasets.files_list", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(list_dataset_files_view_2_1)
    dataset_blueprint.route(
        "/datasets.add", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(add_file_to_dataset_view_2_1)
    dataset_blueprint.route(
        "/datasets.create", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(create_dataset_view_2_1)
    dataset_blueprint.route(
        "/datasets.remove", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(remove_dataset_view_2_1)
    dataset_blueprint.route(
        "/datasets.import", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(import_dataset_view_2_1)
    dataset_blueprint.route(
        "/datasets.edit", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(edit_dataset_view_2_1)
    dataset_blueprint.route(
        "/datasets.unlink", methods=["POST"], provide_automatic_options=False, versions=VERSIONS_BEFORE_2_2
    )(unlink_file_view_2_1)

    return dataset_blueprint
