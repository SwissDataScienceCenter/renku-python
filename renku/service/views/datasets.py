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
from flask import Blueprint, request
from flask_apispec import marshal_with, use_kwargs

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.datasets_add_file import DatasetsAddFileCtrl
from renku.service.controllers.datasets_create import DatasetsCreateCtrl
from renku.service.controllers.datasets_edit import DatasetsEditCtrl
from renku.service.controllers.datasets_files_list import DatasetsFilesListCtrl
from renku.service.controllers.datasets_import import DatasetsImportCtrl
from renku.service.controllers.datasets_list import DatasetsListCtrl
from renku.service.controllers.datasets_remove import DatasetsRemoveCtrl
from renku.service.controllers.datasets_unlink import DatasetsUnlinkCtrl
from renku.service.serializers.datasets import (
    DatasetAddRequest,
    DatasetAddResponseRPC,
    DatasetCreateRequest,
    DatasetCreateResponseRPC,
    DatasetEditRequest,
    DatasetEditResponseRPC,
    DatasetFilesListRequest,
    DatasetFilesListResponseRPC,
    DatasetImportRequest,
    DatasetImportResponseRPC,
    DatasetListRequest,
    DatasetListResponseRPC,
    DatasetRemoveRequest,
    DatasetRemoveResponseRPC,
    DatasetUnlinkRequest,
    DatasetUnlinkResponseRPC,
)
from renku.service.views.decorators import (
    accepts_json,
    handle_common_except,
    header_doc,
    optional_identity,
    requires_cache,
    requires_identity,
)

DATASET_BLUEPRINT_TAG = "datasets"
dataset_blueprint = Blueprint(DATASET_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@use_kwargs(DatasetListRequest, location="query")
@marshal_with(DatasetListResponseRPC)
@header_doc("List all datasets in project.", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.list", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@optional_identity
def list_datasets_view(user_data, cache):
    """List all datasets in project."""
    return DatasetsListCtrl(cache, user_data, dict(request.args)).to_response()


@use_kwargs(DatasetFilesListRequest, location="query")
@marshal_with(DatasetFilesListResponseRPC)
@header_doc("List files in a dataset.", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.files_list", methods=["GET"], provide_automatic_options=False,
)
@handle_common_except
@requires_cache
@optional_identity
def list_dataset_files_view(user_data, cache):
    """List files in a dataset."""
    return DatasetsFilesListCtrl(cache, user_data, dict(request.args)).to_response()


@use_kwargs(DatasetAddRequest)
@marshal_with(DatasetAddResponseRPC)
@header_doc("Add an uploaded file to a cloned repository.", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.add", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def add_file_to_dataset_view(user_data, cache):
    """Add the uploaded file to cloned repository."""
    return DatasetsAddFileCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(DatasetCreateRequest)
@marshal_with(DatasetCreateResponseRPC)
@header_doc("Create a new dataset in a project.", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.create", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def create_dataset_view(user_data, cache):
    """Create a new dataset in a project."""
    return DatasetsCreateCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(DatasetRemoveRequest)
@marshal_with(DatasetRemoveResponseRPC)
@header_doc("Remove a dataset from a project.", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.remove", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def remove_dataset_view(user_data, cache):
    """Remove a dataset from a project."""
    return DatasetsRemoveCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(DatasetImportRequest)
@marshal_with(DatasetImportResponseRPC)
@header_doc("Import a dataset", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.import", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def import_dataset_view(user_data, cache):
    """Import a dataset view."""
    return DatasetsImportCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(DatasetEditRequest)
@marshal_with(DatasetEditResponseRPC)
@header_doc("Edit dataset metadata", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.edit", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def edit_dataset_view(user_data, cache):
    """Edit dataset metadata."""
    return DatasetsEditCtrl(cache, user_data, dict(request.json)).to_response()


@use_kwargs(DatasetUnlinkRequest)
@marshal_with(DatasetUnlinkResponseRPC)
@header_doc("Unlink a file from a dataset", tags=(DATASET_BLUEPRINT_TAG,))
@dataset_blueprint.route(
    "/datasets.unlink", methods=["POST"], provide_automatic_options=False,
)
@handle_common_except
@accepts_json
@requires_cache
@requires_identity
def unlink_file_view(user_data, cache):
    """Unlink a file from a dataset."""
    return DatasetsUnlinkCtrl(cache, user_data, dict(request.json)).to_response()
