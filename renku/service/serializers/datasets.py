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
"""Renku service datasets serializers."""
import marshmallow
from marshmallow import Schema, fields, post_load, pre_load

from renku.core.models.datasets import DatasetCreatorsJson as DatasetCreators
from renku.core.models.datasets import DatasetDetailsJson as DatasetDetails
from renku.core.models.datasets import ImageObjectJson as ImageObject
from renku.core.models.datasets import ImageObjectRequestJson as ImageObjectRequest
from renku.service.serializers.common import RenkuSyncSchema
from renku.service.serializers.rpc import JsonRPCResponse


class DatasetDetailsRequest(DatasetDetails):
    """Request schema with dataset image information."""

    images = marshmallow.fields.List(marshmallow.fields.Nested(ImageObjectRequest))


class DatasetCreateRequest(DatasetDetailsRequest):
    """Request schema for a dataset create view."""

    project_id = fields.String(required=True)

    commit_message = fields.String()

    @pre_load()
    def default_commit_message(self, data, **kwargs):
        """Set default commit message."""
        if not data.get("commit_message"):
            data["commit_message"] = "service: dataset create {0}".format(data["name"])

        return data


class DatasetCreateResponse(RenkuSyncSchema):
    """Response schema for a dataset create view."""

    name = fields.String(required=True)


class DatasetCreateResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetCreateResponse)


class DatasetRemoveRequest(DatasetDetails):
    """Request schema for a dataset create view."""

    project_id = fields.String(required=True)
    name = fields.String(required=True)

    commit_message = fields.String()

    @pre_load()
    def default_commit_message(self, data, **kwargs):
        """Set default commit message."""
        if not data.get("commit_message"):
            data["commit_message"] = "service: dataset delete {0}".format(data["name"])

        return data


class DatasetRemoveResponse(RenkuSyncSchema):
    """Response schema for a dataset create view."""

    name = fields.String(required=True)


class DatasetRemoveResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetRemoveResponse)


class DatasetAddFile(Schema):
    """Schema for a dataset add file view."""

    file_id = fields.String()
    file_path = fields.String()
    file_url = fields.String()
    job_id = fields.String()


class DatasetAddRequest(Schema):
    """Request schema for a dataset add file view."""

    name = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetAddFile), required=True)

    create_dataset = fields.Boolean(missing=False)
    force = fields.Boolean(missing=False)

    project_id = fields.String(required=True)
    commit_message = fields.String()
    client_extras = fields.String()

    @post_load()
    def default_commit_message(self, data, **kwargs):
        """Set default commit message."""
        if not data.get("commit_message"):
            data["commit_message"] = "service: dataset add {0}".format(data["name"])

        return data

    @post_load()
    def check_files(self, data, **kwargs):
        """Check serialized file list."""
        for _file in data["files"]:
            if "file_id" in _file and "file_path" in _file:
                raise marshmallow.ValidationError(("invalid reference found:" "use either `file_id` or `file_path`"))

        return data


class DatasetAddResponse(RenkuSyncSchema):
    """Response schema for a dataset add file view."""

    project_id = fields.String(required=True)
    name = fields.String(required=True)

    files = fields.List(fields.Nested(DatasetAddFile), required=True)


class DatasetAddResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset add."""

    result = fields.Nested(DatasetAddResponse)


class DatasetListRequest(Schema):
    """Request schema for dataset list view."""

    project_id = fields.String()

    git_url = fields.String()
    branch = fields.String()


class DatasetDetailsResponse(DatasetDetails):
    """Request schema with dataset image information."""

    images = marshmallow.fields.List(marshmallow.fields.Nested(ImageObject))


class DatasetListResponse(Schema):
    """Response schema for dataset list view."""

    datasets = fields.List(fields.Nested(DatasetDetailsResponse), required=True)


class DatasetListResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(DatasetListResponse)


class DatasetFilesListRequest(Schema):
    """Request schema for dataset files list view."""

    name = fields.String(required=True)

    project_id = fields.String()

    git_url = fields.String()
    branch = fields.String()


class DatasetFileDetails(Schema):
    """Serialize dataset files to a response object."""

    name = fields.String(required=True)
    path = fields.String()
    created = fields.DateTime()
    added = fields.DateTime()


class DatasetFilesListResponse(Schema):
    """Response schema for dataset files list view."""

    name = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetFileDetails), required=True)


class DatasetFilesListResponseRPC(JsonRPCResponse):
    """RPC schema for dataset files list view."""

    result = fields.Nested(DatasetFilesListResponse)


class DatasetImportRequest(Schema):
    """Dataset import request."""

    project_id = fields.String(required=True)
    dataset_uri = fields.String(required=True)
    name = fields.String()
    extract = fields.Boolean()
    client_extras = fields.String()


class DatasetImportResponse(Schema):
    """Dataset import response."""

    job_id = fields.String(required=True)
    created_at = fields.DateTime()


class DatasetImportResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset import."""

    result = fields.Nested(DatasetImportResponse)


class DatasetEditRequest(Schema):
    """Dataset edit metadata request."""

    name = fields.String(required=True)
    title = fields.String(default=None)
    description = fields.String(default=None)
    creators = fields.List(fields.Nested(DatasetCreators))
    keywords = fields.List(fields.String())
    images = fields.List(fields.Nested(ImageObjectRequest))

    project_id = fields.String()
    git_url = fields.String()
    commit_message = fields.String()


class DatasetEditResponse(RenkuSyncSchema):
    """Dataset edit metadata response."""

    edited = fields.Dict(required=True)
    warnings = fields.List(fields.String())


class DatasetEditResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset import."""

    result = fields.Nested(DatasetEditResponse)


class DatasetUnlinkRequest(Schema):
    """Dataset unlink file request."""

    name = fields.String(required=True)
    include_filters = fields.List(fields.String())
    exclude_filters = fields.List(fields.String())

    project_id = fields.String()
    git_url = fields.String()
    commit_message = fields.String()

    @post_load()
    def check_filters(self, data, **kwargs):
        """Check filters."""
        include_filter = data.get("include_filters")
        exclude_filter = data.get("exclude_filters")

        if not include_filter and not exclude_filter:
            raise marshmallow.ValidationError("one of the filters must be specified")

        return data


class DatasetUnlinkResponse(RenkuSyncSchema):
    """Dataset unlink files response."""

    unlinked = fields.List(fields.String())


class DatasetUnlinkResponseRPC(JsonRPCResponse):
    """Dataset unlink files RPC response."""

    result = fields.Nested(DatasetUnlinkResponse)
