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
from marshmallow import Schema, ValidationError, fields, post_load

from renku.domain_model.dataset import DatasetCreatorsJson as DatasetCreators
from renku.domain_model.dataset import DatasetDetailsJson as DatasetDetails
from renku.domain_model.dataset import ImageObjectJson as ImageObject
from renku.domain_model.dataset import ImageObjectRequestJson as ImageObjectRequest
from renku.ui.service.serializers.common import (
    AsyncSchema,
    JobDetailsResponse,
    LocalRepositorySchema,
    MigrateSchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.ui.service.serializers.rpc import JsonRPCResponse


class DatasetRefSchema(Schema):
    """Schema for specifying a reference."""

    ref = fields.String(metadata={"description": "Target reference."})


class DatasetNameSchema(Schema):
    """Schema for dataset name."""

    name = fields.String(metadata={"description": "Mandatory dataset name."}, required=True)


class DatasetDetailsRequest(DatasetDetails):
    """Request schema with dataset image information."""

    images = fields.List(fields.Nested(ImageObjectRequest))

    custom_metadata: fields.Field = fields.Dict()


class DatasetCreateRequest(
    AsyncSchema, DatasetDetailsRequest, DatasetRefSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema
):
    """Request schema for a dataset create view."""

    # NOTE: Override field in DatasetDetails
    data_directory = fields.String(  # type: ignore
        load_default=None,
        metadata={"description": "Base dataset data directory. '<project.data_directory>/<dataset.name>' by default"},
    )


class DatasetCreateResponse(DatasetNameSchema, RenkuSyncSchema):
    """Response schema for a dataset create view."""


class DatasetCreateResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetCreateResponse)


class DatasetRemoveRequest(
    AsyncSchema, DatasetNameSchema, DatasetRefSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema
):
    """Request schema for a dataset remove."""


class DatasetRemoveResponse(DatasetNameSchema, RenkuSyncSchema):
    """Response schema for a dataset create view."""


class DatasetRemoveResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetRemoveResponse)


class DatasetAddFile(Schema):
    """Schema for a dataset add file view."""

    file_id = fields.String()
    file_path = fields.String()
    file_url = fields.String()
    job_id = fields.String()


class DatasetAddRequest(
    AsyncSchema, DatasetNameSchema, DatasetRefSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema
):
    """Request schema for a dataset add file view."""

    files = fields.List(fields.Nested(DatasetAddFile), required=True)

    create_dataset = fields.Boolean(load_default=False)
    force = fields.Boolean(load_default=False)

    client_extras = fields.String()

    @post_load()
    def check_files(self, data, **kwargs):
        """Check serialized file list."""
        for _file in data["files"]:
            if "file_id" in _file and "file_path" in _file:
                raise ValidationError(("invalid reference found:" "use either `file_id` or `file_path`"))

        return data


class DatasetAddResponse(DatasetNameSchema, RenkuSyncSchema):
    """Response schema for a dataset add file view."""

    project_id = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetAddFile), required=True)


class DatasetAddResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset add."""

    result = fields.Nested(DatasetAddResponse)


class DatasetListRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for dataset list view."""


class DatasetDetailsResponse(DatasetDetails):
    """Request schema with dataset image information."""

    images = fields.List(fields.Nested(ImageObject))


class DatasetListResponse(Schema):
    """Response schema for dataset list view."""

    datasets = fields.List(fields.Nested(DatasetDetailsResponse), required=True)


class DatasetListResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(DatasetListResponse)


class DatasetFilesListRequest(DatasetNameSchema, LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for dataset files list view."""


class DatasetFileDetails(DatasetNameSchema):
    """Serialize dataset files to a response object."""

    path = fields.String()
    created = fields.DateTime()
    added = fields.DateTime()


class DatasetFilesListResponse(DatasetNameSchema):
    """Response schema for dataset files list view."""

    files = fields.List(fields.Nested(DatasetFileDetails), required=True)


class DatasetFilesListResponseRPC(JsonRPCResponse):
    """RPC schema for dataset files list view."""

    result = fields.Nested(DatasetFilesListResponse)


class DatasetImportRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Dataset import request."""

    dataset_uri = fields.String(required=True)
    name = fields.String(metadata={"description": "Optional dataset name."})
    extract = fields.Boolean()
    tag = fields.String(metadata={"description": "Dataset version to import."})
    data_directory = fields.String(
        load_default=None,
        metadata={"description": "Base dataset data directory. '<project.data_directory>/<dataset.name>' by default"},
    )


class DatasetImportResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset import."""

    result = fields.Nested(JobDetailsResponse)


class DatasetEditRequest(
    AsyncSchema,
    DatasetDetailsRequest,
    DatasetNameSchema,
    DatasetRefSchema,
    LocalRepositorySchema,
    RemoteRepositorySchema,
    MigrateSchema,
):
    """Dataset edit metadata request."""

    title = fields.String(metadata={"description": "New title of the dataset"})
    description = fields.String(metadata={"description": "New description of the dataset"})
    creators = fields.List(fields.Nested(DatasetCreators), metadata={"description": "New creators of the dataset"})
    keywords = fields.List(fields.String(), allow_none=True, metadata={"description": "New keywords for the dataset"})
    images = fields.List(
        fields.Nested(ImageObjectRequest), allow_none=True, metadata={"description": "New dataset images"}
    )
    custom_metadata = fields.List(
        fields.Dict(), metadata={"description": "New custom metadata for the dataset"}, allow_none=True
    )
    custom_metadata_source = fields.String(
        allow_none=True,
        metadata={"description": "Source for the custom metadata for the dataset"},
    )


class DatasetEditResponse(RenkuSyncSchema):
    """Dataset edit metadata response."""

    edited = fields.Dict(required=True)
    warnings = fields.List(fields.String())


class DatasetEditResponseRPC(JsonRPCResponse):
    """RPC schema for a dataset import."""

    result = fields.Nested(DatasetEditResponse)


class DatasetUnlinkRequest(
    AsyncSchema, DatasetNameSchema, DatasetRefSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema
):
    """Dataset unlink file request."""

    include_filters = fields.List(fields.String())
    exclude_filters = fields.List(fields.String())

    @post_load()
    def check_filters(self, data, **kwargs):
        """Check filters."""
        include_filter = data.get("include_filters")
        exclude_filter = data.get("exclude_filters")

        if not include_filter and not exclude_filter:
            raise ValidationError("one of the filters must be specified")

        return data


class DatasetUnlinkResponse(RenkuSyncSchema):
    """Dataset unlink files response."""

    unlinked = fields.List(fields.String())


class DatasetUnlinkResponseRPC(JsonRPCResponse):
    """Dataset unlink files RPC response."""

    result = fields.Nested(DatasetUnlinkResponse)
