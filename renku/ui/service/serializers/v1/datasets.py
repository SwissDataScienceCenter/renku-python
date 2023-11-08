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
"""
Renku service datasets serializers for API before 2.2.

In versions before 2.2, ``renku:slug`` was referred to as ``name`` and
``schema:name`` was ``title``. From version 2.2 onward, these are called
``slug`` and ``name`` respectively.
"""

from marshmallow import Schema, ValidationError, fields, post_load

from renku.domain_model.dataset import AnnotationJson
from renku.domain_model.dataset import DatasetCreatorsJson
from renku.domain_model.dataset import DatasetCreatorsJson as DatasetCreators
from renku.domain_model.dataset import ImageObjectJson as ImageObject
from renku.domain_model.dataset import ImageObjectRequestJson as ImageObjectRequest
from renku.ui.service.serializers.common import AsyncSchema, MigrateSchema, RemoteRepositorySchema, RenkuSyncSchema
from renku.ui.service.serializers.datasets import DatasetAddFile, DatasetFileDetails
from renku.ui.service.serializers.rpc import JsonRPCResponse


class DatasetNameSchema_2_1(Schema):
    """Schema for dataset name."""

    name = fields.String(metadata={"description": "Mandatory dataset name."}, required=True, attribute="slug")


class DatasetDetails_2_1(DatasetNameSchema_2_1):
    """Schema for dataset details."""

    version = fields.String(allow_none=True)
    created_at = fields.String(allow_none=True, attribute="date_created")

    title = fields.String(attribute="name")
    creators = fields.List(fields.Nested(DatasetCreatorsJson))
    description = fields.String()
    keywords = fields.List(fields.String())
    identifier = fields.String()
    storage = fields.String()

    annotations = fields.List(fields.Nested(AnnotationJson))

    data_directory = fields.Method("get_datadir")

    @staticmethod
    def get_datadir(obj):
        """Get data directory."""
        if isinstance(obj, dict):
            return str(obj.get("datadir_path", obj.get("datadir", "")))
        if hasattr(obj, "datadir_path"):
            return obj.datadir_path

        return str(obj.get_datadir())


class DatasetDetailsRequest_2_1(DatasetDetails_2_1):
    """Request schema with dataset image information."""

    images = fields.List(fields.Nested(ImageObjectRequest))

    custom_metadata: fields.Field = fields.Dict()


class DatasetCreateRequest_2_1(AsyncSchema, DatasetDetailsRequest_2_1, RemoteRepositorySchema, MigrateSchema):
    """Request schema for a dataset create view."""

    # NOTE: Override field in DatasetDetails
    data_directory = fields.String(  # type: ignore
        load_default=None,
        metadata={"description": "Base dataset data directory. '<project.data_directory>/<dataset.name>' by default"},
    )


class DatasetCreateResponse_2_1(DatasetNameSchema_2_1, RenkuSyncSchema):
    """Response schema for a dataset create view."""


class DatasetCreateResponseRPC_2_1(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetCreateResponse_2_1)


class DatasetRemoveRequest_2_1(AsyncSchema, DatasetNameSchema_2_1, RemoteRepositorySchema, MigrateSchema):
    """Request schema for a dataset remove."""


class DatasetRemoveResponse_2_1(DatasetNameSchema_2_1, RenkuSyncSchema):
    """Response schema for a dataset create view."""


class DatasetRemoveResponseRPC_2_1(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetRemoveResponse_2_1)


class DatasetAddRequest_2_1(AsyncSchema, DatasetNameSchema_2_1, RemoteRepositorySchema, MigrateSchema):
    """Request schema for a dataset add file view."""

    files = fields.List(fields.Nested(DatasetAddFile), required=True)

    create_dataset = fields.Boolean(load_default=False)
    force = fields.Boolean(load_default=False)

    client_extras = fields.String()

    @post_load()
    def check_files(self, data, **_):
        """Check serialized file list."""
        for _file in data["files"]:
            if "file_id" in _file and "file_path" in _file:
                raise ValidationError("invalid reference found: use either `file_id` or `file_path`")

        return data


class DatasetAddResponse_2_1(DatasetNameSchema_2_1, RenkuSyncSchema):
    """Response schema for a dataset add file view."""

    project_id = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetAddFile), required=True)


class DatasetAddResponseRPC_2_1(JsonRPCResponse):
    """RPC schema for a dataset add."""

    result = fields.Nested(DatasetAddResponse_2_1)


class DatasetDetailsResponse_2_1(DatasetDetails_2_1):
    """Request schema with dataset image information."""

    images = fields.List(fields.Nested(ImageObject))


class DatasetListResponse_2_1(Schema):
    """Response schema for dataset list view."""

    datasets = fields.List(fields.Nested(DatasetDetailsResponse_2_1), required=True)


class DatasetListResponseRPC_2_1(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(DatasetListResponse_2_1)


class DatasetFilesListRequest_2_1(DatasetNameSchema_2_1, RemoteRepositorySchema):
    """Request schema for dataset files list view."""


class DatasetFilesListResponse_2_1(DatasetNameSchema_2_1):
    """Response schema for dataset files list view."""

    files = fields.List(fields.Nested(DatasetFileDetails), required=True)


class DatasetFilesListResponseRPC_2_1(JsonRPCResponse):
    """RPC schema for dataset files list view."""

    result = fields.Nested(DatasetFilesListResponse_2_1)


class DatasetImportRequest_2_1(AsyncSchema, RemoteRepositorySchema, MigrateSchema):
    """Dataset import request."""

    dataset_uri = fields.String(required=True)
    name = fields.String(metadata={"description": "Optional dataset name."}, attribute="slug")
    extract = fields.Boolean()
    tag = fields.String(metadata={"description": "Dataset version to import."})
    data_directory = fields.String(
        load_default=None,
        metadata={"description": "Base dataset data directory. '<project.data_directory>/<dataset.name>' by default"},
    )


class DatasetEditRequest_2_1(
    AsyncSchema,
    DatasetDetailsRequest_2_1,
    DatasetNameSchema_2_1,
    RemoteRepositorySchema,
    MigrateSchema,
):
    """Dataset edit metadata request."""

    title = fields.String(metadata={"description": "New name of the dataset"}, attribute="name")
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


class DatasetUnlinkRequest_2_1(AsyncSchema, DatasetNameSchema_2_1, RemoteRepositorySchema, MigrateSchema):
    """Dataset unlink file request."""

    include_filters = fields.List(fields.String())
    exclude_filters = fields.List(fields.String())

    @post_load()
    def check_filters(self, data, **_):
        """Check filters."""
        include_filter = data.get("include_filters")
        exclude_filter = data.get("exclude_filters")

        if not include_filter and not exclude_filter:
            raise ValidationError("one of the filters must be specified")

        return data
