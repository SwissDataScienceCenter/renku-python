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

from renku.service.serializers.rpc import JsonRPCResponse


class DatasetCreators(Schema):
    """Schema for the dataset creators."""

    name = fields.String(required=True)
    email = fields.String()
    affiliation = fields.String()


class DatasetCreateRequest(Schema):
    """Request schema for dataset create view."""

    creators = fields.List(fields.Nested(DatasetCreators))
    commit_message = fields.String()
    dataset_name = fields.String(required=True)
    description = fields.String()
    project_id = fields.String(required=True)

    @pre_load()
    def default_commit_message(self, data, **kwargs):
        """Set default commit message."""
        if not data.get('commit_message'):
            data['commit_message'] = 'service: dataset create {0}'.format(
                data['dataset_name']
            )

        return data


class DatasetCreateResponse(Schema):
    """Response schema for dataset create view."""

    dataset_name = fields.String(required=True)


class DatasetCreateResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset create view."""

    result = fields.Nested(DatasetCreateResponse)


class DatasetAddFile(Schema):
    """Schema for dataset add file view."""

    file_id = fields.String()
    file_path = fields.String()


class DatasetAddRequest(Schema):
    """Request schema for dataset add file view."""

    commit_message = fields.String()
    dataset_name = fields.String(required=True)
    create_dataset = fields.Boolean(missing=False)
    project_id = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetAddFile), required=True)

    @post_load()
    def default_commit_message(self, data, **kwargs):
        """Set default commit message."""
        if not data.get('commit_message'):
            data['commit_message'] = 'service: dataset add {0}'.format(
                data['dataset_name']
            )

        return data

    @post_load()
    def check_files(self, data, **kwargs):
        """Check serialized file list."""
        for _file in data['files']:
            if 'file_id' in _file and 'file_path' in _file:
                raise marshmallow.ValidationError((
                    'invalid reference found:'
                    'use either `file_id` or `file_path`'
                ))

        return data


class DatasetAddResponse(Schema):
    """Response schema for dataset add file view."""

    dataset_name = fields.String(required=True)
    project_id = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetAddFile), required=True)


class DatasetAddResponseRPC(JsonRPCResponse):
    """RPC schema for dataset add."""

    result = fields.Nested(DatasetAddResponse)


class DatasetListRequest(Schema):
    """Request schema for dataset list view."""

    project_id = fields.String(required=True)


class DatasetDetails(Schema):
    """Serialize dataset to response object."""

    identifier = fields.String(required=True)
    name = fields.String(required=True)
    version = fields.String(allow_none=True)
    created = fields.String(allow_none=True)


class DatasetListResponse(Schema):
    """Response schema for dataset list view."""

    datasets = fields.List(fields.Nested(DatasetDetails), required=True)


class DatasetListResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(DatasetListResponse)


class DatasetFilesListRequest(Schema):
    """Request schema for dataset files list view."""

    project_id = fields.String(required=True)
    dataset_name = fields.String(required=True)


class DatasetFileDetails(Schema):
    """Serialzie dataset files to response object."""

    name = fields.String(required=True)


class DatasetFilesListResponse(Schema):
    """Response schema for dataset files list view."""

    dataset_name = fields.String(required=True)
    files = fields.List(fields.Nested(DatasetFileDetails), required=True)


class DatasetFilesListResponseRPC(JsonRPCResponse):
    """RPC schema for dataset files list view."""

    result = fields.Nested(DatasetFilesListResponse)
