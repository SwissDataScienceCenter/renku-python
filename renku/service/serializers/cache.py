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
"""Renku service cache serializers."""
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse

from marshmallow import Schema, ValidationError, fields, post_load, pre_load, \
    validates
from werkzeug.utils import secure_filename

from renku.core.errors import ConfigurationError
from renku.core.models.git import GitURL
from renku.service.config import PROJECT_CLONE_DEPTH_DEFAULT
from renku.service.serializers.rpc import JsonRPCResponse


def extract_file(request):
    """Extract file from Flask request.

    :raises: `ValidationError`
    """
    files = request.files
    if 'file' not in files:
        raise ValidationError('missing key: file')

    file = files['file']
    if file and not file.filename:
        raise ValidationError('wrong filename: {0}'.format(file.filename))

    if file:
        file.filename = secure_filename(file.filename)
        return file


class FileUploadRequest(Schema):
    """Request schema for file upload."""

    override_existing = fields.Boolean(missing=False)
    unpack_archive = fields.Boolean(missing=False)


class FileUploadContext(Schema):
    """Context schema for file upload."""

    created_at = fields.DateTime(missing=datetime.utcnow)
    file_id = fields.String(missing=lambda: uuid.uuid4().hex)

    content_type = fields.String(missing='unknown')
    file_name = fields.String(required=True)

    # measured in bytes (comes from stat() - st_size)
    file_size = fields.Integer(required=True)

    relative_path = fields.String(required=True)
    is_archive = fields.Boolean(missing=False)
    is_dir = fields.Boolean(required=True)
    unpack_archive = fields.Boolean(missing=False)


class FileUploadResponse(Schema):
    """Response schema for file upload."""

    files = fields.List(fields.Nested(FileUploadContext), required=True)


class FileUploadResponseRPC(JsonRPCResponse):
    """RPC response schema for file upload response."""

    result = fields.Nested(FileUploadResponse)


class FileListResponse(Schema):
    """Response schema for files listing."""

    files = fields.List(fields.Nested(FileUploadContext), required=True)


class FileListResponseRPC(JsonRPCResponse):
    """RPC response schema for files listing."""

    result = fields.Nested(FileListResponse)


class ProjectCloneRequest(Schema):
    """Request schema for project clone."""

    git_url = fields.String(required=True)
    depth = fields.Integer(missing=PROJECT_CLONE_DEPTH_DEFAULT)


class ProjectCloneContext(ProjectCloneRequest):
    """Context schema for project clone."""

    project_id = fields.String(missing=lambda: uuid.uuid4().hex)

    # measured in ms
    timestamp = fields.Integer(missing=time.time() * 1e+3)

    name = fields.String(required=True)
    fullname = fields.String(required=True)
    email = fields.String(required=True)
    owner = fields.String(required=True)
    token = fields.String(required=True)
    ref = fields.String(missing='master')

    @validates('git_url')
    def validate_git_url(self, value):
        """Validates git url."""
        try:
            GitURL.parse(value)
        except ConfigurationError as e:
            raise ValidationError(str(e))

        return value

    @post_load()
    def format_url(self, data, **kwargs):
        """Format URL with a username and password."""
        git_url = urlparse(data['git_url'])

        url = 'oauth2:{0}@{1}'.format(data['token'], git_url.netloc)
        data['url_with_auth'] = git_url._replace(netloc=url).geturl()

        return data

    @pre_load()
    def set_owner_name(self, data, **kwargs):
        """Set owner and name fields."""
        git_url = GitURL.parse(data['git_url'])

        data['owner'] = git_url.owner
        data['name'] = git_url.name

        return data


class ProjectCloneResponse(Schema):
    """Response schema for project clone."""

    project_id = fields.String(required=True)
    git_url = fields.String(required=True)


class ProjectCloneResponseRPC(JsonRPCResponse):
    """RPC response schema for project clone response."""

    result = fields.Nested(ProjectCloneResponse)


class ProjectListResponse(Schema):
    """Response schema for project listing."""

    projects = fields.List(fields.Nested(ProjectCloneResponse), required=True)


class ProjectListResponseRPC(JsonRPCResponse):
    """RPC response schema for project listing."""

    result = fields.Nested(ProjectListResponse)


class ProjectMigrateRequest(Schema):
    """Request schema for project migrate."""

    project_id = fields.String(required=True)
    is_delayed = fields.Boolean(default=False)


class ProjectMigrateResponse(Schema):
    """Response schema for project migrate."""

    was_migrated = fields.Boolean()
    messages = fields.List(fields.String)


class ProjectMigrateResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateResponse)


class ProjectMigrateJobResponse(Schema):
    """Response schema for enqueued job of project migration."""

    job_id = fields.String()
    created_at = fields.DateTime()


class ProjectMigrateAsyncResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateJobResponse)


class ProjectMigrationCheckResponse(Schema):
    """Response schema for project migration check."""

    migration_required = fields.Boolean()
    project_supported = fields.Boolean()


class ProjectMigrationCheckResponseRPC(JsonRPCResponse):
    """RPC response schema for project migration check."""

    result = fields.Nested(ProjectMigrationCheckResponse)
