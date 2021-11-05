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
from urllib.parse import urlparse

from marshmallow import Schema, ValidationError, fields, post_load, pre_load
from werkzeug.utils import secure_filename

from renku.core import errors
from renku.core.models.git import GitURL
from renku.core.utils.os import normalize_to_ascii
from renku.service.config import PROJECT_CLONE_DEPTH_DEFAULT
from renku.service.serializers.common import (
    ArchiveSchema,
    AsyncSchema,
    FileDetailsSchema,
    LocalRepositorySchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.service.serializers.rpc import JsonRPCResponse


def extract_file(request):
    """Extract file from Flask request.

    :raises: `ValidationError`
    """
    files = request.files
    if "file" not in files:
        raise ValidationError("missing key: file")

    file = files["file"]
    if file and not file.filename:
        raise ValidationError("wrong filename: {0}".format(file.filename))

    if file:
        file.filename = secure_filename(file.filename)
        return file


class FileUploadRequest(ArchiveSchema):
    """Request schema for file upload."""

    override_existing = fields.Boolean(
        missing=False,
        description="Overried files. Useful when extracting from archives.",
    )


class FileUploadResponse(Schema):
    """Response schema for file upload."""

    files = fields.List(fields.Nested(FileDetailsSchema), required=True)


class FileUploadResponseRPC(JsonRPCResponse):
    """RPC response schema for file upload response."""

    result = fields.Nested(FileUploadResponse)


class FileListResponse(Schema):
    """Response schema for files listing."""

    files = fields.List(fields.Nested(FileDetailsSchema), required=True)


class FileListResponseRPC(JsonRPCResponse):
    """RPC response schema for files listing."""

    result = fields.Nested(FileListResponse)


class RepositoryCloneRequest(RemoteRepositorySchema):
    """Request schema for repository clone."""

    depth = fields.Integer(description="Git fetch depth", missing=PROJECT_CLONE_DEPTH_DEFAULT)
    ref = fields.String(description="Repository reference (branch, commit or tag)", missing=None)


class ProjectCloneContext(RepositoryCloneRequest):
    """Context schema for project clone."""

    # measured in ms
    timestamp = fields.Integer(missing=time.time() * 1e3)

    # user data
    name = fields.String()
    slug = fields.String()
    fullname = fields.String()
    email = fields.String()
    owner = fields.String()
    token = fields.String()

    @pre_load()
    def set_missing_id(self, data, **kwargs):
        """Set project_id when missing."""
        if not data.get("project_id"):
            data["project_id"] = lambda: uuid.uuid4().hex

        return data

    @pre_load()
    def set_owner_name(self, data, **kwargs):
        """Set owner and name fields."""
        try:
            git_url = GitURL.parse(data["git_url"])
        except UnicodeError as e:
            raise ValidationError("`git_url` contains unsupported characters") from e
        except errors.GitConfigurationError as e:
            raise ValidationError("Invalid `git_url`") from e

        if git_url.owner is None:
            raise ValidationError("Invalid `git_url`")
        data["owner"] = git_url.owner

        if git_url.name is None:
            raise ValidationError("Invalid `git_url`")
        data["name"] = git_url.name
        data["slug"] = normalize_to_ascii(data["name"])

        return data

    def format_url(self, data):
        """Format url with auth."""
        git_url = urlparse(data["git_url"])

        url = "oauth2:{0}@{1}".format(data["token"], git_url.netloc)
        return git_url._replace(netloc=url).geturl()

    @post_load
    def finalize_data(self, data, **kwargs):
        """Finalize data."""
        data["url_with_auth"] = self.format_url(data)

        if not data["depth"]:
            # NOTE: In case of `depth=None` or `depth=0` we set to default depth.
            data["depth"] = PROJECT_CLONE_DEPTH_DEFAULT

        try:
            depth = int(data["depth"])

            if depth < 0:
                # NOTE: In case of `depth<0` we remove the depth limit.
                data["depth"] = None

        except ValueError:
            data["depth"] = PROJECT_CLONE_DEPTH_DEFAULT

        return data


class ProjectCloneResponse(Schema):
    """Response schema for project clone."""

    project_id = fields.String(required=True)
    git_url = fields.String(required=True)
    initialized = fields.Boolean(default=False)


class ProjectCloneResponseRPC(JsonRPCResponse):
    """RPC response schema for project clone response."""

    result = fields.Nested(ProjectCloneResponse)


class ProjectListResponse(Schema):
    """Response schema for project listing."""

    projects = fields.List(fields.Nested(ProjectCloneResponse), required=True)


class ProjectListResponseRPC(JsonRPCResponse):
    """RPC response schema for project listing."""

    result = fields.Nested(ProjectListResponse)


class ProjectMigrateRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for project migrate."""

    force_template_update = fields.Boolean(default=False)
    skip_template_update = fields.Boolean(default=False)
    skip_docker_update = fields.Boolean(default=False)
    skip_migrations = fields.Boolean(default=False)

    @pre_load()
    def handle_ref(self, data, **kwargs):
        """Handle ref and branch."""

        # Backward compatibility: branch and ref were both used. Let's keep branch as the exposed field
        # even if interally it gets converted to "ref" later.
        if data.get("ref"):
            data["branch"] = data["ref"]
            del data["ref"]

        return data


class ProjectMigrateResponse(RenkuSyncSchema):
    """Response schema for project migrate."""

    was_migrated = fields.Boolean()
    template_migrated = fields.Boolean()
    docker_migrated = fields.Boolean()
    messages = fields.List(fields.String)


class ProjectMigrateResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateResponse)


class ProjectMigrationCheckRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for project migration check."""


class ProjectMigrationCheckResponse(Schema):
    """Response schema for project migration check."""

    migration_required = fields.Boolean()
    template_update_possible = fields.Boolean()
    automated_template_update = fields.Boolean()
    current_template_version = fields.String(allow_none=True)
    latest_template_version = fields.String(allow_none=True)
    template_source = fields.String()
    template_ref = fields.String()
    template_id = fields.String()
    docker_update_possible = fields.Boolean()
    project_supported = fields.Boolean()
    project_version = fields.String()
    latest_version = fields.String()


class ProjectMigrationCheckResponseRPC(JsonRPCResponse):
    """RPC response schema for project migration check."""

    result = fields.Nested(ProjectMigrationCheckResponse)
