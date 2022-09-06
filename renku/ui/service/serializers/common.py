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
"""Renku service parent serializers."""
import uuid
from datetime import datetime

import yagup
from marshmallow import Schema, fields, validates

from renku.ui.service.errors import UserRepoUrlInvalidError
from renku.ui.service.serializers.rpc import JsonRPCResponse


class LocalRepositorySchema(Schema):
    """Schema for identifying a locally stored repository."""

    # In the long term, the id should be used only for internal operations
    project_id = fields.String(metadata={"description": "Reference to access the project in the local cache."})


class RemoteRepositoryBaseSchema(Schema):
    """Schema for tracking a remote repository."""

    git_url = fields.String(metadata={"description": "Remote git repository url."})

    @validates("git_url")
    def validate_git_url(self, value):
        """Validates git url."""
        if value:
            try:
                yagup.parse(value)
            except yagup.exceptions.InvalidURL as e:
                raise UserRepoUrlInvalidError(e, "Invalid `git_url`")

        return value


class RemoteRepositorySchema(RemoteRepositoryBaseSchema):
    """Schema for tracking a remote repository and branch."""

    branch = fields.String(metadata={"description": "Remote git branch."})


class AsyncSchema(Schema):
    """Schema for adding a commit at the end of the operation."""

    is_delayed = fields.Boolean(metadata={"description": "Whether the job should be delayed or not."})


class MigrateSchema(Schema):
    """Schema for allowing preliminary repository migration."""

    migrate_project = fields.Boolean(
        default=False,
        load_default=False,
        metadata={"description": "Whether the project should be migrated before the other operations take place."},
    )


class ArchiveSchema(Schema):
    """Schema for unpacking archives."""

    unpack_archive = fields.Boolean(
        load_default=False,
        metadata={"description": "Whether to automatically extract archive content."},
    )


class MandatoryUserSchema(Schema):
    """Schema for adding user as requirement."""

    user_id = fields.String(
        required=True,
        metadata={"description": "Mandatory user id."},
    )


class CreationSchema(Schema):
    """Schema for creation date."""

    created_at = fields.DateTime(
        load_default=datetime.utcnow,
        metadata={"description": "Creation date."},
    )


class FileDetailsSchema(ArchiveSchema, CreationSchema):
    """Schema for file details."""

    file_id = fields.String(load_default=lambda: uuid.uuid4().hex)
    content_type = fields.String(load_default="unknown")
    file_name = fields.String(required=True)

    # measured in bytes (comes from stat() - st_size)
    file_size = fields.Integer(required=True)

    relative_path = fields.String(required=True)
    is_archive = fields.Boolean(load_default=False)
    is_dir = fields.Boolean(required=True)


class RenkuSyncSchema(Schema):
    """Parent schema for all Renku write operations."""

    remote_branch = fields.String()


class JobDetailsResponse(Schema):
    """Response schema for enqueued job."""

    job_id = fields.String()
    created_at = fields.DateTime()


class DelayedResponseRPC(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(JobDetailsResponse)
