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
"""Renku service project serializers."""
from marshmallow import fields
from marshmallow.schema import Schema

from renku.core.models.dataset import DatasetCreatorsJson as DatasetCreators
from renku.service.serializers.common import (
    AsyncSchema,
    LocalRepositorySchema,
    MigrateSchema,
    RemoteRepositoryBaseSchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.service.serializers.rpc import JsonRPCResponse


class ProjectShowRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Project show metadata request."""


class ProjectShowResponse(Schema):
    """Response schema for project show."""

    id = fields.String(description="The ID of this project")
    name = fields.String(description="The name of the project")
    description = fields.String(default=None, description="The optional description of the project")
    created = fields.DateTime(description="The date this project was created at.")
    creator = fields.Nested(DatasetCreators, description="The creator of this project")
    agent = fields.String(description="The renku version last used on this project")
    custom_metadata = fields.Dict(
        default=None, attribute="annotations", description="Custom JSON-LD metadata of the project"
    )
    template_info = fields.String(description="The template that was used in the creation of this project")
    keywords = fields.List(
        fields.String(), default=None, Missing=None, description="They keywords associated with this project"
    )


class ProjectShowResponseRPC(RenkuSyncSchema):
    """RPC schema for project show."""

    result = fields.Nested(ProjectShowResponse)


class ProjectEditRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Project edit metadata request."""

    description = fields.String(default=None, description="New description for the project")
    creator = fields.Nested(DatasetCreators, description="New creator for the project")
    custom_metadata = fields.Dict(default=None, description="Custom JSON-LD metadata")
    keywords = fields.List(fields.String(), default=None, Missing=None, description="Keyword(s) for the project")


class ProjectEditResponse(RenkuSyncSchema):
    """Project edit metadata response."""

    edited = fields.Dict(required=True, description="Key:value paris of edited metadata")
    warning = fields.String(description="Warnings raised when editing metadata")


class ProjectEditResponseRPC(JsonRPCResponse):
    """RPC schema for a project edit."""

    result = fields.Nested(ProjectEditResponse)


class ProjectLockStatusRequest(LocalRepositorySchema, RemoteRepositoryBaseSchema):
    """Project lock status request."""


class ProjectLockStatusResponse(Schema):
    """Project lock status response."""

    locked = fields.Boolean(required=True, description="Whether or not a project is locked for writing")


class ProjectLockStatusResponseRPC(JsonRPCResponse):
    """RPC schema for project lock status."""

    result = fields.Nested(ProjectLockStatusResponse)
