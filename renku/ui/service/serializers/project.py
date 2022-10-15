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

from renku.domain_model.dataset import DatasetCreatorsJson as DatasetCreators
from renku.ui.service.serializers.common import (
    AsyncSchema,
    LocalRepositorySchema,
    MigrateSchema,
    RemoteRepositoryBaseSchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.ui.service.serializers.rpc import JsonRPCResponse


class ProjectShowRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Project show metadata request."""


class ProjectShowResponse(Schema):
    """Response schema for project show."""

    id = fields.String(metadata={"description": "The ID of this project"})
    name = fields.String(metadata={"description": "The name of the project"})
    description = fields.String(dump_default=None, metadata={"description": "The optional description of the project"})
    created = fields.DateTime(metadata={"description": "The date this project was created at."})
    creator = fields.Nested(DatasetCreators, metadata={"description": "The creator of this project"})
    agent = fields.String(metadata={"description": "The renku version last used on this project"})
    custom_metadata = fields.List(
        fields.Dict(),
        dump_default=None,
        attribute="annotations",
        metadata={"description": "Custom JSON-LD metadata of the project"},
    )
    template_info = fields.String(
        metadata={"description": "The template that was used in the creation of this project"}
    )
    keywords = fields.List(
        fields.String(),
        dump_default=None,
        load_default=None,
        metadata={"description": "They keywords associated with this project"},
    )


class ProjectShowResponseRPC(RenkuSyncSchema):
    """RPC schema for project show."""

    result = fields.Nested(ProjectShowResponse)


class ProjectEditRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Project edit metadata request."""

    description = fields.String(metadata={"description": "New description for the project"})
    creator = fields.Nested(DatasetCreators, metadata={"description": "New creator for the project"})
    custom_metadata = fields.List(
        fields.Dict(), metadata={"description": "New custom metadata for the project"}, allow_none=True
    )
    custom_metadata_source = fields.String(
        allow_none=True,
        metadata={"description": "The source for the JSON-LD metadata"},
    )
    keywords = fields.List(fields.String(), allow_none=True, metadata={"description": "New keyword(s) for the project"})


class ProjectEditResponse(RenkuSyncSchema):
    """Project edit metadata response."""

    edited = fields.Dict(required=True, metadata={"description": "Key:value paris of edited metadata"})
    warning = fields.String(metadata={"description": "Warnings raised when editing metadata"})


class ProjectEditResponseRPC(JsonRPCResponse):
    """RPC schema for a project edit."""

    result = fields.Nested(ProjectEditResponse)


class ProjectLockStatusRequest(LocalRepositorySchema, RemoteRepositoryBaseSchema):
    """Project lock status request."""

    timeout = fields.Float(
        dump_default=0.0,
        load_default=0.0,
        metadata={"description": "Maximum amount of time to wait trying to acquire a lock when checking status."},
    )


class ProjectLockStatusResponse(Schema):
    """Project lock status response."""

    locked = fields.Boolean(required=True, metadata={"description": "Whether or not a project is locked for writing"})


class ProjectLockStatusResponseRPC(JsonRPCResponse):
    """RPC schema for project lock status."""

    result = fields.Nested(ProjectLockStatusResponse)
