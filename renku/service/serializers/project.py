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

from renku.core.models.dataset import DatasetCreatorsJson as DatasetCreators
from renku.service.serializers.common import (
    AsyncSchema,
    LocalRepositorySchema,
    MigrateSchema,
    RemoteRepositorySchema,
    RenkuSyncSchema,
)
from renku.service.serializers.rpc import JsonRPCResponse


class ProjectEditRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Project edit metadata request."""

    description = fields.String(default=None)
    creator = fields.Nested(DatasetCreators)
    custom_metadata = fields.Dict(default=None)


class ProjectEditResponse(RenkuSyncSchema):
    """Project edit metadata response."""

    edited = fields.Dict(required=True)
    warning = fields.String()


class ProjectEditResponseRPC(JsonRPCResponse):
    """RPC schema for a project edit."""

    result = fields.Nested(ProjectEditResponse)
