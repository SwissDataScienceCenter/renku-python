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
"""Renku graph serializers."""
from marshmallow import Schema, fields

from renku.service.serializers.common import AsyncSchema, LocalRepositorySchema, MigrateSchema, RemoteRepositorySchema
from renku.service.serializers.rpc import JsonRPCResponse


class GraphExportRequest(AsyncSchema, LocalRepositorySchema, RemoteRepositorySchema, MigrateSchema):
    """Request schema for dataset list view."""

    callback_url = fields.URL()
    revision = fields.String(missing="HEAD", allow_none=True)
    format = fields.String(missing="json-ld")


class GraphExportResponse(Schema):
    """Response schema for dataset list view."""

    graph = fields.String()


class GraphExportResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(GraphExportResponse)


class GraphExportCallback(Schema):
    """Callback serializer for graph build."""

    project_url = fields.String()
    commit_id = fields.String()
    type = fields.String(default="RENKU_LOG")


class GraphExportCallbackSuccess(GraphExportCallback):
    """Success callback serializer for graph build."""

    payload = fields.String(default=None, missing=None)


class GraphExportCallbackError(GraphExportCallback):
    """Error callback serializer for graph build."""

    failure = fields.Dict()
