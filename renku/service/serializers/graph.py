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

from renku.service.serializers.rpc import JsonRPCResponse


class GraphBuildRequest(Schema):
    """Request schema for dataset list view."""

    git_url = fields.String(required=True)
    callback_url = fields.URL(required=True)
    revision = fields.String()
    format = fields.String()


class GraphBuildResponse(Schema):
    """Response schema for dataset list view."""

    status = fields.String(default="ok")


class GraphBuildResponseRPC(JsonRPCResponse):
    """RPC response schema for dataset list view."""

    result = fields.Nested(GraphBuildResponse)


class GraphBuildCallback(Schema):
    """Callback serializer for graph build."""

    project_url = fields.String()
    commit_id = fields.String()
    type = fields.String(default="RENKU_LOG")


class GraphBuildCallbackSuccess(GraphBuildCallback):
    """Success callback serializer for graph build."""

    payload = fields.String()


class GraphBuildCallbackError(GraphBuildCallback):
    """Error callback serializer for graph build."""

    failure = fields.Dict()
