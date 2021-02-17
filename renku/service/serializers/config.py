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
"""Renku service config serializers."""

from marshmallow import Schema, fields

from renku.service.serializers.rpc import JsonRPCResponse


class ConfigShowRequest(Schema):
    """Request schema for config show."""

    project_id = fields.String()

    git_url = fields.String()
    branch = fields.String()


class ConfigShowResponse(Schema):
    """Response schema for project config show."""

    config = fields.Dict(required=True)
    default = fields.Dict(required=True)


class ConfigShowResponseRPC(JsonRPCResponse):
    """RPC response schema for project config show response."""

    result = fields.Nested(ConfigShowResponse)


class ConfigSetRequest(Schema):
    """Request schema for config set."""

    project_id = fields.String()

    git_url = fields.String()
    branch = fields.String()

    config = fields.Dict(required=True)


class ConfigSetResponse(Schema):
    """Response schema for project config set."""

    config = fields.Dict(required=True)
    default = fields.Dict(required=True)


class ConfigSetResponseRPC(JsonRPCResponse):
    """RPC response schema for project config set response."""

    result = fields.Nested(ConfigSetResponse)
