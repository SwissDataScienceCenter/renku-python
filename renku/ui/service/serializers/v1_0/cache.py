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

from marshmallow import fields

from renku.ui.service.serializers.common import RenkuSyncSchema
from renku.ui.service.serializers.rpc import JsonRPCResponse


class ProjectMigrateResponse_1_0(RenkuSyncSchema):
    """Response schema for project migrate."""

    was_migrated = fields.Boolean()
    template_migrated = fields.Boolean()
    docker_migrated = fields.Boolean()
    messages = fields.List(fields.String)


class ProjectMigrateResponseRPC_1_0(JsonRPCResponse):
    """RPC response schema for project migrate."""

    result = fields.Nested(ProjectMigrateResponse_1_0)
