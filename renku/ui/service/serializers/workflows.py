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
"""Renku service workflow serializers."""
from marshmallow import Schema, fields

from renku.domain_model.dataset import DatasetCreatorsJson
from renku.ui.service.serializers.common import LocalRepositorySchema, RemoteRepositorySchema
from renku.ui.service.serializers.rpc import JsonRPCResponse


class WorkflowPlansListRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for plan list view."""


class WorflowPlanEntryResponse(Schema):
    """Serialize a plan to a response object."""

    id = fields.String(required=True)
    name = fields.String(required=True)
    description = fields.String()
    type = fields.String()
    created = fields.DateTime()
    creators = fields.List(fields.Nested(DatasetCreatorsJson))
    last_executed = fields.DateTime()
    keywords = fields.List(fields.String())
    number_of_executions = fields.Integer()
    touches_existing_files = fields.Boolean()
    children = fields.List(fields.String)


class WorkflowPlansListResponse(Schema):
    """Response schema for plan list view."""

    plans = fields.List(fields.Nested(WorflowPlanEntryResponse), required=True)


class WorkflowPlansListResponseRPC(JsonRPCResponse):
    """RPC response schema for plan list view."""

    result = fields.Nested(WorkflowPlansListResponse)
