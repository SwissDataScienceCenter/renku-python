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
from enum import Enum

from marshmallow import Schema, fields
from marshmallow_oneofschema import OneOfSchema

from renku.domain_model.dataset import DatasetCreatorsJson
from renku.ui.cli.utils.plugins import get_supported_formats
from renku.ui.service.serializers.common import LocalRepositorySchema, RemoteRepositorySchema
from renku.ui.service.serializers.rpc import JsonRPCResponse


class WorkflowPlansListRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for plan list view."""


class AbstractPlanResponse(Schema):
    """Base schema for abstract plan responses."""

    id = fields.String(required=True)
    name = fields.String(required=True)
    description = fields.String()
    type = fields.String()
    created = fields.DateTime()
    creators = fields.List(fields.Nested(DatasetCreatorsJson))
    keywords = fields.List(fields.String())
    touches_existing_files = fields.Boolean()
    duration = fields.Integer(dump_default=None)


class WorflowPlanEntryResponse(AbstractPlanResponse):
    """Serialize a plan to a response object."""

    last_executed = fields.DateTime()
    number_of_executions = fields.Integer()
    children = fields.List(fields.String)


class WorkflowPlansListResponse(Schema):
    """Response schema for plan list view."""

    plans = fields.List(fields.Nested(WorflowPlanEntryResponse), required=True)


class WorkflowPlansListResponseRPC(JsonRPCResponse):
    """RPC response schema for plan list view."""

    result = fields.Nested(WorkflowPlansListResponse)


class WorkflowPlansShowRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for plan show view."""

    plan_id = fields.String(required=True)


class AnnotationSchema(Schema):
    """Custom metadata annotation schema."""

    id = fields.String(required=True)
    source = fields.String()
    body = fields.Raw()


class ParameterBaseSchema(Schema):
    """Base schema for parameters."""

    id = fields.String(required=True)
    plan_id = fields.String(required=True)
    name = fields.String(required=True)
    type = fields.String()
    description = fields.String(allow_none=True)
    default_value = fields.String()
    prefix = fields.String(allow_none=True)
    position = fields.Integer(allow_none=True)


class InputSchema(ParameterBaseSchema):
    """Schema for plan input."""

    mapped_to = fields.String(allow_none=True)
    encoding_format = fields.String(allow_none=True)
    exists = fields.Boolean()


class OutputSchema(ParameterBaseSchema):
    """Schema for plan input."""

    mapped_to = fields.String(allow_none=True)
    encoding_format = fields.String(allow_none=True)
    create_folder = fields.Boolean()
    exists = fields.Boolean()
    last_touched_by_this_plan = fields.Boolean()


class ParameterSchema(ParameterBaseSchema):
    """Schema for a plan parameter."""

    pass


class PlanDetailsResponse(AbstractPlanResponse):
    """Schema for Plan details."""

    last_executed = fields.DateTime()
    number_of_executions = fields.Integer()
    full_command = fields.String()
    command = fields.String()
    inputs = fields.List(fields.Nested(InputSchema))
    outputs = fields.List(fields.Nested(OutputSchema))
    parameters = fields.List(fields.Nested(ParameterSchema))
    success_codes = fields.List(fields.Integer())
    latest = fields.String(dump_default=None)
    annotations = fields.List(fields.Nested(AnnotationSchema))


class PlanReferenceSchema(Schema):
    """Schema for a plan reference."""

    id = fields.String(required=True)
    name = fields.String(required=True)
    description = fields.String(allow_none=True)


class ParameterTargetSchema(Schema):
    """Schema for a mapping target."""

    id = fields.String(required=True)
    plan_id = fields.String(required=True)
    name = fields.String(required=True)
    type = fields.String()


class MappingSchema(Schema):
    """Schema for a plan mapping."""

    id = fields.String(required=True)
    plan_id = fields.String(required=True)
    name = fields.String(required=True)
    type = fields.String()
    description = fields.String(allow_none=True)
    default_value = fields.String()
    targets = fields.List(fields.Nested(ParameterTargetSchema))


class LinkSchema(Schema):
    """Schema for a parameter link."""

    id = fields.String(required=True)
    plan_id = fields.String(required=True)
    type = fields.String()
    source_entry = fields.Nested(ParameterTargetSchema, data_key="source")
    sink_entries = fields.List(fields.Nested(ParameterTargetSchema), data_key="sinks")


class CompositePlanDetailsResponse(AbstractPlanResponse):
    """Schema for Plan details."""

    steps = fields.List(fields.Nested(PlanReferenceSchema), data_key="plans")
    mappings = fields.List(fields.Nested(MappingSchema))
    links = fields.List(fields.Nested(LinkSchema))
    latest = fields.String(dump_default=None)
    annotations = fields.List(fields.Nested(AnnotationSchema))


class PlanSuperSchema(OneOfSchema):
    """Combined schema for Plan and CompositePlan."""

    type_schemas = {"Plan": PlanDetailsResponse, "CompositePlan": CompositePlanDetailsResponse}

    def get_obj_type(self, obj):
        """Get type from object."""
        return str(obj.type)


class WorkflowPlansShowResponseRPC(JsonRPCResponse):
    """Response schema for plan show view."""

    result = fields.Nested(PlanSuperSchema)


WorkflowExportFormatEnum = Enum(  # type: ignore
    "WorkflowExportFormatEnum",
    zip(get_supported_formats(), get_supported_formats()),
)


class WorkflowPlansExportRequest(LocalRepositorySchema, RemoteRepositorySchema):
    """Request schema for exporting a plan."""

    plan_id = fields.String(required=True)
    # NOTE: the enum values (not names) are used to deserialize format, the result from deserialize is Enum
    format = fields.Enum(WorkflowExportFormatEnum, missing=getattr(WorkflowExportFormatEnum, "cwl"), by_value=True)
    values = fields.Dict(keys=fields.String(), missing=None)


class WorkflowPlansExportResponseRPC(JsonRPCResponse):
    """Response schema for exporting a plan."""

    result = fields.String()
