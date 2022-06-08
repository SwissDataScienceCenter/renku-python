# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Represent run templates."""

from datetime import timezone

import marshmallow

from renku.command.schema.annotation import AnnotationSchema
from renku.command.schema.calamus import JsonLDSchema, Nested, fields, oa, prov, renku, schema
from renku.command.schema.parameter import CommandInputSchema, CommandOutputSchema, CommandParameterSchema
from renku.domain_model.workflow.plan import Plan

MAX_GENERATED_NAME_LENGTH = 25


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork]
        model = Plan
        unknown = marshmallow.EXCLUDE

    command = fields.String(renku.command, missing=None)
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, missing=None)
    date_created = fields.DateTime(schema.dateCreated, format="iso")
    invalidated_at = fields.DateTime(prov.invalidatedAtTime, format="iso")
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    name = fields.String(schema.name, missing=None)
    derived_from = fields.String(prov.wasDerivedFrom, missing=None)
    project_id = fields.IRI(renku.hasPlan, reverse=True)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, missing=None)
    parameters = Nested(renku.hasArguments, CommandParameterSchema, many=True, missing=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), missing=[0])
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)

    @marshmallow.pre_dump
    def _pre_dump(self, in_data, **kwargs):
        """Fix data on dumping."""
        if in_data.invalidated_at is not None and in_data.invalidated_at.tzinfo is None:
            # NOTE: There was a bug that caused invalidated_at to be set without timezone (as UTC time)
            # so we patch in the timezone here
            in_data.unfreeze()
            in_data.invalidated_at = in_data.invalidated_at.replace(microsecond=0).astimezone(timezone.utc)
            in_data.freeze()
        return in_data
