# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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

import marshmallow

from renku.core.commands.schema.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.commands.schema.parameter import CommandInputSchema, CommandOutputSchema, CommandParameterSchema
from renku.core.models.workflow.plan import Plan

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
