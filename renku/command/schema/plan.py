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

import marshmallow

from renku.command.schema.agent import PersonSchema
from renku.command.schema.annotation import AnnotationSchema
from renku.command.schema.calamus import JsonLDSchema, Nested, fields, oa, prov, renku, schema
from renku.command.schema.parameter import CommandInputSchema, CommandOutputSchema, CommandParameterSchema
from renku.domain_model.workflow.plan import Plan

MAX_GENERATED_NAME_LENGTH = 25


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork, renku.Plan]
        model = Plan
        unknown = marshmallow.EXCLUDE

    command = fields.String(renku.command, load_default=None)
    description = fields.String(schema.description, load_default=None)
    creators = Nested(schema.creator, PersonSchema, many=True)
    id = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, load_default=None)
    date_created = fields.DateTime(schema.dateCreated, format="iso")
    date_modified = fields.DateTime(schema.dateModified, format="iso")
    date_removed = fields.DateTime(prov.invalidatedAtTime, format="iso")
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)
    name = fields.String(schema.name, load_default=None)
    derived_from = fields.IRI(prov.wasDerivedFrom, load_default=None)
    project_id = fields.IRI(renku.hasPlan, reverse=True)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, load_default=None)
    parameters = Nested(renku.hasArguments, CommandParameterSchema, many=True, load_default=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), load_default=[0])
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
