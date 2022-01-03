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
"""Activity JSON-LD schema."""

from marshmallow import EXCLUDE

from renku.core.commands.schema.agent import PersonSchema, SoftwareAgentSchema
from renku.core.commands.schema.annotation import AnnotationSchema
from renku.core.commands.schema.calamus import JsonLDSchema, Nested, fields, oa, prov, renku
from renku.core.commands.schema.entity import CollectionSchema, EntitySchema
from renku.core.commands.schema.parameter import ParameterValueSchema
from renku.core.commands.schema.plan import PlanSchema
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40


class AssociationSchema(JsonLDSchema):
    """Association schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Association
        model = Association
        unknown = EXCLUDE

    agent = Nested(prov.agent, [SoftwareAgentSchema, PersonSchema])
    id = fields.Id()
    plan = Nested(prov.hadPlan, PlanSchema)


class UsageSchema(JsonLDSchema):
    """Usage schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Usage
        model = Usage
        unknown = EXCLUDE

    id = fields.Id()
    # TODO: DatasetSchema, DatasetFileSchema
    entity = Nested(prov.entity, [EntitySchema, CollectionSchema])


class GenerationSchema(JsonLDSchema):
    """Generation schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Generation
        model = Generation
        unknown = EXCLUDE

    id = fields.Id()
    # TODO: DatasetSchema, DatasetFileSchema
    entity = Nested(prov.qualifiedGeneration, [EntitySchema, CollectionSchema], reverse=True)


class ActivitySchema(JsonLDSchema):
    """Activity schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Activity
        model = Activity
        unknown = EXCLUDE

    agents = Nested(prov.wasAssociatedWith, [PersonSchema, SoftwareAgentSchema], many=True)
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    association = Nested(prov.qualifiedAssociation, AssociationSchema)
    ended_at_time = fields.DateTime(prov.endedAtTime, add_value_types=True)
    generations = Nested(prov.activity, GenerationSchema, reverse=True, many=True, missing=None)
    id = fields.Id()
    invalidations = Nested(prov.wasInvalidatedBy, EntitySchema, reverse=True, many=True, missing=None)
    parameters = Nested(
        renku.parameter,
        ParameterValueSchema,
        many=True,
        missing=None,
    )
    path = fields.String(prov.atLocation)
    project_id = fields.IRI(renku.hasActivity, reverse=True)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
    usages = Nested(prov.qualifiedUsage, UsageSchema, many=True)
