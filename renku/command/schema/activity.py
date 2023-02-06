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
"""Activity JSON-LD schema."""

from marshmallow import EXCLUDE

from renku.command.schema.agent import PersonSchema, SoftwareAgentSchema
from renku.command.schema.annotation import AnnotationSchema
from renku.command.schema.calamus import JsonLDSchema, Nested, fields, oa, prov, renku, schema
from renku.command.schema.entity import CollectionSchema, EntitySchema
from renku.command.schema.plan import PlanSchema
from renku.command.schema.workflow_file import WorkflowFileCompositePlanSchema, WorkflowFilePlanSchema
from renku.domain_model.provenance.activity import (
    Activity,
    Association,
    Generation,
    Usage,
    WorkflowFileActivityCollection,
)
from renku.domain_model.provenance.parameter import ParameterValue


class AssociationSchema(JsonLDSchema):
    """Association schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Association
        model = Association
        unknown = EXCLUDE

    agent = Nested(prov.agent, [SoftwareAgentSchema, PersonSchema])
    id = fields.Id()
    plan = Nested(prov.hadPlan, [PlanSchema, WorkflowFilePlanSchema, WorkflowFileCompositePlanSchema])


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


class ParameterValueSchema(JsonLDSchema):
    """ParameterValue schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.ParameterValue, schema.PropertyValue]
        model = ParameterValue
        unknown = EXCLUDE

    id = fields.Id()

    parameter = fields.IRI(schema.valueReference, attribute="parameter_id")
    value = fields.Raw(schema.value)


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
    generations = Nested(prov.activity, GenerationSchema, reverse=True, many=True, load_default=None)
    id = fields.Id()
    invalidations = Nested(prov.wasInvalidatedBy, EntitySchema, reverse=True, many=True, load_default=None)
    parameters = Nested(
        renku.parameter,
        ParameterValueSchema,
        many=True,
        load_default=None,
    )
    path = fields.String(prov.atLocation)
    project_id = fields.IRI(renku.hasActivity, reverse=True)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
    usages = Nested(prov.qualifiedUsage, UsageSchema, many=True)


class WorkflowFileActivityCollectionSchema(JsonLDSchema):
    """WorkflowFileActivityCollection schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.WorkflowFileActivityCollection, prov.Collection]
        model = WorkflowFileActivityCollection
        unknown = EXCLUDE

    activities = Nested(schema.hasPart, ActivitySchema, many=True)
    agents = Nested(prov.wasAssociatedWith, [PersonSchema, SoftwareAgentSchema], many=True)
    association = Nested(prov.qualifiedAssociation, AssociationSchema)
    ended_at_time = fields.DateTime(prov.endedAtTime, add_value_types=True)
    id = fields.Id()
    project_id = fields.IRI(renku.hasActivityCollection, reverse=True)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
