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

from marshmallow import EXCLUDE, pre_dump

from renku.core.commands.schema.agent import PersonSchema, SoftwareAgentSchema
from renku.core.commands.schema.annotation import AnnotationSchema
from renku.core.commands.schema.calamus import JsonLDSchema, Nested, fields, oa, prov, renku, schema
from renku.core.commands.schema.entity import CollectionSchema, EntitySchema
from renku.core.commands.schema.plan import PlanSchema
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.provenance.parameter import ParameterValue

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40


class _ObjectWrapper:
    """Object wrapper that allows temporarily overriding fields of immutable objects."""

    def __init__(self, wrapped, **override):
        self.__wrapped = wrapped
        self.__override = override

    def __getattr__(self, name):
        if name in self.__override:
            return self.__override[name]

        return getattr(self.__wrapped, name)


def _fix_id(obj):
    """Fix ids under an activity that were wrong due to a bug."""

    if not obj.id.startswith("/activities/"):
        obj = _ObjectWrapper(obj, id=f"/activities/{obj.id}")

    return obj


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

    @pre_dump
    def _pre_dump(self, obj, **kwargs):
        """Pre-dump hook."""
        return _fix_id(obj)


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

    @pre_dump
    def _pre_dump(self, obj, **kwargs):
        """Pre-dump hook."""
        return _fix_id(obj)


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

    @pre_dump
    def _pre_dump(self, obj, **kwargs):
        """Pre-dump hook."""
        return _fix_id(obj)


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

    @pre_dump
    def _pre_dump(self, obj, **kwargs):
        """Pre-dump hook."""
        return _fix_id(obj)


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

    @pre_dump
    def _pre_dump(self, obj, **kwargs):
        """Pre-dump hook."""
        return _fix_id(obj)
