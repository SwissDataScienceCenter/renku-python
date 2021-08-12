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
"""Represent an execution of a Plan."""

from datetime import datetime
from typing import List, Union
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core.management.command_builder import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.metadata.database import Persistent
from renku.core.metadata.immutable import Immutable
from renku.core.models.calamus import JsonLDSchema, Nested, fields, oa, prov, renku
from renku.core.models.entity import Collection, CollectionSchema, Entity, EntitySchema
from renku.core.models.provenance.agent import Person, PersonSchema, SoftwareAgent, SoftwareAgentSchema
from renku.core.models.provenance.annotation import Annotation, AnnotationSchema
from renku.core.models.provenance.parameter import (
    ParameterValueSchema,
    PathParameterValue,
    PathParameterValueSchema,
    VariableParameterValue,
    VariableParameterValueSchema,
)
from renku.core.models.workflow.plan import Plan, PlanSchema

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40


class Association:
    """Assign responsibility to an agent for an activity."""

    def __init__(self, *, agent: Union[Person, SoftwareAgent] = None, id: str, plan: Plan):
        self.agent: Union[Person, SoftwareAgent] = agent
        self.id: str = id
        self.plan: Plan = plan

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Association identifier."""
        return f"{activity_id}/association"  # TODO: Does it make sense to use plural name here?


class Usage(Immutable):
    """Represent a dependent path."""

    __slots__ = ("entity",)

    entity: Union[Collection, Entity]
    id: str

    def __init__(self, *, entity: Union[Collection, Entity], id: str):
        super().__init__(entity=entity, id=id)

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Usage identifier."""
        return f"{activity_id}/usages/{uuid4().hex}"


class Generation(Immutable):
    """Represent an act of generating a path."""

    __slots__ = ("entity",)

    entity: Union[Collection, Entity]
    id: str

    def __init__(self, *, entity: Union[Collection, Entity], id: str):
        super().__init__(entity=entity, id=id)

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Generation identifier."""
        return f"{activity_id}/generations/{uuid4().hex}"


# @total_ordering
class Activity(Persistent):
    """Represent an activity in the repository."""

    def __init__(
        self,
        *,
        agents: List[Union[Person, SoftwareAgent]] = None,
        annotations: List[Annotation] = None,
        association: Association = None,
        ended_at_time: datetime = None,
        generations: List[Generation] = None,
        id: str,
        invalidations: List[Entity] = None,
        parameters: List[Union[PathParameterValue, VariableParameterValue]] = None,
        started_at_time: datetime = None,
        usages: List[Usage] = None,
    ):
        self.agents: List[Union[Person, SoftwareAgent]] = agents
        self.annotations: List[Annotation] = annotations or []
        self.association: Association = association
        self.ended_at_time: datetime = ended_at_time
        self.generations: List[Generation] = generations or []
        self.id: str = id
        self.invalidations: List[Entity] = invalidations or []
        self.parameters: List[Union[PathParameterValue, VariableParameterValue]] = parameters or []
        # self.project: Project = project
        self.started_at_time: datetime = started_at_time
        self.usages: List[Usage] = usages or []

        # TODO: _was_informed_by = attr.ib(kw_only=True)
        # TODO: influenced = attr.ib(kw_only=True)

    @classmethod
    @inject.autoparams()
    def from_plan(
        cls,
        plan: Plan,
        client_dispatcher: IClientDispatcher,
        started_at_time: datetime,
        ended_at_time: datetime,
        annotations: List[Annotation],
        commit=None,
        update_commits=False,
    ):
        """Convert a ``Plan`` to a ``Activity``."""
        from renku.core.models.provenance.agent import SoftwareAgent

        client = client_dispatcher.current_client

        if not commit:
            commit = client.repo.head.commit

        usages = []
        generations = []
        parameter_values = []

        activity_id = cls.generate_id()

        for input_ in plan.inputs:
            input_path = input_.default_value
            entity = Entity.from_revision(client, path=input_path, revision=commit.hexsha)

            dependency = Usage(entity=entity, id=Usage.generate_id(activity_id))

            usages.append(dependency)

        for output in plan.outputs:
            output_path = output.default_value
            entity = Entity.from_revision(client, path=output_path, revision=commit.hexsha)

            generation = Generation(entity=entity, id=Usage.generate_id(activity_id))

            generations.append(generation)

        agent = SoftwareAgent.from_commit(commit)
        association = Association(agent=agent, id=Association.generate_id(activity_id), plan=plan)

        return cls(
            id=activity_id,
            association=association,
            agents=[agent],
            usages=usages,
            generations=generations,
            parameters=parameter_values,
            started_at_time=started_at_time,
            ended_at_time=ended_at_time,
            annotations=annotations,
        )

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier for an activity."""
        # TODO: make id generation idempotent
        return f"/activities/{uuid4().hex}"

    # def __eq__(self, other):
    #     """Implements total_ordering equality."""
    #     if isinstance(other, str):
    #         return self.id == other

    #     assert isinstance(other, Activity), f"Not an activity: {type(other)}"
    #     return self.id == other.id

    # def __lt__(self, other):
    #     """Implement total_ordering less than."""
    #     assert isinstance(other, Activity), f"Not an activity: {type(other)}"
    #     return ((self.ended_at_time, self.id) < (other.ended_at_time, other.id))


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
    order = fields.Integer(renku.order)
    parameters = Nested(
        renku.parameter,
        [ParameterValueSchema, PathParameterValueSchema, VariableParameterValueSchema],
        many=True,
        missing=None,
    )
    path = fields.String(prov.atLocation)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
    usages = Nested(prov.qualifiedUsage, UsageSchema, many=True)
