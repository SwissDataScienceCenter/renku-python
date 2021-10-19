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
from itertools import chain
from typing import List, Union
from uuid import uuid4

from marshmallow import EXCLUDE
from werkzeug.utils import cached_property

from renku.core.management.command_builder import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.metadata.database import Persistent
from renku.core.metadata.immutable import Immutable
from renku.core.models.calamus import JsonLDSchema, Nested, fields, oa, prov, renku
from renku.core.models.entity import Collection, CollectionSchema, Entity, EntitySchema
from renku.core.models.provenance.agent import Person, PersonSchema, SoftwareAgent, SoftwareAgentSchema
from renku.core.models.provenance.annotation import Annotation, AnnotationSchema
from renku.core.models.provenance.parameter import ParameterValue, ParameterValueSchema
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
        parameters: List[ParameterValue] = None,
        project_id: str = None,
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
        self.parameters: List[ParameterValue] = parameters or []
        self.project_id: str = project_id
        self.started_at_time: datetime = started_at_time
        self.usages: List[Usage] = usages or []

        # TODO: _was_informed_by = attr.ib(kw_only=True)
        # TODO: influenced = attr.ib(kw_only=True)

    @classmethod
    @inject.autoparams("client_dispatcher", "project_gateway")
    def from_plan(
        cls,
        plan: Plan,
        client_dispatcher: IClientDispatcher,
        project_gateway: IProjectGateway,
        started_at_time: datetime,
        ended_at_time: datetime,
        annotations: List[Annotation] = None,
        commit=None,
        update_commits=False,
    ):
        """Convert a ``Plan`` to a ``Activity``."""
        from renku.core.models.provenance.agent import SoftwareAgent
        from renku.core.plugins.pluginmanager import get_plugin_manager

        client = client_dispatcher.current_client

        if not commit:
            commit = client.repo.head.commit

        usages = {}
        generations = {}
        parameter_values = []

        activity_id = cls.generate_id()

        for input in plan.inputs:
            input_path = input.actual_value

            parameter_values.append(
                ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=input.id, value=input_path)
            )

            if input_path in usages:
                continue

            entity = Entity.from_revision(client, path=input_path, revision=commit.hexsha)

            dependency = Usage(entity=entity, id=Usage.generate_id(activity_id))

            usages[input_path] = dependency

        for output in plan.outputs:
            output_path = output.actual_value

            parameter_values.append(
                ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=output.id, value=output_path)
            )

            if output_path in generations:
                continue

            entity = Entity.from_revision(client, path=output_path, revision=commit.hexsha)

            generation = Generation(entity=entity, id=Usage.generate_id(activity_id))

            generations[output_path] = generation

        for parameter in plan.parameters:
            value = parameter.actual_value

            parameter_values.append(
                ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=parameter.id, value=value)
            )

        agent = SoftwareAgent.from_commit(commit)
        person = Person.from_client(client)
        association = Association(agent=agent, id=Association.generate_id(activity_id), plan=plan)

        activity = cls(
            id=activity_id,
            association=association,
            agents=[agent, person],
            usages=list(usages.values()),
            generations=list(generations.values()),
            parameters=parameter_values,
            project_id=project_gateway.get_project().id,
            started_at_time=started_at_time,
            ended_at_time=ended_at_time,
            annotations=annotations,
        )

        pm = get_plugin_manager()

        plugin_annotations = list(chain.from_iterable(pm.hook.activity_annotations(activity=activity)))

        if plugin_annotations:
            activity.annotations.extend(plugin_annotations)

        return activity

    @cached_property
    def plan_with_values(self) -> Plan:
        """Get a copy of the associated plan with values from ParameterValues applied."""
        plan = self.association.plan.copy()

        for parameter in self.parameters:
            parameter.apply_value_to_parameter(plan)

        return plan

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier for an activity."""
        # TODO: make id generation idempotent
        return f"/activities/{uuid4().hex}"

    def has_identical_inputs_and_outputs_as(self, other: "Activity"):
        """Return true if all input and outputs paths are identical regardless of the order."""
        return sorted(u.entity.path for u in self.usages) == sorted(u.entity.path for u in other.usages) and sorted(
            g.entity.path for g in self.generations
        ) == sorted(g.entity.path for g in other.generations)

    def compare_to(self, other: "Activity") -> int:
        """Compare execution date with another activity; return a positive value if self is executed after the other."""
        if self.ended_at_time < other.ended_at_time:
            return -1
        elif self.ended_at_time > other.ended_at_time:
            return 1
        elif self.started_at_time < other.started_at_time:
            return -1
        elif self.started_at_time > other.started_at_time:
            return 1

        return 0


class ActivityCollection(Persistent):
    """Represent a list of activities."""

    def __init__(self, *, activities: List[Activity], id: str = None):
        self.activities: List[Activity] = activities or []
        self.id: str = id or ActivityCollection.generate_id()

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier for an activity."""
        return f"/activity-collection/{uuid4().hex}"


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
