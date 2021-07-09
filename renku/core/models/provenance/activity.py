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
from typing import List, Optional, Union
from urllib.parse import urlparse
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core.management.command_builder import inject
from renku.core.metadata.database import Persistent
from renku.core.metadata.immutable import Immutable
from renku.core.models import entities as old_entities
from renku.core.models.calamus import JsonLDSchema, Nested, fields, oa, prov, renku
from renku.core.models.cwl.annotation import Annotation, AnnotationSchema
from renku.core.models.entity import Collection, Entity, NewCollectionSchema, NewEntitySchema
from renku.core.models.provenance import qualified as old_qualified
from renku.core.models.provenance.activities import ProcessRun, WorkflowRun
from renku.core.models.provenance.agent import Agent, NewPersonSchema, NewSoftwareAgentSchema, Person, SoftwareAgent
from renku.core.models.provenance.parameter import (
    ParameterValueSchema,
    PathParameterValue,
    PathParameterValueSchema,
    VariableParameterValue,
    VariableParameterValueSchema,
)
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.models.workflow.plan import Plan, PlanSchema
from renku.core.utils.git import get_object_hash


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

    __slots__ = ("entity", "id")

    entity: Union[Collection, Entity]
    id: str

    def __init__(self, *, entity: Union[Collection, Entity], id: str):
        super().__init__(entity=entity, id=id)

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Usage identifier."""
        return f"{activity_id}/usages/{uuid4()}"


class Generation(Immutable):
    """Represent an act of generating a path."""

    __slots__ = ("entity", "id")

    entity: Union[Collection, Entity]
    id: str

    def __init__(self, *, entity: Union[Collection, Entity], id: str):
        super().__init__(entity=entity, id=id)

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Generation identifier."""
        return f"{activity_id}/generations/{uuid4()}"


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
        order: Optional[int] = None,  # TODO: Remove order and use ended_at_time for ordering
        parameters: List[Union[PathParameterValue, VariableParameterValue]] = None,
        # project=None,  # TODO: project._id gets messed up when generating and then running commands
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
        self.order: Optional[int] = order
        self.parameters: List[Union[PathParameterValue, VariableParameterValue]] = parameters or []
        # self.project: Project = project
        self.started_at_time: datetime = started_at_time
        self.usages: List[Usage] = usages or []

        # TODO: _was_informed_by = attr.ib(kw_only=True)
        # TODO: influenced = attr.ib(kw_only=True)

    @classmethod
    @inject.params(client="LocalClient")
    def from_process_run(
        cls, process_run: ProcessRun, plan: Plan, rerun_plan: Plan, client, order: Optional[int] = None
    ):
        """Create an Activity from a ProcessRun."""
        activity_id = Activity.generate_id()

        agents = [Agent.from_agent(a) for a in process_run.agents or []]
        association_agent = Agent.from_agent(process_run.association.agent)
        association = Association(agent=association_agent, id=Association.generate_id(activity_id), plan=plan)

        # NOTE: The same entity can have the same id during different times in its lifetime (e.g. different commit_sha,
        # but the same content). When it gets flattened, some fields will have multiple values which will cause an error
        # during deserialization. Make sure that no such Entity attributes exists (store those information in the
        # Generation object).

        invalidations = [_convert_invalidated_entity(e, client) for e in process_run.invalidated]
        generations = [_convert_generation(g, activity_id, client) for g in process_run.generated]
        usages = [_convert_usage(u, activity_id, client) for u in process_run.qualified_usage]

        parameters = _create_parameters(activity_id=activity_id, plan=plan, usages=usages, generations=generations)

        return cls(
            agents=agents,
            annotations=process_run.annotations,
            association=association,
            ended_at_time=process_run.ended_at_time,
            generations=generations,
            id=activity_id,
            invalidations=invalidations,
            order=order,
            parameters=parameters,
            # project=process_run._project,
            started_at_time=process_run.started_at_time,
            usages=usages,
        )

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier for an activity."""
        # TODO: make id generation idempotent
        return f"/activities/{uuid4().hex}"


def _convert_usage(usage: old_qualified.Usage, activity_id: str, client) -> Usage:
    """Convert an old qualified Usage to a new one."""
    commit_sha = _extract_commit_sha(entity_id=usage.entity._id)
    entity = _convert_used_entity(usage.entity, commit_sha, activity_id, client)
    assert entity, f"Top entity was not found for Usage: {usage._id}, {usage.entity.path}"

    return Usage(id=Usage.generate_id(activity_id), entity=entity)


def _convert_generation(generation: old_qualified.Generation, activity_id: str, client) -> Generation:
    """Convert an old Generation to a new one."""
    commit_sha = _extract_commit_sha(entity_id=generation.entity._id)
    entity = _convert_generated_entity(generation.entity, commit_sha, activity_id, client)
    assert entity, f"Root entity was not found for Generation: {generation._id}"

    return Generation(id=Generation.generate_id(activity_id), entity=entity)


def _convert_used_entity(entity: old_entities.Entity, revision: str, activity_id: str, client) -> Optional[Entity]:
    """Convert an old Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit or before the revision.
    """
    assert isinstance(entity, old_entities.Entity)

    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        return None

    if isinstance(entity, old_entities.Collection):
        members = []
        for child in entity.members:
            new_child = _convert_used_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            members.append(new_child)

        new_entity = Collection(checksum=checksum, path=entity.path, members=members)
    else:
        new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _convert_generated_entity(entity: old_entities.Entity, revision: str, activity_id: str, client) -> Optional[Entity]:
    """Convert an Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit as revision.
    """
    assert isinstance(entity, old_entities.Entity)

    try:
        entity_commit = client.find_previous_commit(paths=entity.path, revision=revision)
    except KeyError:
        return None
    if entity_commit.hexsha != revision:
        return None

    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        return None

    if isinstance(entity, old_entities.Collection):
        members = []
        for child in entity.members:
            new_child = _convert_generated_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            members.append(new_child)

        new_entity = Collection(checksum=checksum, path=entity.path, members=members)
    else:
        new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _convert_invalidated_entity(entity: old_entities.Entity, client) -> Optional[Entity]:
    """Convert an Entity to one with proper metadata."""
    assert isinstance(entity, old_entities.Entity)
    assert not isinstance(entity, old_entities.Collection), f"Collection passed as invalidated: {entity._id}"

    commit_sha = _extract_commit_sha(entity_id=entity._id)
    commit = client.find_previous_commit(revision=commit_sha, paths=entity.path)
    revision = commit.hexsha
    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        # Entity was deleted at revision; get the one before it to have object_id
        checksum = get_object_hash(repo=client.repo, revision=f"{revision}~", path=entity.path)
        if not checksum:
            print(f"Cannot find invalidated entity hash for {entity._id} at {revision}:{entity.path}")
            return

    new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _extract_commit_sha(entity_id: str) -> str:
    # NOTE: extracts commit sha from ids like /blob/a3bf8a165dd56da078b96f2ca2ff22f14a3bdd57/input
    path = urlparse(entity_id).path
    assert path.startswith("/blob/"), f"Invalid entity identifier: {entity_id}"

    commit_sha = path[len("/blob/") :].split("/", 1)[0]
    assert len(commit_sha) == 40, f"Entity does not have valid commit SHA: {entity_id}"

    return commit_sha


def _create_parameters(activity_id, plan: Plan, usages: List[Usage], generations: List[Generation]):
    parameters = []

    inputs = {i.default_value: i for i in plan.inputs}
    for usage in usages:
        input = inputs.pop(usage.entity.path, None)
        assert input is not None, f"Cannot find usage path '{usage.entity.path}' in plan {plan.id}"
        id = PathParameterValue.generate_id(activity_id)
        parameters.append(PathParameterValue(id=id, parameter=input, path=usage.entity.path))

    assert not inputs, f"Not all inputs are converted: {inputs}"

    outputs = {o.default_value: o for o in plan.outputs}
    for generation in generations:
        output = outputs.pop(generation.entity.path, None)
        assert output is not None, f"Cannot find generation path '{generation.entity.path}' in plan {plan.id}"
        id = PathParameterValue.generate_id(activity_id)
        parameters.append(PathParameterValue(id=id, parameter=output, path=generation.entity.path))

    assert not outputs, f"Not all outputs are converted: {outputs}"

    for parameter in plan.parameters:
        id = VariableParameterValue.generate_id(activity_id)
        parameters.append(VariableParameterValue(id=id, parameter=parameter, value=parameter.default_value))

    return parameters


class ActivityCollection:
    """Equivalent of a workflow file."""

    def __init__(self, activities: List[Activity] = None):
        self.activities: List[Activity] = activities or []

    @classmethod
    def from_activity(cls, activity: Union[ProcessRun, WorkflowRun], dependency_graph: DependencyGraph):
        """Convert a ProcessRun/WorkflowRun to ActivityCollection."""

        def get_process_runs(workflow_run: WorkflowRun) -> List[ProcessRun]:
            # NOTE: Use Plan subprocesses to get activities because it is guaranteed to have correct order
            sorted_ids = [s.process._id for s in workflow_run.association.plan.subprocesses]
            activities = []
            # NOTE: it's possible to have subprocesses with similar ids but it does not matter since they have the same
            # plan
            # TODO: Remove these redundant subprocesses
            for id_ in sorted_ids:
                for s in workflow_run.subprocesses.values():
                    if s.association.plan._id == id_:
                        activities.append(s)
                        break
            assert len(activities) == len(workflow_run.subprocesses)
            return activities

        process_runs = get_process_runs(activity) if isinstance(activity, WorkflowRun) else [activity]

        self = ActivityCollection()

        for process_run in process_runs:
            assert isinstance(process_run, ProcessRun)
            run = process_run.association.plan
            if run.subprocesses:
                assert len(run.subprocesses) == 1, f"Run in ProcessRun has multiple steps: {run._id}"
                run = run.subprocesses[0]

            plan = Plan.from_run(run=run)
            base_plan = dependency_graph.add(plan)

            activity = Activity.from_process_run(process_run=process_run, plan=base_plan, rerun_plan=plan)
            self.add(activity)

        return self

    def add(self, activity: Activity) -> None:
        """Add an Activity."""
        self.activities.append(activity)


class AssociationSchema(JsonLDSchema):
    """Association schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Association
        model = Association
        unknown = EXCLUDE

    agent = Nested(prov.agent, [NewSoftwareAgentSchema, NewPersonSchema])
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
    entity = Nested(prov.entity, [NewEntitySchema, NewCollectionSchema])


class GenerationSchema(JsonLDSchema):
    """Generation schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Generation
        model = Generation
        unknown = EXCLUDE

    id = fields.Id()
    # TODO: DatasetSchema, DatasetFileSchema
    entity = Nested(prov.qualifiedGeneration, [NewEntitySchema, NewCollectionSchema], reverse=True)


class ActivitySchema(JsonLDSchema):
    """Activity schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Activity
        model = Activity
        unknown = EXCLUDE

    agents = Nested(prov.wasAssociatedWith, [NewPersonSchema, NewSoftwareAgentSchema], many=True)
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    association = Nested(prov.qualifiedAssociation, AssociationSchema)
    ended_at_time = fields.DateTime(prov.endedAtTime, add_value_types=True)
    generations = Nested(prov.activity, GenerationSchema, reverse=True, many=True, missing=None)
    id = fields.Id()
    invalidations = Nested(prov.wasInvalidatedBy, NewEntitySchema, reverse=True, many=True, missing=None)
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
