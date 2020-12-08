# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Represent a run."""

import pathlib
import uuid
from typing import List, Union
from urllib.parse import quote, urljoin, urlparse

from git import GitCommandError
from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, fields, oa, prov, renku
from renku.core.models.cwl.annotation import AnnotationSchema
from renku.core.models.entities import Collection, Entity, EntitySchema
from renku.core.models.provenance.activities import Activity as ActivityRun
from renku.core.models.provenance.activities import ProcessRun, WorkflowRun
from renku.core.models.provenance.agents import PersonSchema, SoftwareAgentSchema
from renku.core.models.provenance.qualified import (
    Association,
    AssociationSchema,
    Generation,
    GenerationSchema,
    Usage,
    UsageSchema,
)
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.models.workflow.plan import Plan
from renku.core.utils.urls import get_host


class Activity:
    """Represent an activity in the repository."""

    def __init__(
        self,
        id_,
        agents=None,
        association=None,
        ended_at_time=None,
        generated=None,
        invalidated=None,
        order=None,
        # project=None,  # TODO: project._id gets messed up when generating and then running commands
        qualified_usage=None,
        started_at_time=None,
        annotations=None,
    ):
        """Initialize."""
        self.agents = agents
        self.association = association
        self.ended_at_time = ended_at_time
        self.generated = generated
        self.id_ = id_
        self.invalidated = invalidated
        self.order = order
        # self.project = project
        self.qualified_usage = qualified_usage
        self.started_at_time = started_at_time
        self.annotations = annotations

        # TODO: _was_informed_by = attr.ib(kw_only=True,)
        # TODO: influenced = attr.ib(kw_only=True)

    @classmethod
    def from_process_run(cls, process_run: ProcessRun, plan: Plan, client, order=None):
        """Create an Activity from a ProcessRun."""
        activity_id = Activity.generate_id(client)

        association = Association(agent=process_run.association.agent, id=f"{activity_id}/association", plan=plan)

        # NOTE: The same entity can have the same id during different times in its lifetime (e.g. different commit_sha,
        # but the same content). When it gets flattened, some fields will have multiple values which will cause an error
        # during deserialization. Make sure that no such Entity attributes exists (store those information in the
        # Generation object).

        qualified_usage = _convert_qualified_usage(process_run.qualified_usage, activity_id, client)

        generated = _convert_generated(process_run.generated, activity_id, client)

        invalidated = [_convert_invalidated_entity(e, activity_id, client) for e in process_run.invalidated]

        return cls(
            agents=process_run.agents,
            association=association,
            ended_at_time=process_run.ended_at_time,
            generated=generated,
            id_=activity_id,
            invalidated=invalidated,
            order=order,
            # project=process_run._project,
            qualified_usage=qualified_usage,
            started_at_time=process_run.started_at_time,
            annotations=process_run.annotations,
        )

    @staticmethod
    def generate_id(client):
        """Generate an identifier for an activity."""
        # TODO: make id generation idempotent
        host = get_host(client)
        return urljoin(f"https://{host}", pathlib.posixpath.join("activities", str(uuid.uuid4())))

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, list):
            raise ValueError(data)

        return ActivitySchema(flattened=True).load(data)

    def to_jsonld(self):
        """Create JSON-LD."""
        return ActivitySchema(flattened=True).dump(self)


def _convert_qualified_usage(qualified_usage: List[Usage], activity_id, client) -> List[Usage]:
    """Convert a qualified Usages."""
    usages = []

    for usage in qualified_usage:
        commit_sha = _extract_commit_sha(entity_id=usage.entity._id)
        entity = _convert_usage_entity(usage.entity, commit_sha, activity_id, client)
        assert entity, f"Top entity was not found for Usage: {usage._id}, {usage.entity.path}"

        id_ = pathlib.posixpath.join("usage", str(uuid.uuid4()), entity.checksum, quote(entity.path))
        usage_id = f"{activity_id}/{id_}"

        new_usage = Usage(id=usage_id, entity=entity, role=usage.role)
        usages.append(new_usage)

    return usages


def _convert_generated(generated: List[Generation], activity_id, client) -> List[Generation]:
    """Convert Generations."""
    generations = []

    for generation in generated:
        commit_sha = _extract_commit_sha(entity_id=generation.entity._id)
        entity = _convert_generation_entity(generation.entity, commit_sha, activity_id, client)
        assert entity, f"Root entity was not found for Generation: {generation._id}"

        quoted_path = quote(entity.path)
        id_ = pathlib.posixpath.join("generation", str(uuid.uuid4()), entity.checksum, quoted_path)
        generation_id = f"{activity_id}/{id_}"

        new_generation = Generation(id=generation_id, entity=entity, role=generation.role)
        generations.append(new_generation)

    return generations


def _convert_usage_entity(entity: Entity, revision, activity_id, client) -> Union[Entity, None]:
    """Convert an Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit or before the revision.
    """
    assert isinstance(entity, Entity)

    checksum = _get_object_hash(revision=revision, path=entity.path, client=client)
    if not checksum:
        return None

    id_ = _generate_entity_id(entity_checksum=checksum, path=entity.path, activity_id=activity_id)

    if isinstance(entity, Collection):
        new_entity = Collection(id=id_, checksum=checksum, path=entity.path)
        for child in entity.members:
            new_child = _convert_usage_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            new_entity.members.append(new_child)
    else:
        new_entity = Entity(id=id_, checksum=checksum, path=entity.path)

    assert type(new_entity) is type(entity)

    return new_entity


def _convert_generation_entity(entity: Entity, revision, activity_id, client) -> Union[Entity, None]:
    """Convert an Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit as revision.
    """
    assert isinstance(entity, Entity)

    try:
        entity_commit = client.find_previous_commit(paths=entity.path, revision=revision)
    except KeyError:
        return None
    if entity_commit.hexsha != revision:
        return None

    checksum = _get_object_hash(revision=revision, path=entity.path, client=client)
    if not checksum:
        return None

    id_ = _generate_entity_id(entity_checksum=checksum, path=entity.path, activity_id=activity_id)

    if isinstance(entity, Collection):
        new_entity = Collection(id=id_, checksum=checksum, path=entity.path)
        for child in entity.members:
            new_child = _convert_generation_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            new_entity.members.append(new_child)
    else:
        new_entity = Entity(id=id_, checksum=checksum, path=entity.path)

    assert type(new_entity) is type(entity)

    return new_entity


def _convert_invalidated_entity(entity: Entity, activity_id, client) -> Union[Entity, None]:
    """Convert an Entity to one with proper metadata."""
    assert isinstance(entity, Entity)
    assert not isinstance(entity, Collection), f"Collection passed as invalidated: {entity._id}"

    commit_sha = _extract_commit_sha(entity_id=entity._id)
    commit = client.find_previous_commit(revision=commit_sha, paths=entity.path)
    commit_sha = commit.hexsha
    checksum = _get_object_hash(revision=commit_sha, path=entity.path, client=client)
    if not checksum:
        # Entity was deleted at commit_sha; get the one before it to have object_id
        checksum = _get_object_hash(revision=f"{commit_sha}~", path=entity.path, client=client)
        if not checksum:
            print(f"Cannot find invalidated entity hash for {entity._id} at {commit_sha}:{entity.path}")
            return None

    id_ = _generate_entity_id(entity_checksum=checksum, path=entity.path, activity_id=activity_id)
    new_entity = Entity(id=id_, checksum=checksum, path=entity.path)
    assert type(new_entity) is type(entity)

    return new_entity


def _generate_entity_id(entity_checksum, path, activity_id):
    quoted_path = quote(path)
    path = pathlib.posixpath.join("blob", entity_checksum, quoted_path)

    return urlparse(activity_id)._replace(path=path).geturl()


def _get_object_hash(revision, path, client):
    try:
        return client.repo.git.rev_parse(f"{revision}:{str(path)}")
    except GitCommandError:
        # NOTE: Either the file was not there when the command ran but was there when workflows were migrated (this
        # can happen only for Usage) or the project is broken. We assume the former here.
        return None


def _extract_commit_sha(entity_id: str):
    # NOTE: extracts commit sha from ids like /blob/a3bf8a165dd56da078b96f2ca2ff22f14a3bdd57/input
    path = urlparse(entity_id).path
    assert path.startswith("/blob/"), f"Invalid entity identifier: {entity_id}"

    commit_sha = path[len("/blob/") :].split("/", 1)[0]
    assert len(commit_sha) == 40, f"Entity does not have valid commit SHA: {entity_id}"

    return commit_sha


class ActivityCollection:
    """Equivalent of a workflow file."""

    def __init__(self, activities=None):
        """Initialize."""
        self._activities = activities or []
        self._path = None

    @classmethod
    def from_activity_run(cls, activity_run: ActivityRun, dependency_graph: DependencyGraph, client):
        """Convert a ProcessRun/WorkflowRun to ActivityCollection."""

        def get_process_runs(activity_run: ActivityRun) -> list:
            assert isinstance(activity_run, WorkflowRun)
            # Use Plan subprocesses to get activities because it is guaranteed to have correct order
            sorted_ids = [s.process._id for s in activity_run.association.plan.subprocesses]
            activities = []
            # NOTE: it's possible to have subprocesses with similar ids but it does not matter since they have the same
            # plan
            # TODO: Remove these redundant subprocesses
            for id_ in sorted_ids:
                for s in activity_run.subprocesses.values():
                    if s.association.plan._id == id_:
                        activities.append(s)
                        break
            assert len(activities) == len(activity_run.subprocesses)
            return activities

        process_runs = get_process_runs(activity_run) if isinstance(activity_run, WorkflowRun) else [activity_run]

        self = ActivityCollection()

        for process_run in process_runs:
            assert isinstance(process_run, ProcessRun)
            run = process_run.association.plan
            if run.subprocesses:
                assert len(run.subprocesses) == 1, f"Run in ProcessRun has multiple steps: {run._id}"
                run = run.subprocesses[0]

            plan = Plan.from_run(run=run, name=None, client=client)
            plan = dependency_graph.add(plan)

            activity = Activity.from_process_run(process_run=process_run, plan=plan, client=client)
            self.add(activity)

        return self

    def add(self, activity):
        """Add an Activity."""
        self._activities.append(activity)


class ActivitySchema(JsonLDSchema):
    """Activity schema."""

    class Meta:
        """Meta class."""

        rdf_type = prov.Activity
        model = Activity
        unknown = EXCLUDE

    agents = Nested(prov.wasAssociatedWith, [PersonSchema, SoftwareAgentSchema], many=True)
    association = Nested(prov.qualifiedAssociation, AssociationSchema)
    ended_at_time = fields.DateTime(prov.endedAtTime, add_value_types=True)
    generated = Nested(prov.activity, GenerationSchema, reverse=True, many=True, missing=None)
    id_ = fields.Id()
    invalidated = Nested(prov.wasInvalidatedBy, EntitySchema, reverse=True, many=True, missing=None)
    order = fields.Integer(renku.order)
    path = fields.String(prov.atLocation)
    qualified_usage = Nested(prov.qualifiedUsage, UsageSchema, many=True)
    started_at_time = fields.DateTime(prov.startedAtTime, add_value_types=True)
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
