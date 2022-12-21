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
"""Represent an execution of a Plan."""

from datetime import datetime
from itertools import chain
from typing import Dict, List, Optional, Union, cast
from uuid import uuid4

from werkzeug.utils import cached_property

from renku.command.command_builder import inject
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util.datetime8601 import local_now
from renku.core.util.git import get_entity_from_revision, get_git_user
from renku.domain_model.entity import Collection, Entity
from renku.domain_model.provenance.agent import Person, SoftwareAgent
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.provenance.parameter import ParameterValue
from renku.domain_model.workflow.plan import Plan
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan
from renku.infrastructure.database import Persistent
from renku.infrastructure.immutable import Immutable
from renku.infrastructure.repository import Repository
from renku.version import __version__, version_url


class Association:
    """Assign responsibility to an agent for an activity."""

    def __init__(self, *, agent: Union[Person, SoftwareAgent], id: str, plan: Union[Plan, WorkflowFileCompositePlan]):
        self.agent: Union[Person, SoftwareAgent] = agent
        self.id: str = id
        self.plan: Union[Plan, WorkflowFileCompositePlan] = plan

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a Association identifier."""
        return f"{activity_id}/association"


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


class HiddenUsage(Usage):
    """Represent a dependent path corresponding to a ``HiddenInput``."""


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

    invalidated_at: Optional[datetime] = None
    hidden_usages: List[HiddenUsage] = list()

    def __init__(
        self,
        *,
        agents: List[Union[Person, SoftwareAgent]],
        annotations: Optional[List[Annotation]] = None,
        association: Association,
        ended_at_time: datetime,
        generations: Optional[List[Generation]] = None,
        hidden_usages: Optional[List[HiddenUsage]] = None,
        id: str,
        invalidated_at: Optional[datetime] = None,
        invalidations: Optional[List[Entity]] = None,
        parameters: Optional[List[ParameterValue]] = None,
        project_id: Optional[str] = None,
        started_at_time: datetime,
        usages: Optional[List[Usage]] = None,
    ):
        self.agents: List[Union[Person, SoftwareAgent]] = agents
        self.annotations: List[Annotation] = annotations or []
        self.association: Association = association
        self.ended_at_time: datetime = ended_at_time
        self.generations: List[Generation] = generations or []
        self.hidden_usages: List[HiddenUsage] = hidden_usages or []
        self.id: str = id
        self.invalidated_at: Optional[datetime] = invalidated_at
        self.invalidations: List[Entity] = invalidations or []
        self.parameters: List[ParameterValue] = parameters or []
        self.project_id: Optional[str] = project_id
        self.started_at_time: datetime = started_at_time
        self.usages: List[Usage] = usages or []

        if not self.id.startswith("/activities/"):
            self.id = f"/activities/{self.id}"

        # TODO: _was_informed_by = attr.ib(kw_only=True)
        # TODO: influenced = attr.ib(kw_only=True)

    @classmethod
    @inject.autoparams("project_gateway")
    def from_plan(
        cls,
        plan: Plan,
        repository: "Repository",
        project_gateway: IProjectGateway,
        started_at_time: datetime,
        ended_at_time: datetime,
        annotations: List[Annotation] = None,
        id: Optional[str] = None,
    ):
        """Convert a ``Plan`` to a ``Activity``."""
        from renku.core.plugin.pluginmanager import get_plugin_manager

        usages: Dict[str, Usage] = {}
        hidden_usages: Dict[str, HiddenUsage] = {}
        generations = {}
        parameter_values = []

        activity_id = id or cls.generate_id()

        def process_input(input, already_processed, cls, add_parameter_value):
            input_path = input.actual_value

            if add_parameter_value:
                parameter_values.append(
                    ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=input.id, value=input_path)
                )

            if input_path in already_processed:
                return

            entity = get_entity_from_revision(repository=repository, path=input_path, bypass_cache=True)

            dependency = cls(entity=entity, id=cls.generate_id(activity_id))

            already_processed[input_path] = dependency

        for input in plan.inputs:
            process_input(input=input, already_processed=usages, cls=Usage, add_parameter_value=True)

        for input in plan.hidden_inputs:
            process_input(input=input, already_processed=hidden_usages, cls=HiddenUsage, add_parameter_value=False)

        for output in plan.outputs:
            output_path = output.actual_value

            parameter_values.append(
                ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=output.id, value=output_path)
            )

            if output_path in generations:
                continue

            entity = get_entity_from_revision(repository=repository, path=output_path, bypass_cache=True)

            generation = Generation(entity=entity, id=Generation.generate_id(activity_id))

            generations[output_path] = generation

        for parameter in plan.parameters:
            value = parameter.actual_value

            parameter_values.append(
                ParameterValue(id=ParameterValue.generate_id(activity_id), parameter_id=parameter.id, value=value)
            )

        agent = SoftwareAgent(id=version_url, name=f"renku {__version__}")
        person = cast(Person, get_git_user(repository))
        association = Association(agent=agent, id=Association.generate_id(activity_id), plan=plan)

        activity = cls(
            id=activity_id,
            association=association,
            agents=[agent, person],
            usages=list(usages.values()),
            hidden_usages=list(hidden_usages.values()),
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

    def __repr__(self):
        return f"<Activity '{self.id}': {self.association.plan.name} @ {self.ended_at_time}>"

    @cached_property
    def plan_with_values(self) -> Plan:
        """Get a copy of the associated plan with values from ParameterValues applied."""
        plan = self.association.plan.copy()

        for parameter in self.parameters:
            parameter.apply_value_to_parameter(plan)

        return plan

    @property
    def deleted(self) -> bool:
        """Return if the activity was deleted."""
        return self.invalidated_at is not None

    @staticmethod
    def generate_id(uuid: Optional[str] = None) -> str:
        """Generate an identifier for an activity."""
        if uuid is None:
            uuid = uuid4().hex
        return f"/activities/{uuid}"

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

    def delete(self, when: datetime = local_now()):
        """Mark the activity as deleted."""
        self.unfreeze()
        self.invalidated_at = when
        self.freeze()


class ActivityCollection(Persistent):
    """Represent a list of activities."""

    def __init__(self, *, activities: List[Activity], id: str = None):
        self.activities: List[Activity] = activities or []
        self.id: str = id or self.generate_id()

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier for an activity collection."""
        return f"/activity-collection/{uuid4().hex}"


class WorkflowFileActivityCollection(ActivityCollection):
    """Represent activities of a workflow file execution."""

    def __init__(
        self,
        *,
        activities: List[Activity],
        agents: List[Union[Person, SoftwareAgent]],
        association: Association,
        ended_at_time: datetime,
        id: str = None,
        invalidated_at: Optional[datetime] = None,
        project_id: Optional[str] = None,
        started_at_time: datetime,
    ):
        super().__init__(activities=activities, id=id)

        self.agents: List[Union[Person, SoftwareAgent]] = agents
        self.association: Association = association
        self.ended_at_time: datetime = ended_at_time
        self.invalidated_at: Optional[datetime] = invalidated_at
        self.project_id: Optional[str] = project_id
        self.started_at_time: datetime = started_at_time

    @classmethod
    @inject.autoparams("project_gateway")
    def from_activities(
        cls, plan: WorkflowFileCompositePlan, project_gateway: IProjectGateway, activities: List[Activity]
    ):
        """Create an instance from a list of ``Activity``."""
        id = cls.generate_id()
        association = Association(agent=activities[0].association.agent, id=Association.generate_id(id), plan=plan)

        return cls(
            activities=activities,
            agents=activities[0].agents.copy(),
            association=association,
            ended_at_time=max(a.ended_at_time for a in activities),
            id=id,
            invalidated_at=None,
            project_id=project_gateway.get_project().id,
            started_at_time=min(a.started_at_time for a in activities),
        )

    @staticmethod
    def generate_id() -> str:
        """Generate an identifier."""
        return f"/workflow-file-activity-collection/{uuid4().hex}"

    def __repr__(self):
        return f"<WorkflowFileActivityCollection '{self.id}': {self.association.plan.name} @ {self.ended_at_time}>"
