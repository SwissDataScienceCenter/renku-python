# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
r"""Renku API Activity.

``Activity`` represents executed workflows in a Renku project. You can get a
list of all activities in a project by calling its ``list`` method:

.. code-block:: python

    from renku.api import Activity

    activities = Activity.list()

The ``Activity`` class provides a static ``filter`` method that returns a
subset of activities. It can filter activities based on their input, outputs,
parameter names, and parameter values. You can pass a literal value, a list of
values, or a function predicate for each of these fields to filter activities:

.. code-block:: python

    from numbers import Number
    from renku.api import Activity

    # Return activities that use ``path/to/an/input``
    Activity.filter(inputs="path/to/an/input")

    # Return activities that use ``input-1`` or ``input-2`` AND generate
    # output files that their name starts with ``data-``
    Activity.filter(inputs=["input-1", "input-2"], outputs=lambda path: path.startswith("data-"))

    # Return activities that use values between ``0.5`` and ``1.5`` for the
    # parameter ``lr``
    Activity.filter(parameters="lr", values=lambda value: 0.5 <= value <= 1.5 if isinstance(value, Number) else False)

"""

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, NamedTuple, Optional, Set, Union

from renku.core import errors
from renku.core.util.os import matches
from renku.domain_model.provenance import activity as core_activity
from renku.domain_model.provenance import parameter as core_parameter
from renku.domain_model.provenance.agent import Person
from renku.domain_model.workflow import plan as core_plan
from renku.ui.api.models.parameter import Input, Output, Parameter, convert_parameter
from renku.ui.api.models.plan import Plan
from renku.ui.api.util import get_activity_gateway, get_plan_gateway

if TYPE_CHECKING:
    from renku.infrastructure.gateway.activity_gateway import ActivityGateway


class Activity:
    """API Activity."""

    def __init__(
        self,
        activity: core_activity.Activity,
        annotations: List,
        generated_outputs: List["GeneratedOutput"],
        ended_at: datetime,
        executed_command: str,
        id: str,
        values: List["FieldValue"],
        plan: Optional[Plan],
        started_at: datetime,
        used_inputs: List["UsedInput"],
        user: str,
    ):
        self._activity: core_activity.Activity = activity
        self._values: Optional[List["FieldValue"]] = values or None
        self._plan: Optional[Plan] = plan
        self.annotations: List = annotations or []
        self.generated_outputs: List["GeneratedOutput"] = generated_outputs or []
        self.ended_at: datetime = ended_at
        self.executed_command: str = executed_command
        self.id: str = id
        self.started_at: datetime = started_at
        self.used_inputs: List["UsedInput"] = used_inputs or []
        self.user: str = user

    @classmethod
    def from_activity(cls, activity: core_activity.Activity) -> "Activity":
        """Create an instance from a core Activity model.

        Args:
            activity(core_activity.Activity): The core activity.

        Returns:
            Activity: An API Activity model.
        """

        def get_user() -> str:
            user = next((a for a in activity.agents if isinstance(a, Person)), None)
            return user.full_identity if user else ""

        return cls(
            activity=activity,
            annotations=activity.annotations,
            generated_outputs=[GeneratedOutput.from_generation(g) for g in activity.generations],
            ended_at=activity.ended_at_time,
            executed_command=" ".join(activity.association.plan.to_argv(with_streams=True)),
            id=activity.id,
            values=[],
            plan=None,
            started_at=activity.started_at_time,
            used_inputs=[UsedInput.from_usage(u) for u in activity.usages],
            user=get_user(),
        )

    @staticmethod
    def list() -> List["Activity"]:
        """List all activities in a project.

        Returns:
            A list of all activities in the supplied project.
        """
        return get_activities()

    @staticmethod
    def filter(
        *,
        inputs: Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]] = None,
        outputs: Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]] = None,
        parameters: Union[str, Iterable[str], Callable[[str], bool]] = None,
        values: Union[Any, Iterable[Any], Callable[[Any], bool]] = None,
    ) -> List["Activity"]:
        """Return a filtered list of activities in a project.

        Args:
            inputs(Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]): A literal value, list of
                values, or a predicate to filter activities that use an input.
            outputs(Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]): A literal value, list of
                values, or a predicate to filter activities that generate an output.
            parameters(Union[str, Iterable[str], Callable[[str], bool]]): A string, list of strings, or a predicate to
                filter activities based on parameter names that they use.
            values(Union[Any, Iterable[Any], Callable[[Any], bool]]): A literal value, list of values, or a predicate
                to filter activities based on their executed parameter values.

        Returns:
            A list of all activities that match the criteria.
        """
        activity_gateway: Optional["ActivityGateway"] = get_activity_gateway()
        if activity_gateway is None:
            return []

        activities = set(activity_gateway.get_all_activities())

        if inputs is not None:
            filtered = Activity._filter_by_path(
                "get_all_usage_paths", "get_activities_by_usage", path=inputs, activity_gateway=activity_gateway
            )
            activities &= filtered

        if outputs is not None:
            filtered = Activity._filter_by_path(
                "get_all_generation_paths",
                "get_activities_by_generation",
                path=outputs,
                activity_gateway=activity_gateway,
            )
            activities &= filtered

        if parameters is not None or values is not None:
            filtered = Activity._filter_by_parameter(parameters, values, activity_gateway=activity_gateway)
            activities &= filtered

        return [Activity.from_activity(a) for a in activities]

    @staticmethod
    def filter_by_input(path: Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]) -> List["Activity"]:
        """Return a filtered list of activities based on their inputs.

        Args:
            path(Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]): A literal value, list of
                values, or a predicate to filter activities that use an input.

        Returns:
            A list of all activities that match the criteria.
        """
        activities = Activity._filter_by_path("get_all_usage_paths", "get_activities_by_usage", path=path)

        return [Activity.from_activity(a) for a in activities]

    @staticmethod
    def filter_by_output(path: Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]) -> List["Activity"]:
        """Return a filtered list of activities based on their outputs.

        Args:
            path(Union[Path, str, Iterable[Union[Path, str]], Callable[[str], bool]]): A literal value, list of
                values, or a predicate to filter activities that use an input.

        Returns:
            A list of all activities that match the criteria.
        """
        activities = Activity._filter_by_path("get_all_generation_paths", "get_activities_by_generation", path=path)

        return [Activity.from_activity(a) for a in activities]

    @staticmethod
    def _filter_by_path(
        get_all_method: str, get_by_method: str, path, activity_gateway: Optional["ActivityGateway"] = None
    ) -> Set[core_activity.Activity]:
        if activity_gateway is None:
            activity_gateway = get_activity_gateway()
            if activity_gateway is None:
                return set()

        activity_paths = getattr(activity_gateway, get_all_method)()

        if isinstance(path, (Path, str)):
            filtered = [a for a in activity_paths if matches(path=a, pattern=str(path))]
        elif isinstance(path, Iterable):
            filtered = [a for a in activity_paths if any(p for p in path if matches(path=a, pattern=str(p)))]
        elif isinstance(path, Callable):  # type: ignore
            filtered = [a for a in activity_paths if path(a)]
        else:
            raise errors.ParameterError(f"Invalid input/output type: {type(path)}")

        get_activities_by_method = getattr(activity_gateway, get_by_method)
        activities_list = [get_activities_by_method(u) for u in filtered]
        return {a for activities in activities_list for a in activities}

    @staticmethod
    def filter_by_parameter(
        name: Union[str, Iterable[str], Callable[[str], bool]] = None,
        value: Union[Any, Iterable[Any], Callable[[Any], bool]] = None,
    ) -> List["Activity"]:
        """Return a filtered list of activities based on parameters and their values.

        Args:
            name(Union[str, Iterable[str], Callable[[str], bool]]): A string, list of strings, or a predicate to
                filter activities based on parameter names that they use.
            value(Union[Any, Iterable[Any], Callable[[Any], bool]]): A literal value, list of values, or a predicate
                to filter activities based on their executed parameter values.

        Returns:
            A list of all activities that match the criteria.
        """
        filtered_activities = Activity._filter_by_parameter(name=name, value=value)

        return [Activity.from_activity(a) for a in filtered_activities]

    @staticmethod
    def _filter_by_parameter(
        name: Union[str, Iterable[str], Callable[[str], bool]] = None,
        value: Union[Any, Iterable[Any], Callable[[Any], bool]] = None,
        activity_gateway: Optional["ActivityGateway"] = None,
    ) -> Set[core_activity.Activity]:
        if activity_gateway is None:
            activity_gateway = get_activity_gateway()
            if activity_gateway is None:
                return set()

        def name_matches(parameter, plan) -> bool:
            if name is None:
                return True

            parameter_name = plan.get_field_by_id(parameter.parameter_id).name

            if isinstance(name, str):
                return name == parameter_name
            elif isinstance(name, Iterable):
                return parameter_name in name
            elif isinstance(name, Callable):  # type: ignore
                return name(parameter_name)

            raise errors.ParameterError(f"Invalid name type: {type(name)}")

        def value_matches(parameter) -> bool:
            if value is None:
                return True
            elif isinstance(value, Iterable):
                return parameter.value in value
            elif isinstance(value, Callable):  # type: ignore
                return value(parameter.value)
            else:
                return value == parameter.value

        activities = activity_gateway.get_all_activities()
        filtered_activities = set()

        for activity in activities:
            for parameter in activity.parameters:
                if name_matches(parameter, plan=activity.association.plan) and value_matches(parameter):
                    filtered_activities.add(activity)
                    break

        return filtered_activities

    def __repr__(self):
        return f"<Activity '{self._activity.association.plan.name}' @ {self.ended_at.isoformat()[:-6]}>"

    @property
    def preceding_activities(self) -> List["Activity"]:
        """Return a list of upstream activities."""
        activity_gateway: Optional["ActivityGateway"] = get_activity_gateway()
        if activity_gateway is None:
            return []

        return [Activity.from_activity(a) for a in activity_gateway.get_upstream_activities(self._activity)]

    @property
    def following_activities(self) -> List["Activity"]:
        """Return a list of downstream activities."""
        activity_gateway: Optional["ActivityGateway"] = get_activity_gateway()
        if activity_gateway is None:
            return []

        return [Activity.from_activity(a) for a in activity_gateway.get_downstream_activities(self._activity)]

    @property
    def values(self) -> List["FieldValue"]:
        """Return list of values for inputs/outputs/parameters that were used in this execution."""
        if self._values is None:
            self._values = [
                FieldValue.from_parameter_value(p, self._activity.association.plan) for p in self._activity.parameters
            ]

        return self._values

    @property
    def plan(self) -> Plan:
        """Activity's base plan."""
        if self._plan is None:
            self._plan = Plan.from_plan(self._activity.association.plan)

        return self._plan


class UsedInput(NamedTuple):
    """Represent an input used in an execution."""

    path: str
    checksum: str

    @classmethod
    def from_usage(cls, usage: core_activity.Usage):
        """Create an instance from core Usage."""
        return cls(path=usage.entity.path, checksum=usage.entity.checksum)

    def __repr__(self):
        return f"<Input '{self.path}'>"


class GeneratedOutput(NamedTuple):
    """Represent an output generated in an execution."""

    path: str
    checksum: str

    @classmethod
    def from_generation(cls, generation: core_activity.Generation):
        """Create an instance from core Generation."""
        return cls(path=generation.entity.path, checksum=generation.entity.checksum)

    def __repr__(self):
        return f"<Output '{self.path}'>"


class FieldValue(NamedTuple):
    """Represent a parameter used in an execution."""

    field: Union[Input, Output, Parameter]
    value: Any

    @classmethod
    def from_parameter_value(cls, parameter: core_parameter.ParameterValue, plan: core_plan.Plan):
        """Create an instance from core ParameterValue."""
        return cls(field=convert_parameter(plan.get_field_by_id(parameter.parameter_id)), value=parameter.value)

    def __repr__(self):
        if isinstance(self.field, Input):
            return f"<Input '{self.field.name}'={self.value}>"
        elif isinstance(self.field, Output):
            return f"<Output '{self.field.name}'={self.value}>"

        return f"<Parameter '{self.field.name}'={self.value}>"


def get_activities(plan_id: str = None) -> List[Activity]:
    """Return list of activities that use a plan or one of its predecessor plans."""
    activity_gateway = get_activity_gateway()
    plan_gateway = get_plan_gateway()
    if not activity_gateway or not plan_gateway:
        return []

    activities = activity_gateway.get_all_activities()

    if plan_id:
        ids = {plan_id}
        plan = plan_gateway.get_by_id(plan_id)

        while plan is not None and plan.derived_from is not None:
            ids.add(plan.derived_from)
            plan = plan_gateway.get_by_id(plan.derived_from)

        activities = [a for a in activities if a.association.plan.id in ids]

    return [Activity.from_activity(a) for a in activities]
