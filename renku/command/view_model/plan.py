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
"""Plan view model."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import List, Optional, Union, cast

from renku.command.view_model.agent import PersonViewModel
from renku.command.view_model.composite_plan import CompositePlanViewModel
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.parameter import CommandInput, CommandOutput, CommandParameter
from renku.domain_model.workflow.plan import AbstractPlan, Plan


class CommandInputViewModel:
    """View model for ``CommandInput``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        description: Optional[str] = None,
        position: Optional[str] = None,
        prefix: Optional[str] = None,
        mapped_to: Optional[str] = None,
        plan_id: Optional[str] = None,
        encoding_format: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.default_value = default_value
        self.position = position
        self.prefix = prefix
        self.mapped_to = mapped_to
        self.plan_id = plan_id
        self.type = "Input"
        self.exists = (project_context.path / self.default_value).exists()
        self.encoding_format = encoding_format

    @classmethod
    def from_input(cls, input: CommandInput, plan_id: Optional[str] = None):
        """Create view model from ``CommandInput``.

        Args:
            input(CommandInput): Command input to convert.

        Returns:
            View model for command input.
        """
        return cls(
            name=input.name,
            description=input.description,
            default_value=str(input.default_value),
            position=str(input.position) if input.position is not None else None,
            prefix=input.prefix,
            mapped_to=input.mapped_to.stream_type if input.mapped_to else None,
            plan_id=plan_id,
            encoding_format=",".join(input.encoding_format) if input.encoding_format else None,
        )


class CommandOutputViewModel:
    """View model for ``CommandOutput``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        create_folder: bool,
        description: Optional[str] = None,
        position: Optional[str] = None,
        prefix: Optional[str] = None,
        mapped_to: Optional[str] = None,
        plan_id: Optional[str] = None,
        encoding_format: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.create_folder = create_folder
        self.default_value = default_value
        self.position = position
        self.prefix = prefix
        self.mapped_to = mapped_to
        self.plan_id = plan_id
        self.type = "Output"
        self.exists = (project_context.path / self.default_value).exists()
        self.encoding_format = encoding_format

    @classmethod
    def from_output(cls, output: CommandOutput, plan_id: Optional[str] = None):
        """Create view model from ``CommandOutput``.

        Args:
            output(CommandOutput): Command output to convert.

        Returns:
            View model for command output.
        """
        return cls(
            name=output.name,
            description=output.description,
            create_folder=output.create_folder,
            default_value=str(output.default_value),
            position=str(output.position) if output.position is not None else None,
            prefix=output.prefix,
            mapped_to=output.mapped_to.stream_type if output.mapped_to else None,
            plan_id=plan_id,
            encoding_format=",".join(output.encoding_format) if output.encoding_format else None,
        )


class CommandParameterViewModel:
    """View model for ``CommandParameter``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        description: Optional[str] = None,
        position: Optional[str] = None,
        prefix: Optional[str] = None,
        plan_id: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.default_value = default_value
        self.position = position
        self.prefix = prefix
        self.plan_id = plan_id
        self.type = "Parameter"

    @classmethod
    def from_parameter(cls, parameter: CommandParameter, plan_id: Optional[str] = None):
        """Create view model from ``CommandParameter``.

        Args:
            parameter(CommandParameter): Command parameter to convert.

        Returns:
            View model for command parameter.
        """
        return cls(
            name=parameter.name,
            description=parameter.description,
            default_value=str(parameter.default_value),
            position=str(parameter.position) if parameter.position is not None else None,
            prefix=parameter.prefix,
            plan_id=plan_id,
        )


class PlanViewModel:
    """A view model for a ``Plan``."""

    def __init__(
        self,
        id: str,
        name: str,
        created: datetime,
        command: str,
        full_command: str,
        inputs: List[CommandInputViewModel],
        outputs: List[CommandOutputViewModel],
        parameters: List[CommandParameterViewModel],
        keywords: List[str],
        description: Optional[str] = None,
        success_codes: Optional[str] = None,
        annotations: Optional[str] = None,
        creators: Optional[List[PersonViewModel]] = None,
        touches_existing_files: Optional[bool] = None,
        last_executed: Optional[datetime] = None,
        number_of_executions: Optional[int] = None,
        latest: Optional[str] = None,
        duration: Optional[timedelta] = None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.created = created
        self.command = command
        self.full_command = full_command
        self.success_codes = success_codes
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = parameters
        self.annotations = annotations
        self.creators = creators
        self.keywords = keywords
        self.touches_existing_files = touches_existing_files
        self.last_executed = last_executed
        self.number_of_executions = number_of_executions
        self.latest = latest
        self.type = "Plan"

        if duration is not None:
            self.duration = duration.seconds

    @classmethod
    def from_plan(cls, plan: Plan):
        """Create view model from ``Plan``.

        Args:
            plan(Plan): The plan to convert.

        Returns:
            View model for plan.
        """
        return cls(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            created=plan.date_created,
            full_command=" ".join(plan.to_argv(with_streams=True)),
            command=plan.command,
            success_codes=", ".join(str(c) for c in plan.success_codes),
            inputs=[CommandInputViewModel.from_input(input, plan.id) for input in plan.inputs],
            outputs=[CommandOutputViewModel.from_output(output, plan.id) for output in plan.outputs],
            parameters=[CommandParameterViewModel.from_parameter(param, plan.id) for param in plan.parameters],
            annotations=json.dumps([{"id": a.id, "body": a.body, "source": a.source} for a in plan.annotations])
            if plan.annotations
            else None,
            creators=[PersonViewModel.from_person(p) for p in plan.creators] if plan.creators else None,
            keywords=plan.keywords,
            touches_existing_files=getattr(plan, "touches_existing_files", False),
            latest=getattr(plan, "latest", None),
            last_executed=getattr(plan, "last_executed", None),
            number_of_executions=getattr(plan, "number_of_executions", None),
            duration=getattr(plan, "duration", None),
        )


def plan_view(workflow: AbstractPlan, latest: bool = False) -> Union[CompositePlanViewModel, PlanViewModel]:
    """Convert a ``CompositePlan`` or ``Plan`` to a ``ViewModel``.

    Args:
        workflow(AbstractPlan): Plan to convert.
        latest(bool): Whether to get latest plan data.

    Returns:
        View model for converted Plan.
    """
    if isinstance(workflow, CompositePlan):
        return CompositePlanViewModel.from_composite_plan(workflow, latest=latest)
    return PlanViewModel.from_plan(cast(Plan, workflow))
