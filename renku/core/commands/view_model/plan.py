# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

from typing import List, Optional, Union

from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.parameter import CommandInput, CommandOutput, CommandParameter
from renku.core.models.workflow.plan import AbstractPlan, Plan

from .composite_plan import CompositePlanViewModel


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
    ):
        self.name = name
        self.description = description
        self.default_value = default_value
        self.position = position
        self.prefix = prefix
        self.mapped_to = mapped_to

    @classmethod
    def from_input(cls, input: CommandInput):
        """Create view model from ``CommandInput``."""
        return cls(
            name=input.name,
            description=input.description,
            default_value=str(input.default_value),
            position=str(input.position),
            prefix=input.prefix,
            mapped_to=input.mapped_to.stream_type if input.mapped_to else None,
        )


class CommandOutputViewModel:
    """View model for ``CommandOutput``."""

    def __init__(
        self,
        name: str,
        default_value: str,
        description: Optional[str] = None,
        position: Optional[str] = None,
        prefix: Optional[str] = None,
        mapped_to: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.default_value = default_value
        self.position = position
        self.prefix = prefix
        self.mapped_to = mapped_to

    @classmethod
    def from_output(cls, output: CommandOutput):
        """Create view model from ``CommandOutput``."""
        return cls(
            name=output.name,
            description=output.description,
            default_value=str(output.default_value),
            position=str(output.position),
            prefix=output.prefix,
            mapped_to=output.mapped_to.stream_type if output.mapped_to else None,
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
    ):
        self.name = name
        self.description = description
        self.default_value = default_value
        self.position = position
        self.prefix = prefix

    @classmethod
    def from_parameter(cls, parameter: CommandParameter):
        """Create view model from ``CommandParameter``."""
        return cls(
            name=parameter.name,
            description=parameter.description,
            default_value=str(parameter.default_value),
            position=str(parameter.position),
            prefix=parameter.prefix,
        )


class PlanViewModel:
    """A view model for a ``Plan``."""

    def __init__(
        self,
        id: str,
        name: str,
        full_command: str,
        inputs: List[CommandInputViewModel],
        outputs: List[CommandOutputViewModel],
        parameters: List[CommandParameterViewModel],
        description: Optional[str] = None,
        success_codes: Optional[str] = None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.full_command = full_command
        self.success_codes = success_codes
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = parameters

    @classmethod
    def from_plan(cls, plan: Plan):
        """Create view model from ``Plan``."""
        return cls(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            full_command=" ".join(plan.to_argv(with_streams=True)),
            success_codes=", ".join(str(c) for c in plan.success_codes),
            inputs=[CommandInputViewModel.from_input(input) for input in plan.inputs],
            outputs=[CommandOutputViewModel.from_output(output) for output in plan.outputs],
            parameters=[CommandParameterViewModel.from_parameter(param) for param in plan.parameters],
        )


def plan_view(workflow: AbstractPlan) -> Union[CompositePlanViewModel, PlanViewModel]:
    """Convert an ``CompositePlan`` or ``Plan`` to a ``ViewModel``."""
    if isinstance(workflow, CompositePlan):
        return CompositePlanViewModel.from_composite_plan(workflow)
    return PlanViewModel.from_plan(workflow)
