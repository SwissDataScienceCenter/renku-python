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
"""WorkflowFile view models."""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, List, Optional

from renku.core.workflow.model.workflow_file import HiddenParameter, Input, Output, Parameter, Step, WorkflowFile


@dataclasses.dataclass
class WorkflowFileViewModel:
    """View model for ``WorkflowFile``."""

    description: Optional[str]
    keywords: List[str]
    name: str
    path: str
    steps: List[StepViewModel]

    @classmethod
    def from_workflow_file(cls, workflow_file: WorkflowFile) -> WorkflowFileViewModel:
        """Create an instance from a ``WorkflowFile``."""
        return cls(
            description=workflow_file.description,
            keywords=workflow_file.keywords.copy(),
            name=workflow_file.name,
            path=workflow_file.path,
            steps=[StepViewModel.from_step(s) for s in workflow_file.steps],
        )


@dataclasses.dataclass
class StepViewModel:
    """View models for ``Step``."""

    command: str
    full_command: str
    date_created: datetime
    description: Optional[str]
    inputs: List[InputViewModel]
    keywords: List[str]
    name: str
    outputs: List[OutputViewModel]
    parameters: List[ParameterViewModel]
    success_codes: List[int]

    @classmethod
    def from_step(cls, step: Step) -> StepViewModel:
        """Create an instance from a ``Step``."""
        return cls(
            command=step.command,
            full_command=step.original_command,
            date_created=step.date_created,
            description=step.description,
            inputs=[InputViewModel.from_input(i) for i in step.inputs],
            keywords=step.keywords.copy(),
            name=step.name,
            outputs=[OutputViewModel.from_output(o) for o in step.outputs],
            parameters=[
                ParameterViewModel.from_parameter(p) for p in step.parameters if not isinstance(p, HiddenParameter)
            ],
            success_codes=step.success_codes.copy(),
        )


@dataclasses.dataclass
class ParameterViewModel:
    """View model for ``Parameter``."""

    description: Optional[str]
    implicit: bool
    name: Optional[str]
    position: Optional[int]
    prefix: Optional[str]
    value: Any

    @classmethod
    def from_parameter(cls, parameter: Parameter) -> ParameterViewModel:
        """Create an instance from a ``Parameter``."""
        return cls(
            description=parameter.description,
            implicit=parameter.implicit,
            name=parameter.name,
            position=parameter.position,
            prefix=parameter.prefix,
            value=parameter.value,
        )


@dataclasses.dataclass
class InputViewModel:
    """View model for ``Input``."""

    description: Optional[str]
    implicit: bool
    mapped_to: Optional[str]
    name: Optional[str]
    path: str
    position: Optional[int]
    prefix: Optional[str]

    @classmethod
    def from_input(cls, input: Input) -> InputViewModel:
        """Create an instance from an ``Input``."""
        return cls(
            description=input.description,
            implicit=input.implicit,
            mapped_to=input.mapped_to,
            name=input.name,
            path=input.path,
            position=input.position,
            prefix=input.prefix,
        )


@dataclasses.dataclass
class OutputViewModel:
    """View model for ``Output``."""

    description: Optional[str]
    implicit: bool
    mapped_to: Optional[str]
    name: Optional[str]
    path: str
    persist: bool
    position: Optional[int]
    prefix: Optional[str]

    @classmethod
    def from_output(cls, output: Output) -> OutputViewModel:
        """Create an instance from an ``Output``."""
        return cls(
            description=output.description,
            implicit=output.implicit,
            mapped_to=output.mapped_to,
            name=output.name,
            path=output.path,
            persist=output.persist,
            position=output.position,
            prefix=output.prefix,
        )
