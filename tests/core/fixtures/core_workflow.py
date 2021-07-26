# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku core fixtures for workflow testing."""

import pytest

from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.parameter import CommandInput, CommandOutput, CommandParameter
from renku.core.models.workflow.plan import Plan


def _create_run(name: str) -> Plan:

    run_id = Plan.generate_id()
    input1 = CommandInput(
        id=CommandInput.generate_id(run_id, 1),
        position=1,
        default_value=1,
        name=f"{name}_input1",
    )
    input2 = CommandInput(
        id=CommandInput.generate_id(run_id, 2),
        position=2,
        default_value=2,
        name=f"{name}_input2",
    )

    output1 = CommandOutput(
        id=CommandOutput.generate_id(run_id, 3),
        position=3,
        default_value=3,
        name=f"{name}_output1",
    )
    output2 = CommandOutput(
        id=CommandOutput.generate_id(run_id, 4),
        position=4,
        default_value=4,
        name=f"{name}_output2",
    )

    param1 = CommandParameter(
        id=CommandParameter.generate_id(run_id, 5),
        position=5,
        default_value=5,
        name=f"{name}_param1",
    )
    param2 = CommandParameter(
        id=CommandParameter.generate_id(run_id, 6),
        position=6,
        default_value=6,
        name=f"{name}_param2",
    )

    return Plan(
        id=run_id,
        name=name,
        command="cat",
        inputs=[input1, input2],
        outputs=[output1, output2],
        parameters=[param1, param2],
    )


@pytest.fixture
def composite_plan():
    """Fixture for a basic CompositePlan."""
    run1 = _create_run("run1")
    run2 = _create_run("run2")

    grouped = CompositePlan(id=CompositePlan.generate_id(), plans=[run1, run2], name="grouped1")

    return grouped, run1, run2
