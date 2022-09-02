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

from datetime import datetime, timedelta

import pytest

from renku.domain_model.provenance.activity import Activity
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.parameter import CommandInput, CommandOutput, CommandParameter
from renku.domain_model.workflow.plan import Plan
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from tests.utils import create_dummy_plan


@pytest.fixture
def composite_plan():
    """Fixture for a basic CompositePlan."""

    def create_run(name: str) -> Plan:

        run_id = Plan.generate_id()
        input1 = CommandInput(
            id=CommandInput.generate_id(run_id, 1),
            position=1,
            default_value="1",
            name=f"{name}_input1",
        )
        input2 = CommandInput(
            id=CommandInput.generate_id(run_id, 2),
            position=2,
            default_value="2",
            name=f"{name}_input2",
        )

        output1 = CommandOutput(
            id=CommandOutput.generate_id(run_id, 3),
            position=3,
            default_value="3",
            name=f"{name}_output1",
        )
        output2 = CommandOutput(
            id=CommandOutput.generate_id(run_id, 4),
            position=4,
            default_value="4",
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

    run1 = create_run("run1")
    run2 = create_run("run2")

    grouped = CompositePlan(id=CompositePlan.generate_id(), plans=[run1, run2], name="grouped1")

    return grouped, run1, run2


@pytest.fixture
def project_with_runs(repository, with_injections_manager):
    """A client with runs."""

    def create_activity(plan, date, index) -> Activity:
        """Create an activity with id /activities/index."""
        return Activity.from_plan(
            plan=plan,
            id=Activity.generate_id(str(index)),
            started_at_time=date,
            ended_at_time=date + timedelta(seconds=1),
        )

    date_1 = datetime(2022, 5, 20, 0, 42, 0)
    date_2 = datetime(2022, 5, 20, 0, 43, 0)

    plan_1 = create_dummy_plan(
        command="command-1",
        date_created=date_1,
        description="First plan",
        index=1,
        inputs=["input"],
        keywords=["plans", "1"],
        name="plan-1",
        outputs=[("intermediate", "stdout")],
        parameters=[("parameter-1", "42", "-n ")],
        success_codes=[0, 1],
    )

    plan_2 = create_dummy_plan(
        command="command-2",
        date_created=date_2,
        description="Second plan",
        index=2,
        inputs=["intermediate"],
        keywords=["plans", "2"],
        name="plan-2",
        outputs=[("output", "stdout")],
        parameters=[("int-parameter", 43, "-n "), ("str-parameter", "some value", None)],
    )

    with with_injections_manager(repository):
        activity_1 = create_activity(plan_1, date_1, index=1)
        activity_2 = create_activity(plan_2, date_2, index=2)

        activity_gateway = ActivityGateway()

        activity_gateway.add(activity_1)
        activity_gateway.add(activity_2)

    repository.add(all=True)
    repository.commit("Add runs")

    yield repository
