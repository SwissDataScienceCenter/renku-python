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
"""Tests for Plan API."""

import os

from renku.ui.api import Activity, CompositePlan, Input, Output, Parameter, Plan
from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_list_plans(client_with_runs):
    """Test listing plans."""
    plans = Plan.list()

    assert {"plan-1", "plan-2"} == {p.name for p in plans}
    assert isinstance(plans[0], Plan)
    assert isinstance(plans[1], Plan)


def test_list_deleted_plans(client_with_runs, runner):
    """Test listing deleted plans."""
    result = runner.invoke(cli, ["workflow", "remove", "plan-1"])
    assert 0 == result.exit_code, format_result_exception(result)

    plans = Plan.list()

    assert {"plan-2"} == {p.name for p in plans}

    plans = Plan.list(include_deleted=True)

    assert {"plan-1", "plan-2"} == {p.name for p in plans}


def test_list_datasets_outside_a_renku_project(directory_tree):
    """Test listing plans in a non-renku directory."""
    os.chdir(directory_tree)

    assert [] == Plan.list()


def test_get_plan_attributes(client_with_runs):
    """Test getting attributes of a plan."""
    plan = next(p for p in Plan.list() if p.name == "plan-1")

    assert "/plans/1" == plan.id
    assert "command-1" == plan.command
    assert ["plans", "1"] == plan.keywords
    assert "First plan" == plan.description
    assert [0, 1] == plan.success_codes


def test_get_plan_parameters(client_with_runs):
    """Test getting parameters of a plan."""
    plan = next(p for p in Plan.list() if p.name == "plan-1")

    assert {"parameter-1"} == {p.name for p in plan.parameters}
    parameter = plan.parameters[0]

    assert isinstance(parameter, Parameter)
    assert "42" == parameter.default_value
    assert "42" == parameter.value
    assert "-n" == parameter.prefix.strip()
    assert 1 == parameter.position


def test_get_plan_inputs(client_with_runs):
    """Test getting inputs of a plan."""
    plan = next(p for p in Plan.list() if p.name == "plan-1")

    assert {"input-1"} == {p.name for p in plan.inputs}
    input = plan.inputs[0]

    assert isinstance(input, Input)
    assert "input" == input.default_value
    assert "input" == input.value
    assert input.prefix is None
    assert 2 == input.position
    assert input.mapped_stream is None


def test_get_plan_outputs(client_with_runs):
    """Test getting outputs of a plan."""
    plan = next(p for p in Plan.list() if p.name == "plan-1")

    assert {"output-1"} == {p.name for p in plan.outputs}
    output = plan.outputs[0]

    assert isinstance(output, Output)
    assert "intermediate" == output.default_value
    assert "intermediate" == output.value
    assert output.prefix is None
    assert 3 == output.position
    assert "stdout" == output.mapped_stream


def test_list_composite_plans(client_with_runs, runner):
    """Test listing plans."""
    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--map",
            "input_file=@step1.@input1",
            "--link",
            "@step1.@output1=@step2.@input1",
            "--description",
            "Composite plan",
            "composite-plan",
            "plan-1",
            "plan-2",
            "--set",
            "input_file=composite-input-file",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result)

    plans = Plan.list()

    assert {"plan-1", "plan-2"} == {p.name for p in plans}

    plans = CompositePlan.list()

    assert {"composite-plan"} == {p.name for p in plans}

    plan = next(p for p in plans if p.name == "composite-plan")
    assert isinstance(plan, CompositePlan)
    assert "Composite plan" == plan.description
    assert {"plan-1", "plan-2"} == {p.name for p in plan.plans}

    assert {"input_file"} == {m.name for m in plan.mappings}
    mapping = plan.mappings[0]
    assert "composite-input-file" == mapping.value
    mapping_parameter = mapping.parameters[0]
    assert isinstance(mapping_parameter, Input)
    assert "input-1" == mapping_parameter.name

    assert 1 == len(plan.links)
    link = plan.links[0]
    assert isinstance(link.source, Output)
    assert "output-1" == link.source.name
    assert ["input-1"] == [s.name for s in link.sinks]


def test_get_plan_activities(client_with_runs):
    """Test getting activities that are based on a plan."""
    plan = next(p for p in Plan.list() if p.name == "plan-1")

    assert 1 == len(plan.activities)
    activity = plan.activities[0]

    assert isinstance(activity, Activity)
