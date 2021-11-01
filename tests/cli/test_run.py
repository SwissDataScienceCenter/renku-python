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
"""Test ``run`` command."""

import os

import pytest

from renku.cli import cli
from renku.core.metadata.gateway.activity_gateway import ActivityGateway
from renku.core.metadata.gateway.plan_gateway import PlanGateway
from tests.utils import format_result_exception


def test_run_simple(runner, project):
    """Test tracking of run command."""
    cmd = ["echo", "test"]

    result = runner.invoke(cli, ["run", "--no-output"] + cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    # Display tools with no outputs.
    result = runner.invoke(cli, ["graph", "export"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "test" in result.output


def test_run_many_args(client, run):
    """Test a renku run command which implicitly relies on many inputs."""
    os.mkdir("files")
    output = "output.txt"
    for i in range(103):
        os.system("touch files/{}.txt".format(i))
    client.repo.index.add(["files/"])
    client.repo.index.commit("add many files")

    exit_code = run(args=("run", "ls", "files/"), stdout=output)
    assert 0 == exit_code


@pytest.mark.serial
@pytest.mark.shelled
def test_run_clean(runner, project, run_shell):
    """Test tracking of run command in clean repo."""
    # Run a shell command with pipe.
    output = run_shell('renku run echo "a unique string" > my_output_file')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export"])
    assert "my_output_file" in result.output
    assert "a unique string" in result.output


def test_run_metadata(renku_cli, client, client_database_injection_manager):
    """Test run with workflow metadata."""
    exit_code, activity = renku_cli(
        "run", "--name", "run-1", "--description", "first run", "--keyword", "key1", "--keyword", "key2", "touch", "foo"
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert "run-1" == plan.name
    assert "first run" == plan.description
    assert {"key1", "key2"} == set(plan.keywords)

    with client_database_injection_manager(client):
        plan_gateway = PlanGateway()
        plan = plan_gateway.get_by_id(plan.id)
        assert "run-1" == plan.name
        assert "first run" == plan.description
        assert {"key1", "key2"} == set(plan.keywords)


@pytest.mark.parametrize(
    "command, name",
    [
        (["echo", "-n", "some value"], "echo--n-some_value-"),
        (["echo", "-n", "some long value"], "echo--n-some_long_v-"),
    ],
)
def test_generated_run_name(runner, client, command, name, client_database_injection_manager):
    """Test generated run name."""
    result = runner.invoke(cli, ["run", "--no-output"] + command)

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        plan_gateway = PlanGateway()
        plan = plan_gateway.get_all_plans()[0]
        assert name == plan.name[:-5]


def test_run_invalid_name(runner, client):
    """Test run with invalid name."""
    result = runner.invoke(cli, ["run", "--name", "invalid name", "touch", "foo"])

    assert 2 == result.exit_code
    assert not (client.path / "foo").exists()
    assert "Invalid name: 'invalid name' (Hint: 'invalid_name' is valid)." in result.output


def test_run_argument_parameters(runner, client, client_database_injection_manager):
    """Test names and values of workflow/provenance arguments and parameters."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--input",
            "Dockerfile",
            "--output",
            "README.md",
            "echo",
            "-n",
            "some message",
            "--template",
            "requirements.txt",
            "--delta",
            "42",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        plan_gateway = PlanGateway()
        plans = plan_gateway.get_all_plans()
        assert 1 == len(plans)
        plan = plans[0]

        assert 2 == len(plan.inputs)
        plan.inputs.sort(key=lambda i: i.name)
        assert plan.inputs[0].name.startswith("input-")
        assert "template-2" == plan.inputs[1].name

        assert 1 == len(plan.outputs)
        assert plan.outputs[0].name.startswith("output-")

        assert 2 == len(plan.parameters)
        plan.parameters.sort(key=lambda i: i.name)
        assert "delta-3" == plan.parameters[0].name
        assert "n-1" == plan.parameters[1].name

        activity_gateway = ActivityGateway()
        activities = activity_gateway.get_all_activities()
        assert 1 == len(activities)
        activity = activities[0]

        assert 2 == len(activity.usages)
        activity.usages.sort(key=lambda e: e.entity.path)
        assert "Dockerfile" == activity.usages[0].entity.path
        assert "requirements.txt" == activity.usages[1].entity.path

        assert 5 == len(activity.parameters)
        parameters_values = {p.value for p in activity.parameters}
        assert {"42", "Dockerfile", "README.md", "requirements.txt", "some message"} == parameters_values

    result = runner.invoke(cli, ["graph", "export", "--format", "jsonld", "--strict"])

    assert 0 == result.exit_code, format_result_exception(result)
