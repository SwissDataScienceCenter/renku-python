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
from renku.core.models.provenance.provenance_graph import ProvenanceGraph


def test_run_simple(runner, project):
    """Test tracking of run command."""
    cmd = ["echo", "test"]

    result = runner.invoke(cli, ["run", "--no-output"] + cmd)
    assert 0 == result.exit_code

    # There are no output files.
    result = runner.invoke(cli, ["log"])
    assert 0 == result.exit_code
    assert 1 == len(result.output.strip().split("\n"))

    # Display tools with no outputs.
    result = runner.invoke(cli, ["log", "--no-output"])
    assert 0 == result.exit_code
    assert ".renku/workflow/" in result.output


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


@pytest.mark.skip("needs a fix for https://github.com/RDFLib/OWL-RL/issues/44")
@pytest.mark.serial
@pytest.mark.shelled
def test_run_clean(runner, project, run_shell):
    """Test tracking of run command in clean repo."""
    # Run a shell command with pipe.
    output = run_shell('renku run echo "a" > output')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["log"])
    assert "output" in result.output
    assert ".yaml" in result.output
    assert ".renku/workflow/" in result.output


def test_run_metadata(renku_cli, client_with_new_graph):
    """Test run with workflow metadata."""
    exit_code, activity = renku_cli(
        "run", "--name", "run-1", "--description", "first run", "--keyword", "key1", "--keyword", "key2", "touch", "foo"
    )

    assert 0 == exit_code
    assert "run-1" == activity.name
    assert "first run" == activity.description
    assert {"key1", "key2"} == set(activity.keywords)

    plan = client_with_new_graph.dependency_graph.plans[0]
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
def test_generated_run_name(runner, client, command, name):
    """Test generated run name."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code

    result = runner.invoke(cli, ["run", "--no-output"] + command)

    assert 0 == result.exit_code
    assert 1 == len(client.dependency_graph.plans)
    assert name == client.dependency_graph.plans[0].name[:-5]


def test_run_invalid_name(runner, client):
    """Test run with invalid name."""
    result = runner.invoke(cli, ["run", "--name", "invalid name", "touch", "foo"])

    assert 2 == result.exit_code
    assert not (client.path / "foo").exists()
    assert "Invalid name: 'invalid name' (Hint: 'invalid_name' is valid)." in result.output


def test_run_argument_parameters(runner, client):
    """Test names and values of workflow/provenance arguments and parameters."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code

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

    assert 0 == result.exit_code
    assert 1 == len(client.dependency_graph.plans)
    plan = client.dependency_graph.plans[0]

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

    provenance_graph = ProvenanceGraph.from_json(client.provenance_graph_path)
    assert 1 == len(provenance_graph.activities)
    activity = provenance_graph.activities[0]

    assert 2 == len(activity.usages)
    activity.usages.sort(key=lambda e: e.entity.path)
    assert "Dockerfile" == activity.usages[0].entity.path
    assert "requirements.txt" == activity.usages[1].entity.path

    assert 5 == len(activity.parameters)
    parameters_values = {p.parameter.default_value for p in activity.parameters}
    assert {42, "Dockerfile", "README.md", "requirements.txt", "some message"} == parameters_values

    result = runner.invoke(cli, ["graph", "export", "--format", "jsonld", "--strict"])

    assert 0 == result.exit_code, result.output
