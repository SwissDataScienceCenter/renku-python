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
"""Test ``workflow`` commands."""

from pathlib import Path

from cwl_utils import parser_v1_2 as cwlgen

from renku.cli import cli
from renku.core.metadata.database import Database
from tests.utils import format_result_exception


def test_workflow_compose(runner, project, run_shell, client):
    """Test renku workflow compose."""
    # Run a shell command with pipe.
    output = run_shell('renku run --name run1 -- echo "a" > output1')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Run a shell command with pipe.
    output = run_shell("renku run --name run2 -- cp output1 output2")

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--map",
            "input_str=@step1.@param1",
            "--map",
            "output_file=run2.@output1",
            "--link",
            "@step1.@output1=@step2.@input1",
            "--set",
            "input_str=b",
            "--set",
            "output_file=other_output.csv",
            "-p",
            "input_str=the input string for the workflow",
            "-p",
            "output_file=the final output file produced",
            "composite_workflow",
            "run1",
            "run2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)

    composite_plan = database["plans-by-name"]["composite_workflow"]

    assert composite_plan

    assert len(composite_plan.plans) == 2
    assert len(composite_plan.mappings) == 2

    assert composite_plan.mappings[0].name == "input_str"
    assert composite_plan.mappings[0].default_value == "b"
    assert composite_plan.mappings[0].description == "the input string for the workflow"

    assert composite_plan.mappings[1].name == "output_file"
    assert composite_plan.mappings[1].default_value == "other_output.csv"
    assert composite_plan.mappings[1].description == "the final output file produced"


def test_workflow_show(runner, project, run_shell, client):
    """Test renku workflow show."""
    # Run a shell command with pipe.
    output = run_shell('renku run --name run1 --description "my workflow" --success-code 0 -- echo "a" > output1')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Run a shell command with pipe.
    output = run_shell("renku run --name run2 -- cp output1 output2")

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    result = runner.invoke(cli, ["workflow", "show", "run1"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "run1" in result.output
    assert "output1" in result.output
    assert "my workflow" in result.output

    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--description",
            "My composite workflow",
            "--map",
            "input_str=@step1.@param1",
            "--map",
            "output_file=run2.@output1",
            "--link",
            "@step1.@output1=@step2.@input1",
            "--set",
            "input_str=b",
            "--set",
            "output_file=other_output.csv",
            "-p",
            "input_str=the input string for the workflow",
            "-p",
            "output_file=the final output file produced",
            "composite_workflow",
            "run1",
            "run2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "show", "composite_workflow"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "composite_workflow" in result.output
    assert "input_str" in result.output
    assert "output_file" in result.output
    assert "Links:" in result.output
    assert "Mappings:" in result.output
    assert "My composite workflow" in result.output


def test_workflow_remove_command(runner, project):
    """test workflow remove with builder."""
    workflow_name = "test_workflow"

    result = runner.invoke(cli, ["workflow", "remove", workflow_name])
    assert 2 == result.exit_code

    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", workflow_name, "echo", "foo"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["workflow", "remove", "--force", workflow_name])
    assert 0 == result.exit_code


def test_workflow_export_command(runner, project):
    """test workflow export with builder."""
    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", "run1", "touch", "data.csv"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["workflow", "export", "run1", "-o", "run1.cwl"])
    assert 0 == result.exit_code
    assert Path("run1.cwl").exists()

    workflow = cwlgen.load_document("run1.cwl")
    assert workflow.baseCommand[0] == "touch"
    assert len(workflow.inputs) == 3
    assert len(workflow.outputs) == 1


def test_workflow_edit(runner, client):
    """Test naming of CWL tools and workflows."""
    workflow_name = "test"
    result = runner.invoke(cli, ["run", "--name", "test", "touch", "data.txt"])
    assert 0 == result.exit_code, format_result_exception(result)

    cmd = ["workflow", "edit", workflow_name, "--name", "first"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)

    test_plan = database["plans-by-name"][workflow_name]
    first_plan = database["plans-by-name"]["first"]

    assert first_plan
    assert first_plan.name == "first"
    assert first_plan.derived_from == test_plan.id

    cmd = ["workflow", "edit", workflow_name, "--description", "Test workflow"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    """
    assert len(composite_plan.plans) == 2
    assert len(composite_plan.mappings) == 2

    assert composite_plan.mappings[0].name == "input_str"
    assert composite_plan.mappings[0].default_value == "b"
    assert composite_plan.mappings[0].description == "the input string for the workflow"

    assert composite_plan.mappings[1].name == "output_file"
    assert composite_plan.mappings[1].default_value == "other_output.csv"
    assert composite_plan.mappings[1].description == "the final output file produced"
    """
