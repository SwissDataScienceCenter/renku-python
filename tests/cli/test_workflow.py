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

from renku.cli import cli
from renku.core.metadata.database import Database


def test_workflow_group(runner, project, run_shell, client):
    """Test renku workflow group."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code

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
            "group",
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
            "grouped_workflow",
            "run1",
            "run2",
        ],
    )

    assert 0 == result.exit_code

    database = Database.from_path(client.database_path)

    grouped_run = database.get("plans-by-name").get("grouped_workflow")

    assert grouped_run

    assert len(grouped_run.plans) == 2
    assert len(grouped_run.mappings) == 2

    assert grouped_run.mappings[0].name == "input_str"
    assert grouped_run.mappings[0].default_value == "b"
    assert grouped_run.mappings[0].description == "the input string for the workflow"

    assert grouped_run.mappings[1].name == "output_file"
    assert grouped_run.mappings[1].default_value == "other_output.csv"
    assert grouped_run.mappings[1].description == "the final output file produced"
