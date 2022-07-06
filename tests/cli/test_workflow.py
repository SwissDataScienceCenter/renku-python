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
"""Test ``workflow`` commands."""

import datetime
import itertools
import logging
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

import pexpect
import pyte
import pytest
from cwl_utils.parser import cwl_v1_2 as cwlgen

from renku.core.plugin.provider import available_workflow_providers
from renku.core.util.yaml import write_yaml
from renku.infrastructure.database import Database
from renku.infrastructure.gateway.activity_gateway import ActivityGateway
from renku.ui.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


def _execute(capsys, runner, args):
    with capsys.disabled():
        try:
            cli.main(
                args=args,
                prog_name=runner.get_default_prog_name(cli),
            )
        except SystemExit as e:
            assert e.code in {None, 0}


def test_workflow_list(runner, project, run_shell, client):
    """Test listing of workflows."""
    # Run a shell command with pipe.
    output = run_shell('renku run --name run1 --description desc1 -- echo "a" > output1')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Run a shell command with pipe.
    output = run_shell("renku run --name run2 --description desc2 -- cp output1 output2")

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    result = runner.invoke(cli, ["workflow", "ls"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "run1" in result.output
    assert "run2" in result.output
    assert "desc1" not in result.output
    assert "desc2" not in result.output
    assert "echo a > output1" in result.output
    assert "cp output1 output2" in result.output

    result = runner.invoke(cli, ["workflow", "ls", "-c", "id,description"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "run1" not in result.output
    assert "run2" not in result.output
    assert "desc1" in result.output
    assert "desc2" in result.output
    assert "echo a > output1" not in result.output
    assert "cp output1 output2" not in result.output

    result = runner.invoke(cli, ["workflow", "ls", "--format", "json"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "run1" in result.output
    assert "run2" in result.output
    assert "desc1" in result.output
    assert "desc2" in result.output
    assert "echo a > output1" in result.output
    assert "cp output1 output2" in result.output


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

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_compose_from_paths(runner, project, run_shell, client):
    """Test renku workflow compose with input/output paths."""
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

    # Run a shell command with pipe.
    output = run_shell("renku run --name run3 -- cat output1 output2 > output3")

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--to",
            "output3",
            "composite_workflow1",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)

    composite_plan = database["plans-by-name"]["composite_workflow1"]

    assert composite_plan

    assert len(composite_plan.plans) == 3
    assert len(composite_plan.mappings) == 7
    assert composite_plan.mappings[0].name == "1-output-2"
    assert composite_plan.mappings[0].default_value == "output1"

    assert composite_plan.mappings[6].name == "3-output-3"
    assert composite_plan.mappings[6].default_value == "output3"

    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--from",
            "output1",
            "--to",
            "output3",
            "composite_workflow2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)

    composite_plan = database["plans-by-name"]["composite_workflow2"]

    assert composite_plan

    assert len(composite_plan.plans) == 2
    assert len(composite_plan.mappings) == 5
    assert composite_plan.mappings[0].name == "1-input-1"
    assert composite_plan.mappings[0].default_value == "output1"

    assert composite_plan.mappings[4].name == "2-output-3"
    assert composite_plan.mappings[4].default_value == "output3"

    result = runner.invoke(
        cli,
        [
            "workflow",
            "compose",
            "--from",
            "output1",
            "composite_workflow3",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)

    composite_plan = database["plans-by-name"]["composite_workflow3"]

    assert composite_plan

    assert len(composite_plan.plans) == 2
    assert len(composite_plan.mappings) == 5
    assert composite_plan.mappings[0].name == "1-input-1"
    assert composite_plan.mappings[0].default_value == "output1"

    assert composite_plan.mappings[4].name == "2-output-3"
    assert composite_plan.mappings[4].default_value == "output3"


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
    """Test workflow remove with builder."""
    workflow_name = "test_workflow"

    result = runner.invoke(cli, ["workflow", "remove", workflow_name])
    assert 2 == result.exit_code

    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", workflow_name, "echo", "foo"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "remove", "--force", workflow_name])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "edit", workflow_name, "--name", "new_name"])
    assert 2 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "execute", workflow_name])
    assert 2 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "iterate", workflow_name])
    assert 2 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "compose", "composite", workflow_name])
    assert 1 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "export", workflow_name])
    assert 2 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "show", workflow_name])
    assert 2 == result.exit_code, format_result_exception(result)


def test_workflow_export_command(runner, project):
    """Test workflow export with builder."""
    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", "run1", "touch", "data.csv"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "export", "run1", "-o", "run1.cwl"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert Path("run1.cwl").exists()

    workflow = cwlgen.load_document("run1.cwl")
    assert workflow.baseCommand[0] == "touch"
    assert len(workflow.inputs) == 2
    assert len(workflow.outputs) == 1


def test_workflow_edit(runner, client):
    """Test naming of CWL tools and workflows."""

    def _get_plan_id(output):
        return output.split("\n")[0].split(":")[1].strip()

    workflow_name = "run1"
    result = runner.invoke(cli, ["run", "--name", workflow_name, "touch", "data.txt"])
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)
    test_plan = database["plans-by-name"][workflow_name]

    cmd = ["workflow", "edit", workflow_name, "--name", "first"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    workflow_name = "first"
    database = Database.from_path(client.database_path)
    first_plan = database["plans-by-name"]["first"]

    assert first_plan
    assert first_plan.name == "first"
    assert first_plan.derived_from == test_plan.id

    cmd = ["workflow", "edit", workflow_name, "--description", "Test workflow"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)
    first_plan = database["plans"][_get_plan_id(result.stdout)]
    assert first_plan.description == "Test workflow"

    # edit parameter
    cmd = ["workflow", "edit", workflow_name, "--rename-param", "param1=param2"]
    result = runner.invoke(cli, cmd)
    assert 0 != result.exit_code, format_result_exception(result)

    cmd = ["workflow", "edit", workflow_name, "--set", "param1=0"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)
    edited_plan_id = _get_plan_id(result.output)

    database = Database.from_path(client.database_path)
    renamed_param_plan = database["plans"][_get_plan_id(result.output)]
    assert len(renamed_param_plan.parameters) > 0

    cmd = ["workflow", "edit", workflow_name, "--rename-param", "param1=param2"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)
    renamed_param_plan = database["plans"][_get_plan_id(result.output)]
    parameter_names = list(map(lambda x: x.name, renamed_param_plan.parameters))
    assert len(parameter_names) > 0
    assert "param1" not in parameter_names
    assert "param2" in parameter_names
    param2_plan_id = _get_plan_id(result.stdout)

    # edit parameter description
    cmd = ["workflow", "edit", param2_plan_id, "--describe-param", "param1=Test"]
    result = runner.invoke(cli, cmd)
    assert 0 != result.exit_code, format_result_exception(result)

    cmd = ["workflow", "edit", param2_plan_id, "--describe-param", 'param2="Test parameter"']
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)
    renamed_param_plan = database["plans"][_get_plan_id(result.output)]
    assert "Test parameter" == renamed_param_plan.parameters[0].description

    # edit parameter mapping
    cmd = ["run", "--name", "run2", "cp", "data.txt", "data2.txt"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

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
            edited_plan_id,
            "run2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    # remove mapping
    cmd = ["workflow", "edit", "composite_workflow", "--map", "output_file="]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    database = Database.from_path(client.database_path)
    edited_composite_plan = database["plans"][_get_plan_id(result.output)]
    assert len(edited_composite_plan.mappings) == 1
    assert edited_composite_plan.mappings[0].mapped_parameters[0].name == "param1"

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_edit_no_change(runner, client, run_shell):
    """Ensure that workflow edit doesn't commit if there's no changes."""

    workflow_name = "my-workflow"

    result = runner.invoke(cli, ["run", "--name", workflow_name, "touch", "data.txt"])
    assert 0 == result.exit_code, format_result_exception(result)

    before = client.repository.head.commit

    result = runner.invoke(cli, ["workflow", "edit", workflow_name])
    assert 0 == result.exit_code, format_result_exception(result)

    assert before == client.repository.head.commit


def test_workflow_show_outputs_with_directory(runner, client, run):
    """Output files in directory are not shown as separate outputs."""
    base_sh = ["bash", "-c", 'DIR="$0"; mkdir -p "$DIR"; ' 'for x in "$@"; do touch "$DIR/$x"; done']

    assert 0 == run(args=["run"] + base_sh + ["output", "foo", "bar"])
    assert (client.path / "output" / "foo").exists()
    assert (client.path / "output" / "bar").exists()

    cmd = ["workflow", "outputs"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output/foo"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output/foo", "output/bar"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"output"} == set(result.output.strip().split("\n"))


@pytest.mark.parametrize("provider", available_workflow_providers())
@pytest.mark.parametrize("yaml", [False, True])
@pytest.mark.parametrize(
    "workflows, parameters",
    [
        ([("run", 'echo "a" > output1')], {}),
        ([("run", 'echo "a" > output1')], {"run": {"outputs": ["replaced"]}}),
        ([("run", 'echo "a" > output1')], {"run": {"parameters": ["foo"], "outputs": ["bar"]}}),
        ([("run1", "touch data.csv"), ("run2", "wc data.csv > output")], {}),
        (
            [("run1", "touch data.csv"), ("run2", "wc data.csv > output")],
            {"run1": {"outputs": ["foo"]}, "run2": {"inputs": ["foo"], "outputs": ["bar"]}},
        ),
        (
            [("run1", "touch data.csv"), ("run2", "touch data2.csv"), ("run3", "wc data.csv data2.csv > output")],
            {
                "run1": {"outputs": ["foo"]},
                "run2": {"outputs": ["bar"]},
                "run3": {"inputs": ["foo", "bar"], "outputs": ["changed"]},
            },
        ),
    ],
)
def test_workflow_execute_command(runner, run_shell, project, capsys, client, provider, yaml, workflows, parameters):
    """Test workflow execute."""

    for wf in workflows:
        output = run_shell(f"renku run --name {wf[0]} -- {wf[1]}")
        # Assert expected empty stdout.
        assert b"" == output[0]
        # Assert not allocated stderr.
        assert output[1] is None

    is_composite = True if len(workflows) > 1 else False

    if is_composite:
        composed_name = uuid.uuid4().hex
        cmd = itertools.chain(["workflow", "compose", composed_name], map(lambda x: x[0], workflows))

        result = runner.invoke(cli, cmd)
        assert 0 == result.exit_code, format_result_exception(result)

    def _flatten_dict(obj, key_string=""):
        if type(obj) == dict:
            key_string = key_string + "." if key_string else key_string
            for k in obj:
                yield from _flatten_dict(obj[k], key_string + str(k))
        else:
            yield key_string, obj

    workflow_name = composed_name if is_composite else workflows[0][0]

    if not parameters:
        execute_cmd = ["workflow", "execute", "-p", provider, workflow_name]
        _execute(capsys, runner, execute_cmd)
    else:
        database = Database.from_path(client.database_path)
        plan = database["plans-by-name"][workflow_name]
        execute_cmd = ["workflow", "execute", "-p", provider]

        overrides = dict()
        outputs = []
        if is_composite:
            overrides = {}
            for p in plan.plans:
                if p.name not in parameters:
                    continue
                overrides[p.name] = {}
                for k, values in parameters[p.name].items():
                    for i, v in enumerate(values):
                        overrides[p.name][getattr(p, k)[i].name] = v
                        if k == "outputs":
                            outputs.append(v)
        else:
            for k, values in parameters[workflow_name].items():
                for i, v in enumerate(values):
                    overrides[getattr(plan, k)[i].name] = v
                    if k == "outputs":
                        outputs.append(v)

        if yaml:
            fd, values_path = tempfile.mkstemp()
            os.close(fd)
            write_yaml(values_path, overrides)
            execute_cmd += ["--values", values_path]
        else:
            override_generator = _flatten_dict(overrides) if is_composite else overrides.items()
            [execute_cmd.extend(["--set", f"{k}={v}"]) for k, v in override_generator]

        execute_cmd.append(workflow_name)

        _execute(capsys, runner, execute_cmd)

        # check whether parameters setting was effective
        for o in outputs:
            assert Path(o).resolve().exists()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_workflow_execute_command_with_api_parameter_set(runner, run_shell, project, capsys, client, provider):
    """Test executing a workflow with --set for a renku.ui.api.Parameter."""
    script = client.path / "script.py"
    output = client.path / "output"

    with client.commit():
        script.write_text("from renku.ui.api import Parameter\n" 'print(Parameter("test", "hello world").value)\n')

    result = run_shell(f"renku run --name run1 -- python3 {script} > {output}")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    assert "hello world\n" == output.read_text()

    result = run_shell(f"renku workflow execute -p {provider} --set test=goodbye run1")

    # Assert not allocated stderr.
    assert result[1] is None

    assert "goodbye\n" == output.read_text()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_workflow_execute_command_with_api_input_set(runner, run_shell, project, capsys, client, provider):
    """Test executing a workflow with --set for a renku.ui.api.Input."""
    script = client.path / "script.py"
    output = client.path / "output"
    input = client.path / "input"
    input.write_text("input string")
    other_input = client.path / "other_input"
    other_input.write_text("my other input string")

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Input\nwith open(Input('my-input', '{input.name}'), 'r') as f:\n"
            "    print(f.read())"
        )

    result = run_shell(f"renku run --name run1 -- python3 {script.name} > {output.name}")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    assert "input string\n" == output.read_text()
    result = run_shell(f"renku workflow execute -p {provider} --set my-input={other_input.name} run1")

    # Assert not allocated stderr.
    assert result[1] is None

    assert "my other input string\n" == output.read_text()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_workflow_execute_command_with_api_output_set(runner, run_shell, project, capsys, client, provider):
    """Test executing a workflow with --set for a renku.ui.api.Output."""
    script = client.path / "script.py"
    output = client.path / "output"
    other_output = client.path / "other_output"

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Output\nwith open(Output('my-output', '{output.name}'), 'w') as f:\n"
            "    f.write('test')"
        )

    result = run_shell(f"renku run --name run1 -- python3 {script.name}")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    assert "test" == output.read_text()
    result = run_shell(f"renku workflow execute -p {provider} --set my-output={other_output.name} run1")

    # Assert not allocated stderr.
    assert result[1] is None

    assert "test" == other_output.read_text()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_execute_command_with_api_duplicate_output(runner, run_shell, project, capsys, client):
    """Test executing a workflow with duplicate output with differing path."""
    script = client.path / "script.py"
    output = client.path / "output"
    other_output = client.path / "other_output"

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Output\nopen(Output('my-output', '{output.name}'), 'w')\n"
            f"open(Output('my-output', '{other_output.name}'), 'w')"
        )

    result = run_shell(f"renku run --name run1 -- python {script.name}")

    # Assert expected empty stdout.
    assert b"Error: Invalid parameter value - Duplicate input/output name found: my-output\n" in result[0]


def test_workflow_execute_command_with_api_valid_duplicate_output(runner, run_shell, project, capsys, client):
    """Test executing a workflow with duplicate output with same path."""
    script = client.path / "script.py"
    output = client.path / "output"

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Output\nopen(Output('my-output', '{output.name}'), 'w')\n"
            f"open(Output('my-output', '{output.name}'), 'w')"
        )

    result = run_shell(f"renku run --name run1 -- python3 {script.name}")

    # Assert expected empty stdout.
    assert b"" == result[0]

    # Assert not allocated stderr.
    assert result[1] is None


def test_workflow_execute_command_with_api_duplicate_input(runner, run_shell, project, capsys, client):
    """Test executing a workflow with duplicate input with differing path."""
    script = client.path / "script.py"
    input = client.path / "input"
    other_input = client.path / "other_input"

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Input\nopen(Input('my-input', '{input.name}'), 'w')\n"
            f"open(Input('my-input', '{other_input.name}'), 'w')"
        )

    result = run_shell(f"renku run --no-output --name run1 -- python {script.name}")

    # Assert expected empty stdout.
    assert b"Error: Invalid parameter value - Duplicate input/output name found: my-input\n" in result[0]


def test_workflow_execute_command_with_api_valid_duplicate_input(runner, run_shell, project, capsys, client):
    """Test executing a workflow with duplicate input with same path."""
    script = client.path / "script.py"
    input = client.path / "input"

    with client.commit():
        script.write_text(
            f"from renku.ui.api import Input\nopen(Input('my-input', '{input.name}'), 'w')\n"
            f"open(Input('my-input', '{input.name}'), 'w')"
        )

    result = run_shell(f"renku run --no-output --name run1 -- python {script.name}")

    # Assert expected empty stdout.
    assert b"" == result[0]

    # Assert not allocated stderr.
    assert result[1] is None


def test_workflow_visualize_non_interactive(runner, project, client, workflow_graph):
    """Test renku workflow visualize in non-interactive mode."""

    # We don't use pytest paramtrization for performance reasons, so we don't need to build the workflow_graph fixture
    # for each execution
    columns = [[], ["-c", "command"], ["-c", "command,id,date,plan"]]
    from_command = [
        ([], set()),
        (["--from", "B"], {"A", "Z", "H", "J"}),
        (["--from", "A"], {"H", "J"}),
        (["--from", "B", "--from", "A"], {"H", "J"}),
        (["--from", "H"], {"A", "S", "Z"}),
    ]
    paths = [([], set()), (["X"], {"H", "J"}), (["B", "Z"], {"H", "J", "X", "Y"}), (["J"], {"A", "S", "Z"})]

    commands = list(itertools.product(columns, from_command, paths))

    for command in commands:
        excludes = set()
        base_command = ["workflow", "visualize", "--no-pager"]
        columns, from_command, paths = command
        if columns:
            base_command.extend(columns)
        if from_command[0]:
            base_command.extend(from_command[0])
            excludes |= from_command[1]
        if paths[0]:
            base_command.extend(paths[0])
            excludes |= paths[1]

        result = runner.invoke(cli, base_command)
        assert 0 == result.exit_code, format_result_exception(result)

        assert all(e not in result.output for e in excludes)

    result = runner.invoke(
        cli, ["workflow", "visualize", "--no-pager", "-x", "-a", "--no-color", "--revision", "HEAD^", "H", "S"]
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "A" in result.output
    assert "J" not in result.output
    assert "S" in result.output
    assert "H" in result.output


@pytest.mark.skip(
    "Doesn't actually work, not really a tty available in github actions, "
    "see https://github.com/actions/runner/issues/241"
)
def test_workflow_visualize_interactive(runner, project, client, workflow_graph):
    """Test renku workflow visualize in interactive mode."""

    dimensions = (120, 120)
    screen = pyte.Screen(dimensions[1], dimensions[0])
    stream = pyte.Stream(screen)

    output = []

    def _try_and_show_error(child):
        # If there was an error, we'd get the 'Aaaaahhh' screen, so get it to print the exception and return the
        # screen after that.
        child.send("\n")
        child.expect(pexpect.TIMEOUT, timeout=2)
        return _update_screen(child.before)

    def _update_screen(data):
        output.append(data)
        for chunk in output:
            stream.feed(chunk)

        lines = screen.display
        screen.reset()

        return "\n".join(lines)

    env = os.environ.copy()
    env.update({"LINES": str(dimensions[0]), "COLUMNS": str(dimensions[1])})

    # we need bash so everything behaves properly
    child = pexpect.spawn("renku workflow visualize --interactive", encoding="utf-8", dimensions=dimensions, echo=False)
    child.expect(pexpect.TIMEOUT, timeout=10)

    content = _update_screen(child.before)

    assert "Navigate using arrow keys" in content, _try_and_show_error(child)
    assert "Press <q> to exit" in content, _try_and_show_error(child)
    assert "echo test > A" in content, _try_and_show_error(child)

    # show activity details
    child.send(chr(10))
    child.expect(pexpect.TIMEOUT, timeout=5)
    content = _update_screen(child.before)
    assert "Plan Id:" in content, _try_and_show_error(child)
    assert "Inputs:" in content, _try_and_show_error(child)
    assert "Outputs:" in content, _try_and_show_error(child)
    assert "Agents:" in content, _try_and_show_error(child)
    assert "Press <q> to exit" not in content, _try_and_show_error(child)

    # move cursor around a bit, we use letters because ANSI codes don't seem to work in pexpect
    child.send("k")
    child.expect(pexpect.TIMEOUT, timeout=2)
    _update_screen(child.before)
    child.send("l")
    child.expect(pexpect.TIMEOUT, timeout=2)
    _update_screen(child.before)
    child.send("k")
    child.expect(pexpect.TIMEOUT, timeout=2)
    _update_screen(child.before)
    child.send("j")
    child.expect(pexpect.TIMEOUT, timeout=2)
    _update_screen(child.before)
    child.send("i")
    child.expect(pexpect.TIMEOUT, timeout=2)
    _update_screen(child.before)

    child.send(chr(10))
    child.expect(pexpect.TIMEOUT, timeout=5)
    content = _update_screen(child.before)
    assert "Plan Id:" in content, _try_and_show_error(child)
    assert "Inputs:" in content, _try_and_show_error(child)
    assert "Outputs:" in content, _try_and_show_error(child)
    assert "Agents:" in content, _try_and_show_error(child)
    assert "Press <q> to exit" not in content, _try_and_show_error(child)

    child.send("\n")
    child.expect(pexpect.TIMEOUT, timeout=5)
    _update_screen(child.before)

    child.send("h")
    child.expect(pexpect.TIMEOUT, timeout=5)
    content = _update_screen(child.before)

    assert "Navigate using arrow keys" in content, _try_and_show_error(child)
    assert "Press <q> to exit" in content, _try_and_show_error(child)
    assert "echo test > A" in content, _try_and_show_error(child)
    assert "Inputs:" not in content, _try_and_show_error(child)
    assert "Outputs:" not in content, _try_and_show_error(child)
    assert "Agents:" not in content, _try_and_show_error(child)

    child.send("q")

    child.expect(pexpect.EOF, timeout=2)
    assert not child.isalive()


def test_workflow_compose_execute(runner, project, run_shell, client):
    """Test renku workflow compose with execute."""
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

    # Run a shell command with pipe.
    output = run_shell('renku run --name run3 -- echo "b" > output3')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Run a shell command with pipe.
    output = run_shell("renku run --name run4 -- cp output3 output4")

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # we need to run in a subprocess to ensure the execute below uses a clean Database, to test against
    # issues with cached parameters
    output = run_shell("renku workflow compose --link run2.output-2=run4.input-1 composite_workflow1 run1 run2 run4")

    # Assert not allocated stderr.
    assert output[1] is None

    assert "b\n" == Path("output4").read_text()

    output = run_shell('renku workflow execute --set run1.parameter-1="xyz" composite_workflow1')

    # Assert not allocated stderr.
    assert output[1] is None

    assert "xyz\n" == Path("output4").read_text()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
@pytest.mark.parametrize(
    "workflow, parameters, num_iterations",
    [
        ('echo "a" > output', {}, 0),
        ('echo "a" > output', {"parameter-1": ["b", "c", "d"], "output-2": "output_{iter_index}"}, 3),
        (
            "head -n 1 Dockerfile > output",
            {
                "input-2": ["environment.yml", "Dockerfile", "requirements.txt"],
                "n-1": ["3", "4"],
                "output-3": "output_{iter_index}",
            },
            6,
        ),
        (
            "head -n 1 Dockerfile > output",
            {
                "input-2@tag1": ["environment.yml", "Dockerfile", "requirements.txt"],
                "n-1@tag1": ["3", "4", "5"],
                "output-3": "output_{iter_index}",
            },
            3,
        ),
        (
            'sh -c \'head -n "$0" "$1" | tail -n "$2" > "$3"\' 1 Dockerfile 1 output',
            {
                "input-3@tag1": ["environment.yml", "requirements.txt"],
                "parameter-2@tag2": ["3", "4", "5"],
                "parameter-4@tag2": ["1", "2", "3"],
                "output-5": "output_{iter_index}",
            },
            6,
        ),
    ],
)
def test_workflow_iterate(runner, run_shell, client, workflow, parameters, provider, num_iterations):
    """Test renku workflow iterate."""

    workflow_name = "foobar"

    # Run a shell command with pipe.
    output = run_shell(f"renku run --name {workflow_name} -- {workflow}")
    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    iteration_cmd = ["renku", "workflow", "iterate", "-p", provider, workflow_name]
    outputs = []
    index_re = re.compile(r"{iter_index}")

    for k, v in filter(lambda x: x[0].startswith("output"), parameters.items()):
        if isinstance(v, str) and index_re.search(v):
            outputs += [index_re.sub(str(i), v) for i in range(num_iterations)]
        else:
            outputs.extend(v)

    fd, values_path = tempfile.mkstemp()
    os.close(fd)
    write_yaml(values_path, parameters)
    iteration_cmd += ["--mapping", values_path]

    output = run_shell(" ".join(iteration_cmd))

    # Assert not allocated stderr.
    assert output[1] is None

    # Check for error keyword in stdout
    assert b"error" not in output[0]

    if len(parameters) == 0:
        # no effective mapping was suppiled
        # this should result in an error
        assert b"Error: Please check the provided mappings" in output[0]
        return

    # check whether parameters setting was effective
    for o in outputs:
        assert Path(o).resolve().exists()

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("provider", available_workflow_providers())
def test_workflow_iterate_command_with_parameter_set(runner, run_shell, project, capsys, client, provider):
    """Test executing a workflow with --set float value for a renku.ui.api.Parameter."""
    script = client.path / "script.py"
    output = client.path / "output"

    with client.commit():
        script.write_text("import sys\nprint(sys.argv[1])\n")

    result = run_shell(f"renku run --name run1 -- python {script} 3.98 > {output}")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    assert "3.98\n" == output.read_text()

    result = run_shell(f"renku workflow execute -p {provider} --set parameter-2=2.0 run1")

    # Assert not allocated stderr.
    assert result[1] is None

    assert "2.0\n" == output.read_text()

    result = run_shell(f"renku workflow iterate -p {provider} --map parameter-2=[0.1,0.3,0.5,0.8,0.95] run1")

    # Assert not allocated stderr.
    assert result[1] is None
    assert output.read_text() in [
        "0.1\n",
        "0.3\n",
        "0.5\n",
        "0.8\n",
        "0.95\n",
    ]

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_cycle_detection(run_shell, project, capsys, client):
    """Test creating a cycle is not possible with renku run or workflow execute."""
    input = client.path / "input"

    with client.commit():
        input.write_text("test")

    result = run_shell("renku run --name run1 -- cp input output")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    result = run_shell("renku run --name run2 -- wc output > input")

    assert b"Cycles detected in execution graph" in result[0]

    run_shell("git clean -fd && git reset --hard")

    result = run_shell("renku run --name run2 -- wc output > wordcount")

    # Assert expected empty stdout.
    assert b"" == result[0]
    # Assert not allocated stderr.
    assert result[1] is None

    result = run_shell("renku workflow execute  --set output-2=input run2")

    assert b"Cycles detected in execution graph" in result[0]


@pytest.mark.skipif(sys.platform == "darwin", reason="GitHub macOS image doesn't include Docker")
def test_workflow_execute_docker_toil(runner, client, run_shell, caplog):
    """Test workflow execute using docker with the toil provider."""
    caplog.set_level(logging.INFO)

    write_and_commit_file(client.repository, "input", "first line\nsecond line")
    output = client.path / "output"

    run_shell("renku run --name run-1 -- tail -n 1 input > output")

    assert "first line" not in output.read_text()

    write_and_commit_file(client.repository, "toil.yaml", "logLevel: INFO\ndocker:\n  image: ubuntu")

    result = runner.invoke(cli, ["workflow", "execute", "-p", "toil", "-s", "n-1=2", "-c", "toil.yaml", "run-1"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "first line" in output.read_text()
    assert "executing with Docker" in caplog.text


def test_workflow_execute_docker_toil_stderr(runner, client, run_shell):
    """Test workflow execute using docker with the toil provider and stderr redirection."""
    write_and_commit_file(client.repository, "input", "first line\nsecond line")
    output = client.path / "output"

    run_shell("renku run --name run-1 -- tail -n 1 input 2> output")

    assert "first line" not in output.read_text()

    write_and_commit_file(client.repository, "toil.yaml", "docker:\n  image: ubuntu")

    result = runner.invoke(cli, ["workflow", "execute", "-p", "toil", "-s", "n-1=2", "-c", "toil.yaml", "run-1"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "Cannot run workflows that have stdin or stderr redirection with Docker" in result.output


@pytest.mark.parametrize("provider", available_workflow_providers())
@pytest.mark.parametrize(
    "workflow, parameters, outputs",
    [
        (
            "touch foo",
            {"output-1": "{:%Y-%m-%d}"},
            [datetime.datetime.now().strftime("%Y-%m-%d")],
        )
    ],
)
def test_workflow_templated_params(runner, run_shell, client, capsys, workflow, parameters, provider, outputs):
    """Test executing a workflow with templated parameters."""
    workflow_name = "foobar"

    # Run a shell command with pipe.
    output = run_shell(f"renku run --name {workflow_name} {workflow}")
    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    execute_cmd = ["workflow", "execute", "-p", provider, workflow_name]
    [execute_cmd.extend(["--set", f"{k}={v}"]) for k, v in parameters.items()]
    _execute(capsys, runner, execute_cmd)

    for o in outputs:
        assert Path(o).resolve().exists()


def test_reverted_activity_status(client, runner, client_database_injection_manager):
    """Test that reverted activity doesn't affect status/update/log/etc."""
    input = client.path / "input"
    write_and_commit_file(client.repository, input, "content")
    output = client.path / "output"

    assert 0 == runner.invoke(cli, ["run", "cat", input], stdout=output).exit_code
    write_and_commit_file(client.repository, input, "changes")

    with client_database_injection_manager(client):
        activity_gateway = ActivityGateway()
        activity_id = activity_gateway.get_all_activities()[0].id

    assert 1 == runner.invoke(cli, ["status"]).exit_code
    assert "output" in runner.invoke(cli, ["update", "--all", "--dry-run"]).output
    assert "cat input > output" in runner.invoke(cli, ["workflow", "visualize", "output"]).output
    assert activity_id in runner.invoke(cli, ["log"]).output
    assert "input" in runner.invoke(cli, ["workflow", "inputs"]).output
    assert "output" in runner.invoke(cli, ["workflow", "outputs"]).output

    result = runner.invoke(cli, ["workflow", "revert", activity_id])

    assert 0 == result.exit_code, format_result_exception(result)

    assert 0 == runner.invoke(cli, ["status"]).exit_code
    assert "output" not in runner.invoke(cli, ["update", "--all", "--dry-run"]).output
    assert "cat input > output" not in runner.invoke(cli, ["workflow", "visualize", "output"]).output
    assert activity_id not in runner.invoke(cli, ["log"]).output
    assert "input" not in runner.invoke(cli, ["workflow", "inputs"]).output
    assert "output" not in runner.invoke(cli, ["workflow", "outputs"]).output
