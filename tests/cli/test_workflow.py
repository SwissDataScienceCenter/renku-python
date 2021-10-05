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

import itertools
import os
import tempfile
from pathlib import Path

import pexpect
import pyte
import pytest
from cwl_utils import parser_v1_2 as cwlgen

from renku.cli import cli
from renku.core.metadata.database import Database
from renku.core.models.jsonld import write_yaml
from renku.core.plugins.provider import available_workflow_providers
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
    """test workflow remove with builder."""
    workflow_name = "test_workflow"

    result = runner.invoke(cli, ["workflow", "remove", workflow_name])
    assert 2 == result.exit_code

    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", workflow_name, "echo", "foo"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "remove", "--force", workflow_name])
    assert 0 == result.exit_code, format_result_exception(result)


def test_workflow_export_command(runner, project):
    """test workflow export with builder."""
    result = runner.invoke(cli, ["run", "--success-code", "0", "--no-output", "--name", "run1", "touch", "data.csv"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "export", "run1", "-o", "run1.cwl"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert Path("run1.cwl").exists()

    workflow = cwlgen.load_document("run1.cwl")
    assert workflow.baseCommand[0] == "touch"
    assert len(workflow.inputs) == 3
    assert len(workflow.outputs) == 1


def test_workflow_edit(runner, client, run_shell):
    """Test naming of CWL tools and workflows."""

    def _get_plan_id(output):
        return output.split("\n")[0].split(":")[1].strip()

    workflow_name = "run1"
    result = runner.invoke(cli, ["run", "--name", workflow_name, "touch", "data.txt"])
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
        (
            [("run1", "touch data.csv"), ("run2", "wc data.csv > output")],
            {"run1": {"outputs": ["foo"]}, "run2": {"inputs": ["foo"], "outputs": ["bar"]}},
        ),
    ],
)
def test_workflow_execute_command(runner, run_shell, project, capsys, client, provider, yaml, workflows, parameters):
    """test workflow execute."""

    for wf in workflows:
        output = run_shell(f"renku run --name {wf[0]} -- {wf[1]}")
        # Assert expected empty stdout.
        assert b"" == output[0]
        # Assert not allocated stderr.
        assert output[1] is None

    def _execute(args):
        with capsys.disabled():
            try:
                cli.main(
                    args=args,
                    prog_name=runner.get_default_prog_name(cli),
                )
            except SystemExit as e:
                assert e.code in {None, 0}

    if not parameters:
        for wf in workflows:
            execute_cmd = ["workflow", "execute", "-p", provider, wf[0]]
            _execute(execute_cmd)
    else:
        database = Database.from_path(client.database_path)
        for wf in workflows:
            if wf[0] in parameters:
                plan = database["plans-by-name"][wf[0]]
                execute_cmd = ["workflow", "execute", "-p", provider]

                overrides = dict()
                for k, values in parameters[wf[0]].items():
                    for i, v in enumerate(values):
                        overrides[getattr(plan, k)[i].name] = v

                if yaml:
                    fd, values_path = tempfile.mkstemp()
                    os.close(fd)
                    write_yaml(values_path, overrides)
                    execute_cmd += ["--values", values_path]
                else:
                    [execute_cmd.extend(["--set", f"{k}={v}"]) for k, v in overrides.items()]

                execute_cmd.append(wf[0])

                _execute(execute_cmd)

                # check whether parameters setting was effective
                if "outputs" in parameters[wf[0]]:
                    for o in parameters[wf[0]]["outputs"]:
                        assert Path(o).resolve().exists()


def test_workflow_visualize_non_interactive(runner, project, client, workflow_graph):
    """Test renku workflow visualize in non-interactive mode."""

    # We don't use pytest paramtrization for performance reasons, so we don't need to build the workflow_graph fixture
    # for each execution
    columns = [[], ["-c", "command"], ["-c", "command,id,date"]]
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

    result = runner.invoke(cli, ["workflow", "visualize", "--no-pager", "-x", "-a", "--no-color"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "A" in result.output
    assert "J" in result.output
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
