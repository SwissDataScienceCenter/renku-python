# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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

from pathlib import Path

import pytest

from renku.core.management.workflow.plan_factory import PlanFactory


def test_1st_tool(client, client_database_injection_manager):
    """Check creation of 1st tool example from args."""
    with client_database_injection_manager(client):
        plan = PlanFactory(("echo", "Hello world!")).to_plan()

    assert "Hello world!" == plan.parameters[0].default_value


def test_03_input(client, client_database_injection_manager):
    """Check the essential input parameters."""
    whale = Path(client.path) / "whale.txt"
    whale.touch()

    client.repo.index.add([str(whale)])
    client.repo.index.commit("add whale.txt")

    argv = [
        "echo",
        "-f",
        "-i42",
        "--example-string",
        "hello",
        "--file=whale.txt",
    ]
    with client_database_injection_manager(client):
        plan = PlanFactory(argv, directory=client.path, working_dir=client.path).to_plan()

    assert ["-f"] == plan.parameters[0].to_argv()

    assert "42" == plan.parameters[1].default_value
    assert "-i" == plan.parameters[1].prefix

    assert "hello" == plan.parameters[2].default_value
    assert "--example-string " == plan.parameters[2].prefix

    assert plan.inputs[0].default_value == "whale.txt"
    assert "--file=" == plan.inputs[0].prefix

    assert argv == plan.to_argv()


def test_base_command_detection(client, client_database_injection_manager):
    """Test base command detection."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()

    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    with client_database_injection_manager(client):
        plan = PlanFactory(argv, directory=client.path, working_dir=client.path).to_plan()

    assert "tar xf" == plan.command
    assert plan.inputs[0].default_value == "hello.tar"
    assert plan.inputs[0].prefix is None

    assert argv == plan.to_argv()


def test_base_command_as_file_input(client, client_database_injection_manager):
    """Test base command detection when it is a script file."""
    cwd = Path(client.path)
    script = cwd / "script.py"
    script.touch()

    input_file = cwd / "input.csv"
    input_file.touch()

    client.repo.index.add([str(script), str(input_file)])
    client.repo.index.commit("add file")

    argv = ["script.py", "input.csv"]
    with client_database_injection_manager(client):
        plan = PlanFactory(argv, directory=client.path, working_dir=client.path).to_plan()

    assert not plan.command
    assert 2 == len(plan.inputs)


def test_short_base_command_detection(client, client_database_injection_manager):
    """Test base command detection without parameters."""
    with client_database_injection_manager(client):
        plan = PlanFactory(("echo", "A")).to_plan()

    assert "A" == plan.parameters[0].default_value
    assert ["echo", "A"] == plan.to_argv()


def test_04_output(client, client_database_injection_manager):
    """Test describtion of outputs from a command."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()

    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path)

    # simulate run

    output = Path(client.path) / "hello.txt"
    output.touch()

    factory.add_outputs([output])
    parameters = factory.outputs

    assert "hello.txt" == parameters[0].default_value

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_05_stdout(client, client_database_injection_manager):
    """Test stdout mapping."""
    output = Path(client.path) / "output.txt"
    output.touch()

    client.repo.index.add([str(output)])
    client.repo.index.commit("add output")

    argv = ["echo", "Hello world!"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path, stdout="output.txt")

    assert "output.txt" == factory.stdout
    factory.add_outputs(["output.txt"])
    assert "stdout" == factory.outputs[0].mapped_to.stream_type
    assert 2 == factory.outputs[0].position

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert ["echo", '"Hello world!"'] == plan.to_argv()


def test_stdout_with_conflicting_arg(client, client_database_injection_manager):
    """Test stdout with conflicting argument value."""
    output = Path(client.path) / "lalala"
    output.touch()

    client.repo.index.add([str(output)])
    client.repo.index.commit("add lalala")

    argv = ["echo", "lalala"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path, stdout="lalala")

    assert "lalala" == factory.parameters[0].default_value
    assert "lalala" == factory.stdout

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_06_params(client, client_database_injection_manager):
    """Test referencing input parameters in other fields."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()
    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar", "goodbye.txt"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path)

    assert "goodbye.txt" == factory.parameters[0].default_value

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_09_array_inputs(client, client_database_injection_manager):
    """Test specification of input parameters in arrays."""
    argv = [
        "echo",
        "-A",
        "one",
        "two",
        "three",
        "-B=four",
        "-B=five",
        "-B=six",
        "-C=seven,eight,nine",
    ]
    with client_database_injection_manager(client):
        plan = PlanFactory(argv, directory=client.path, working_dir=client.path).to_plan()

    assert "seven,eight,nine" == plan.parameters[-1].default_value
    assert "-C=" == plan.parameters[-1].prefix

    assert argv == plan.to_argv()


@pytest.mark.parametrize("argv", [["wc"], ["wc", "-l"]])
def test_stdin_and_stdout(argv, client, client_database_injection_manager):
    """Test stdout mapping."""
    input_ = Path(client.path) / "input.txt"
    input_.touch()
    output = Path(client.path) / "output.txt"
    output.touch()
    error = Path(client.path) / "error.log"
    error.touch()

    client.repo.index.add([str(input_), str(output), str(error)])
    client.repo.index.commit("add files")

    factory = PlanFactory(
        argv,
        directory=client.path,
        working_dir=client.path,
        stdin="input.txt",
        stdout="output.txt",
        stderr="error.log",
    )

    assert factory.stdin
    if len(argv) > 1:
        assert factory.parameters

    assert "output.txt" == factory.stdout
    factory.add_outputs(["output.txt", "error.log"])
    assert "stdout" == factory.outputs[0].mapped_to.stream_type

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert argv == plan.to_argv()
    min_pos = len(argv) - 1
    assert any(i.mapped_to and i.mapped_to.stream_type == "stdin" and i.position == min_pos + 1 for i in plan.inputs)
    assert any(
        o.position and o.mapped_to and o.mapped_to.stream_type == "stdout" and min_pos + 1 < o.position < min_pos + 4
        for o in plan.outputs
    )
    assert any(
        o.position and o.mapped_to and o.mapped_to.stream_type == "stderr" and min_pos + 1 < o.position < min_pos + 4
        for o in plan.outputs
    )


def test_input_directory(client, client_database_injection_manager):
    """Test input directory."""
    cwd = Path(client.path)
    src = cwd / "src"
    src.mkdir(parents=True)

    for i in range(5):
        (src / str(i)).touch()

    src_tar = cwd / "src.tar"
    src_tar.touch()

    client.repo.index.add([str(src), str(src_tar)])
    client.repo.index.commit("add file and folder")

    argv = ["tar", "czvf", "src.tar", "src"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path)

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    assert argv == plan.to_argv()

    inputs = sorted(plan.inputs, key=lambda x: x.position)

    assert src_tar.name == inputs[0].default_value
    assert inputs[1].default_value == src.name


@pytest.mark.skip("CWLConverter doesn't yet support new metadata, renable once it does")
def test_existing_output_directory(client, runner, project, client_database_injection_manager):
    """Test creation of InitialWorkDirRequirement for output."""
    from renku.core.management.workflow.converters.cwl import CWLConverter

    client.path = client.path
    output = client.path / "output"

    argv = ["script", "output"]
    factory = PlanFactory(argv, directory=client.path, working_dir=client.path)

    with factory.watch(client, no_output=True) as tool:
        # Script creates the directory.
        output.mkdir(parents=True)

    with client_database_injection_manager(client):
        plan = factory.to_plan()

    cwl, _ = CWLConverter.convert(plan, client.path)

    assert 1 == len([r for r in cwl.requirements if hasattr(r, "listing")])

    output.mkdir(parents=True, exist_ok=True)
    with factory.watch(client) as tool:
        # The directory already exists.
        (output / "result.txt").touch()

    assert 1 == len(tool.outputs)

    with client_database_injection_manager(client):
        plan = tool.to_plan()
    cwl, _ = CWLConverter.convert(plan, client.path)

    reqs = [r for r in cwl.requirements if hasattr(r, "listing")]

    assert 1 == len(reqs)
    assert output.name == reqs[0].listing[0].entryname
    assert 1 == len(tool.outputs)
