# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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

from renku.core.models.cwl.command_line_tool import CommandLineToolFactory
from renku.core.models.entities import Collection, Entity


def test_1st_tool(client):
    """Check creation of 1st tool example from args."""
    tool = CommandLineToolFactory(("echo", "Hello world!")).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan
    assert "Hello world!" == tool.arguments[0].value


def test_03_input(client):
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
    tool = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan

    assert ["-f"] == tool.arguments[0].to_argv()

    assert 42 == tool.arguments[1].value
    assert "-i" == tool.arguments[1].prefix

    assert "hello" == tool.arguments[2].value
    assert "--example-string " == tool.arguments[2].prefix

    assert tool.inputs[0].consumes.path == "whale.txt"
    assert isinstance(tool.inputs[0].consumes, Entity)
    assert not isinstance(tool.inputs[0].consumes, Collection)
    assert "--file=" == tool.inputs[0].prefix

    assert argv == tool.to_argv()


def test_base_command_detection(client):
    """Test base command detection."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()

    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    tool = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan

    assert "tar xf" == tool.command
    assert tool.inputs[0].consumes.path == "hello.tar"
    assert isinstance(tool.inputs[0].consumes, Entity)
    assert not isinstance(tool.inputs[0].consumes, Collection)
    assert tool.inputs[0].prefix is None

    assert argv == tool.to_argv()


def test_base_command_as_file_input(client):
    """Test base command detection when it is a script file."""
    cwd = Path(client.path)
    script = cwd / "script.py"
    script.touch()

    input_file = cwd / "input.csv"
    input_file.touch()

    client.repo.index.add([str(script), str(input_file)])
    client.repo.index.commit("add file")

    argv = ["script.py", "input.csv"]
    tool = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path,).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan

    assert not tool.command
    assert 2 == len(tool.inputs)


def test_short_base_command_detection(client):
    """Test base command detection without arguments."""
    tool = CommandLineToolFactory(("echo", "A")).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan

    assert "A" == tool.arguments[0].value
    assert ["echo", "A"] == tool.to_argv()


def test_04_output(client):
    """Test describtion of outputs from a command."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()

    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path)

    # simulate run

    output = Path(client.path) / "hello.txt"
    output.touch()

    parameters = list(factory.guess_outputs([output]))

    assert "File" == parameters[0][0].type
    assert "hello.txt" == parameters[0][0].outputBinding.glob

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()


def test_05_stdout(client):
    """Test stdout mapping."""
    output = Path(client.path) / "output.txt"
    output.touch()

    client.repo.index.add([str(output)])
    client.repo.index.commit("add output")

    argv = ["echo", "Hello world!"]
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path, stdout="output.txt",)

    assert "output.txt" == factory.stdout
    assert "stdout" == factory.outputs[0].type

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()


def test_stdout_with_conflicting_arg(client):
    """Test stdout with conflicting argument value."""
    output = Path(client.path) / "lalala"
    output.touch()

    client.repo.index.add([str(output)])
    client.repo.index.commit("add lalala")

    argv = ["echo", "lalala"]
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path, stdout="lalala",)

    assert "lalala" == factory.inputs[0].default
    assert "string" == factory.inputs[0].type
    assert "lalala" == factory.stdout
    assert "stdout" == factory.outputs[0].type

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()


def test_06_params(client):
    """Test referencing input parameters in other fields."""
    hello = Path(client.path) / "hello.tar"
    hello.touch()
    client.repo.index.add([str(hello)])
    client.repo.index.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar", "goodbye.txt"]
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path,)

    assert "goodbye.txt" == factory.inputs[1].default
    assert "string" == factory.inputs[1].type
    assert 2 == factory.inputs[1].inputBinding.position

    goodbye_id = factory.inputs[1].id

    # simulate run

    output = Path(client.path) / "goodbye.txt"
    output.touch()

    parameters = list(factory.guess_outputs([output]))

    assert "File" == parameters[0][0].type
    assert "$(inputs.{0})".format(goodbye_id) == parameters[0][0].outputBinding.glob

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()


def test_09_array_inputs(client):
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
    tool = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path).generate_process_run(
        client=client, commit=client.repo.head.commit, path="dummy.yaml"
    )

    tool = tool.association.plan

    assert "seven,eight,nine" == tool.arguments[-1].value
    assert "-C=" == tool.arguments[-1].prefix

    assert argv == tool.to_argv()


@pytest.mark.parametrize("argv", [["wc"], ["wc", "-l"]])
def test_stdin_and_stdout(argv, client):
    """Test stdout mapping."""
    input_ = Path(client.path) / "input.txt"
    input_.touch()
    output = Path(client.path) / "output.txt"
    output.touch()
    error = Path(client.path) / "error.log"
    error.touch()

    client.repo.index.add([str(input_), str(output), str(error)])
    client.repo.index.commit("add files")

    factory = CommandLineToolFactory(
        argv,
        directory=client.path,
        working_dir=client.path,
        stdin="input.txt",
        stdout="output.txt",
        stderr="error.log",
    )

    assert factory.stdin
    if len(argv) > 1:
        assert factory.arguments

    assert "output.txt" == factory.stdout
    assert "stdout" == factory.outputs[0].type

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()
    assert any(i.mapped_to and i.mapped_to.stream_type == "stdin" for i in tool.inputs)
    assert any(o.mapped_to and o.mapped_to.stream_type == "stdout" for o in tool.outputs)
    assert any(o.mapped_to and o.mapped_to.stream_type == "stderr" for o in tool.outputs)


def test_input_directory(client):
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
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path,)

    tool = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    tool = tool.association.plan
    assert argv == tool.to_argv()

    inputs = sorted(tool.inputs, key=lambda x: x.position)

    assert src_tar.name == inputs[0].consumes.path
    assert isinstance(inputs[0].consumes, Entity)
    assert not isinstance(inputs[0].consumes, Collection)
    assert inputs[1].consumes.path == src.name
    assert isinstance(inputs[1].consumes, Collection)


def test_existing_output_directory(client, runner, project):
    """Test creation of InitialWorkDirRequirement for output."""
    from renku.core.models.workflow.converters.cwl import CWLConverter

    client.path = client.path
    output = client.path / "output"

    argv = ["script", "output"]
    factory = CommandLineToolFactory(argv, directory=client.path, working_dir=client.path,)

    with factory.watch(client, no_output=True) as tool:
        # Script creates the directory.
        output.mkdir(parents=True)

    run = factory.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")

    cwl, _ = CWLConverter.convert(run.association.plan, client)

    assert 1 == len([r for r in cwl.requirements if hasattr(r, "listing")])

    output.mkdir(parents=True, exist_ok=True)
    with factory.watch(client) as tool:
        # The directory already exists.
        (output / "result.txt").touch()

    assert 1 == len(tool.inputs)

    run = tool.generate_process_run(client=client, commit=client.repo.head.commit, path="dummy.yaml")
    cwl, _ = CWLConverter.convert(run.association.plan, client)

    reqs = [r for r in cwl.requirements if hasattr(r, "listing")]

    assert 1 == len(reqs)
    assert output.name == reqs[0].listing[0].entryname
    assert 1 == len(tool.outputs)
