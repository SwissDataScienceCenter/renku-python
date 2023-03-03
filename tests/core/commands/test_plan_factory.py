#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Test plan factory."""

from pathlib import Path

import pytest

from renku.core.workflow.plan_factory import PlanFactory


def test_1st_tool(project, with_injection):
    """Check creation of 1st tool example from args."""
    with with_injection():
        plan = PlanFactory(("echo", "Hello world!")).to_plan()

    assert "Hello world!" == plan.parameters[0].default_value


def test_03_input(project, with_injection):
    """Check the essential input parameters."""
    whale = Path(project.path) / "whale.txt"
    whale.touch()

    project.repository.add(whale)
    project.repository.commit("add whale.txt")

    argv = [
        "echo",
        "-f",
        "-i42",
        "--example-string",
        "hello",
        "--file=whale.txt",
    ]
    with with_injection():
        plan = PlanFactory(argv, directory=project.path, working_dir=project.path).to_plan()

    assert ["-f"] == plan.parameters[0].to_argv()

    assert "42" == plan.parameters[1].default_value
    assert "-i" == plan.parameters[1].prefix

    assert "hello" == plan.parameters[2].default_value
    assert "--example-string " == plan.parameters[2].prefix

    assert plan.inputs[0].default_value == "whale.txt"
    assert "--file=" == plan.inputs[0].prefix

    assert argv == plan.to_argv()


def test_base_command_detection(project, with_injection):
    """Test base command detection."""
    hello = Path(project.path) / "hello.tar"
    hello.touch()

    project.repository.add(hello)
    project.repository.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    with with_injection():
        plan = PlanFactory(argv, directory=project.path, working_dir=project.path).to_plan()

    assert "tar xf" == plan.command
    assert plan.inputs[0].default_value == "hello.tar"
    assert plan.inputs[0].prefix is None

    assert argv == plan.to_argv()


def test_base_command_as_file_input(project, with_injection):
    """Test base command detection when it is a script file."""
    cwd = Path(project.path)
    script = cwd / "script.py"
    script.touch()

    input_file = cwd / "input.csv"
    input_file.touch()

    project.repository.add(script, input_file)
    project.repository.commit("add file")

    argv = ["script.py", "input.csv"]
    with with_injection():
        plan = PlanFactory(argv, directory=project.path, working_dir=project.path).to_plan()

    assert not plan.command
    assert 2 == len(plan.inputs)


def test_short_base_command_detection(project, with_injection):
    """Test base command detection without parameters."""
    with with_injection():
        plan = PlanFactory(("echo", "A")).to_plan()

    assert "A" == plan.parameters[0].default_value
    assert ["echo", "A"] == plan.to_argv()


def test_04_output(project, with_injection):
    """Test description of outputs from a command."""
    hello = Path(project.path) / "hello.tar"
    hello.touch()

    project.repository.add(hello)
    project.repository.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path)

    # simulate run

    output = Path(project.path) / "hello.txt"
    output.touch()

    factory.add_outputs([(output, None)])
    parameters = factory.outputs

    assert "hello.txt" == parameters[0].default_value

    with with_injection():
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_05_stdout(project, with_injection):
    """Test stdout mapping."""
    output = Path(project.path) / "output.txt"
    output.touch()

    project.repository.add(output)
    project.repository.commit("add output")

    argv = ["echo", "Hello world!"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path, stdout="output.txt")

    assert "output.txt" == factory.stdout
    factory.add_outputs([("output.txt", None)])
    assert "stdout" == factory.outputs[0].mapped_to.stream_type
    assert 2 == factory.outputs[0].position

    with with_injection():
        plan = factory.to_plan()

    assert ["echo", "'Hello world!'"] == plan.to_argv()


def test_stdout_with_conflicting_arg(project, with_injection):
    """Test stdout with conflicting argument value."""
    output = Path(project.path) / "lalala"
    output.touch()

    project.repository.add(output)
    project.repository.commit("add lalala")

    argv = ["echo", "lalala"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path, stdout="lalala")

    assert "lalala" == factory.parameters[0].default_value
    assert "lalala" == factory.stdout

    with with_injection():
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_06_params(project, with_injection):
    """Test referencing input parameters in other fields."""
    hello = Path(project.path) / "hello.tar"
    hello.touch()
    project.repository.add(hello)
    project.repository.commit("add hello.tar")

    argv = ["tar", "xf", "hello.tar", "goodbye.txt"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path)

    assert "goodbye.txt" == factory.parameters[0].default_value

    with with_injection():
        plan = factory.to_plan()

    assert argv == plan.to_argv()


def test_09_array_inputs(project, with_injection):
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
    with with_injection():
        plan = PlanFactory(argv, directory=project.path, working_dir=project.path).to_plan()

    assert "seven,eight,nine" == plan.parameters[-1].default_value
    assert "-C=" == plan.parameters[-1].prefix

    assert argv == plan.to_argv()


@pytest.mark.parametrize("argv", [["wc"], ["wc", "-l"]])
def test_stdin_and_stdout(argv, project, with_injection):
    """Test stdout mapping."""
    input = project.path / "input.txt"
    input.touch()
    output = project.path / "output.txt"
    output.touch()
    error = project.path / "error.txt"
    error.touch()

    project.repository.add(input, output, error)
    project.repository.commit("add files")

    factory = PlanFactory(
        argv,
        directory=project.path,
        working_dir=project.path,
        stdin="input.txt",
        stdout="output.txt",
        stderr="error.txt",
    )

    assert factory.stdin
    if len(argv) > 1:
        assert factory.parameters

    assert "output.txt" == factory.stdout
    factory.add_outputs([("output.txt", None), ("error.txt", None)])
    assert "stdout" == factory.outputs[0].mapped_to.stream_type

    with with_injection():
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


def test_input_directory(project, with_injection):
    """Test input directory."""
    cwd = Path(project.path)
    src = cwd / "src"
    src.mkdir(parents=True)

    for i in range(5):
        (src / str(i)).touch()

    src_tar = cwd / "src.tar"
    src_tar.touch()

    project.repository.add(src, src_tar)
    project.repository.commit("add file and folder")

    argv = ["tar", "czvf", "src.tar", "src"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path)

    with with_injection():
        plan = factory.to_plan()

    assert argv == plan.to_argv()

    inputs = sorted(plan.inputs, key=lambda x: x.position)

    assert src_tar.name == inputs[0].default_value
    assert inputs[1].default_value == src.name


@pytest.mark.skip("CWLConverter doesn't yet support new metadata, re-enable once it does")
def test_existing_output_directory(runner, project, with_injection):
    """Test creation of InitialWorkDirRequirement for output."""
    from renku.core.workflow.converters.cwl import CWLConverter

    project.path = project.path
    output = project.path / "output"

    argv = ["script", "output"]
    factory = PlanFactory(argv, directory=project.path, working_dir=project.path)

    with factory.watch(no_output=True):
        # Script creates the directory.
        output.mkdir(parents=True)

    with with_injection():
        plan = factory.to_plan()

    cwl, _ = CWLConverter.convert(plan, project.path)

    assert 1 == len([r for r in cwl.requirements if hasattr(r, "listing")])

    output.mkdir(parents=True, exist_ok=True)
    with factory.watch() as tool:
        # The directory already exists.
        (output / "result.txt").touch()

    assert 1 == len(tool.outputs)

    with with_injection():
        plan = tool.to_plan()
    cwl, _ = CWLConverter.convert(plan, project.path)

    requirements = [r for r in cwl.requirements if hasattr(r, "listing")]

    assert 1 == len(requirements)
    assert output.name == requirements[0].listing[0].entryname
    assert 1 == len(tool.outputs)
