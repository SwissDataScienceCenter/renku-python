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
"""Tests for workflow file."""

import functools
import re
import shutil
import textwrap
from pathlib import Path

import pytest

import renku.core.workflow.workflow_file
from renku.core import errors
from renku.core.plugin.workflow_file_parser import get_available_workflow_file_parsers, read_workflow_file
from renku.core.util.yaml import load_yaml
from renku.core.workflow.parser.renku import convert_to_workflow_file
from renku.core.workflow.workflow_file import Input, Output, Parameter, run_workflow_file


def test_load_valid_renku_workflow_file():
    """Test loading a valid workflow file."""
    path = Path(__file__).parent / ".." / ".." / "data" / "workflow-file.yml"

    workflow = read_workflow_file(path)

    assert "workflow-file" == workflow.name
    assert "workflow-file" == workflow.qualified_name
    assert {"workflow file", "v1"} == set(workflow.keywords)
    assert "A sample workflow file used for testing" == workflow.description
    assert ["head", "tail", "line-count"] == [s.name for s in workflow.steps]
    assert ["workflow-file.head", "workflow-file.tail", "workflow-file.line-count"] == [
        s.qualified_name for s in workflow.steps
    ]
    assert "head" == workflow.steps[0].command
    assert "tail" == workflow.steps[1].command

    assert ["preprocessing", "first step"] == workflow.steps[0].keywords
    assert [0, 127] == workflow.steps[0].success_codes
    assert "temporary-result" == workflow.steps[0].outputs[0].name
    assert "intermediate" == workflow.steps[0].outputs[0].path
    assert workflow.steps[0].outputs[0].persist is False
    assert "temporary intermediate result that won't be saved" == workflow.steps[0].outputs[0].description

    assert "intermediate" == workflow.steps[1].inputs[0].name
    assert "intermediate" == workflow.steps[1].inputs[0].path

    assert "models-and-colors" == workflow.steps[1].outputs[0].name
    assert "results/output.csv" == workflow.steps[1].outputs[0].path
    assert workflow.steps[1].outputs[0].persist is True


def test_load_valid_renku_workflow_file_simple():
    """Test loading a valid workflow file with a simple format."""
    path = Path(__file__).parent / ".." / ".." / "data" / "workflow-file-simple.yml"

    workflow = read_workflow_file(path)

    assert "workflow-file" == workflow.name
    assert {"workflow file", "v1"} == set(workflow.keywords)
    assert "A sample workflow file used for testing" == workflow.description
    assert ["head", "tail", "line-count"] == [s.name for s in workflow.steps]

    assert {"first step", "preprocessing"} == set(workflow.steps[0].keywords)

    assert workflow.steps[0].outputs[0].description is None
    assert ["data/collection/models.csv", "data/collection/colors.csv"] == [i.path for i in workflow.steps[0].inputs]
    assert ["intermediate"] == [o.path for o in workflow.steps[0].outputs]
    assert ["-n", "10"] == [p.value for p in workflow.steps[0].parameters]

    assert ["intermediate"] == [i.path for i in workflow.steps[1].inputs]

    assert ["results/output.csv"] == [o.path for o in workflow.steps[1].outputs]
    assert ["-n", "5"] == [p.value for p in workflow.steps[1].parameters]


def test_parse_position_in_renku_workflow_file():
    """Test parsing correct position for a command's parameters."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: head -n 10 data/collection/models.csv data/collection/colors.csv > intermediate
            inputs:
              models:
                path: data/collection/models.csv
              colors:
                path: data/collection/colors.csv
            outputs:
              temporary-result:
                path: intermediate
            parameters:
              n:
                prefix: -n
                value: 10
        """
    )

    workflow = convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    step = workflow.steps[0]

    assert 1 == step.parameters[0].position
    assert 2 == next(i for i in step.inputs if i.path.endswith("models.csv")).position
    assert 3 == next(i for i in step.inputs if i.path.endswith("colors.csv")).position
    assert 4 == step.outputs[0].position

    assert "stdout" == step.outputs[0].mapped_to


def test_step_command_parser():
    """Test parsing command of a workflow file step."""
    Step = functools.partial(renku.core.workflow.workflow_file.Step, path="", workflow_file_name="wff")

    step = Step(name="test", command="renku dataset ls-files")

    assert "renku dataset ls-files" == step.command

    step = Step(
        name="test",
        command="./script -n 42 --input input.csv",
        inputs=[Input(path="./script"), Input(path="input.csv", prefix="--input")],
        parameters=[Parameter(prefix="-n", value=42)],
    )

    assert "" == step.command
    assert 1 == step.inputs[0].position
    assert step.inputs[0].prefix is None
    assert "./script" == step.inputs[0].path
    assert 3 == step.inputs[1].position
    assert "--input " == step.inputs[1].prefix  # ' ' is appended to the prefix
    assert "input.csv" == step.inputs[1].path

    assert 2 == step.parameters[0].position
    assert "-n " == step.parameters[0].prefix
    assert 42 == step.parameters[0].value

    step = Step(
        name="test",
        command="/system/command sub sub-subcommand -n 42 -n 43 --input input.csv --values={1,2,3} -o output 2> errors",
        inputs=[Input(path="input.csv", prefix="--input")],
        parameters=[
            Parameter(prefix="-n", value=43),
            Parameter(prefix="-n", value=42),
            Parameter(prefix="--values", value="{1,2,3}"),
        ],
        outputs=[Output(prefix="-o", path="output"), Output(path="errors")],
    )

    assert "/system/command sub sub-subcommand" == step.command

    assert 3 == step.inputs[0].position
    assert "--input " == step.inputs[0].prefix  # ' ' is appended to the prefix
    assert "input.csv" == step.inputs[0].path

    assert 2 == step.parameters[0].position
    assert "-n " == step.parameters[0].prefix
    assert 43 == step.parameters[0].value
    assert 1 == step.parameters[1].position
    assert "-n " == step.parameters[1].prefix
    assert 42 == step.parameters[1].value
    assert 4 == step.parameters[2].position
    assert "--values=" == step.parameters[2].prefix  # '=' is appended to the prefix
    assert "{1,2,3}" == step.parameters[2].value

    assert 5 == step.outputs[0].position
    assert "-o " == step.outputs[0].prefix
    assert "output" == step.outputs[0].path
    assert 6 == step.outputs[1].position
    assert step.outputs[1].prefix is None
    assert "errors" == step.outputs[1].path
    assert "stderr" == step.outputs[1].mapped_to

    # Test various forms of parameters prefix and value: 1: Without separator, 2: With separator
    step = Step(
        name="test",
        command="command -no-value no-dash-equal-1=v1 no-dash-equal-2=42 "
        "-single-dash-space-1 v2 -single-dash-space-2 43 -single-dash-equal-1=v3 -single-dash-equal-2=44 "
        "--double-dash-space-1 v4 --double-dash-space-2 42.1 --double-dash-equal-1=v5 --double-dash-equal-2=42.2",
        parameters=[
            Parameter(prefix="no-dash-equal-1", value="v1"),
            Parameter(prefix="no-dash-equal-2=", value=42),
            Parameter(prefix="-single-dash-space-1", value="v2"),
            Parameter(prefix="-single-dash-space-2 ", value=43),
            Parameter(prefix="-single-dash-equal-1", value="v3"),
            Parameter(prefix="-single-dash-equal-2=", value=44),
            Parameter(prefix="--double-dash-space-1", value="v4"),
            Parameter(prefix="--double-dash-space-2 ", value=42.1),
            Parameter(prefix="--double-dash-equal-1", value="v5"),
            Parameter(prefix="--double-dash-equal-2=", value=42.2),
            Parameter(value="-no-value"),
        ],
    )

    assert "command" == step.command

    assert "no-dash-equal-1=" == step.parameters[0].prefix
    assert "v1" == step.parameters[0].value

    assert "no-dash-equal-2=" == step.parameters[1].prefix
    assert 42 == step.parameters[1].value

    assert "-single-dash-space-1 " == step.parameters[2].prefix
    assert "v2" == step.parameters[2].value

    assert "-single-dash-space-2 " == step.parameters[3].prefix
    assert 43 == step.parameters[3].value

    assert "-single-dash-equal-1=" == step.parameters[4].prefix
    assert "v3" == step.parameters[4].value

    assert "-single-dash-equal-2=" == step.parameters[5].prefix
    assert 44 == step.parameters[5].value

    assert "--double-dash-space-1 " == step.parameters[6].prefix
    assert "v4" == step.parameters[6].value

    assert "--double-dash-space-2 " == step.parameters[7].prefix
    assert 42.1 == step.parameters[7].value

    assert "--double-dash-equal-1=" == step.parameters[8].prefix
    assert "v5" == step.parameters[8].value

    assert "--double-dash-equal-2=" == step.parameters[9].prefix
    assert 42.2 == step.parameters[9].value

    assert step.parameters[10].prefix is None
    assert "-no-value" == step.parameters[10].value

    # Test various forms of parameters value
    step = Step(
        name="test",
        command="command -no-value no-dash-equal-1=v1 no-dash-equal-2=42 "
        "-single-dash-space-1 v2 -single-dash-space-2 43 -single-dash-equal-1=v3 -single-dash-equal-2=44 "
        "--double-dash-space-1 v4 --double-dash-space-2 42.1 --double-dash-equal-1=v5 --double-dash-equal-2=42.2",
        parameters=[
            Parameter(value="no-dash-equal-1=v1"),
            Parameter(value="no-dash-equal-2=42"),
            Parameter(value="-single-dash-space-1 v2"),
            Parameter(value="-single-dash-space-2 43"),
            Parameter(value="-single-dash-equal-1=v3"),
            Parameter(value="-single-dash-equal-2=44"),
            Parameter(value="--double-dash-space-1 v4"),
            Parameter(value="--double-dash-space-2 42.1"),
            Parameter(value="--double-dash-equal-1=v5"),
            Parameter(value="--double-dash-equal-2=42.2"),
            Parameter(value="-no-value"),
        ],
    )

    assert step.parameters[0].prefix is None
    assert "no-dash-equal-1=v1" == step.parameters[0].value

    assert step.parameters[1].prefix is None
    assert "no-dash-equal-2=42" == step.parameters[1].value

    assert step.parameters[2].prefix is None
    assert "-single-dash-space-1 v2" == step.parameters[2].value

    assert step.parameters[3].prefix is None
    assert "-single-dash-space-2 43" == step.parameters[3].value

    assert step.parameters[4].prefix is None
    assert "-single-dash-equal-1=v3" == step.parameters[4].value

    assert step.parameters[5].prefix is None
    assert "-single-dash-equal-2=44" == step.parameters[5].value

    assert step.parameters[6].prefix is None
    assert "--double-dash-space-1 v4" == step.parameters[6].value

    assert step.parameters[7].prefix is None
    assert "--double-dash-space-2 42.1" == step.parameters[7].value

    assert step.parameters[8].prefix is None
    assert "--double-dash-equal-1=v5" == step.parameters[8].value

    assert step.parameters[9].prefix is None
    assert "--double-dash-equal-2=42.2" == step.parameters[9].value

    assert step.parameters[10].prefix is None
    assert "-no-value" == step.parameters[10].value

    # Test various forms of inputs/outputs
    step = Step(
        name="test",
        command="command no-prefix no-dash-equal-1=v1 no-dash-equal-2=42 "
        "-single-dash-space-1 v2 -single-dash-space-2 43 -single-dash-equal-1=v3 -single-dash-equal-2=44 "
        "--double-dash-space-1 v4 --double-dash-space-2 42.1 --double-dash-equal-1=v5 --double-dash-equal-2=42.2",
        inputs=[
            Input(prefix="no-dash-equal-1", path="v1"),
            Input(prefix="no-dash-equal-2=", path="42"),
            Input(prefix="-single-dash-space-1", path="v2"),
            Input(prefix="-single-dash-space-2 ", path="43"),
            Input(prefix="-single-dash-equal-1", path="v3"),
            Input(prefix="-single-dash-equal-2=", path="44"),
            Input(prefix="--double-dash-space-1", path="v4"),
            Input(prefix="--double-dash-space-2 ", path="42.1"),
            Input(prefix="--double-dash-equal-1", path="v5"),
            Input(prefix="--double-dash-equal-2=", path="42.2"),
            Input(path="no-prefix"),
        ],
    )

    assert "command" == step.command

    assert "no-dash-equal-1=" == step.inputs[0].prefix
    assert "v1" == step.inputs[0].path

    assert "no-dash-equal-2=" == step.inputs[1].prefix
    assert "42" == step.inputs[1].path

    assert "-single-dash-space-1 " == step.inputs[2].prefix
    assert "v2" == step.inputs[2].path

    assert "-single-dash-space-2 " == step.inputs[3].prefix
    assert "43" == step.inputs[3].path

    assert "-single-dash-equal-1=" == step.inputs[4].prefix
    assert "v3" == step.inputs[4].path

    assert "-single-dash-equal-2=" == step.inputs[5].prefix
    assert "44" == step.inputs[5].path

    assert "--double-dash-space-1 " == step.inputs[6].prefix
    assert "v4" == step.inputs[6].path

    assert "--double-dash-space-2 " == step.inputs[7].prefix
    assert "42.1" == step.inputs[7].path

    assert "--double-dash-equal-1=" == step.inputs[8].prefix
    assert "v5" == step.inputs[8].path

    assert "--double-dash-equal-2=" == step.inputs[9].prefix
    assert "42.2" == step.inputs[9].path

    assert step.inputs[10].prefix is None
    assert "no-prefix" == step.inputs[10].path

    step = Step(
        name="test",
        command="command -no-value no-dash-equal-1=v1 no-dash-equal-2=42 "
        "-single-dash-space-1 v2 -single-dash-space-2 43 -single-dash-equal-1=v3 -single-dash-equal-2=44 "
        "--double-dash-space-1 v4 --double-dash-space-2 42.1 --double-dash-equal-1=v5 --double-dash-equal-2=42.2",
        inputs=[
            Input(path="no-dash-equal-1=v1"),
            Input(path="no-dash-equal-2=42"),
            Input(path="-single-dash-space-1 v2"),
            Input(path="-single-dash-space-2 43"),
            Input(path="-single-dash-equal-1=v3"),
            Input(path="-single-dash-equal-2=44"),
            Input(path="--double-dash-space-1 v4"),
            Input(path="--double-dash-space-2 42.1"),
            Input(path="--double-dash-equal-1=v5"),
            Input(path="--double-dash-equal-2=42.2"),
            Input(path="-no-value"),
        ],
    )

    assert step.inputs[0].prefix is None
    assert "no-dash-equal-1=v1" == step.inputs[0].path

    assert step.inputs[1].prefix is None
    assert "no-dash-equal-2=42" == step.inputs[1].path

    assert step.inputs[2].prefix is None
    assert "-single-dash-space-1 v2" == step.inputs[2].path

    assert step.inputs[3].prefix is None
    assert "-single-dash-space-2 43" == step.inputs[3].path

    assert step.inputs[4].prefix is None
    assert "-single-dash-equal-1=v3" == step.inputs[4].path

    assert step.inputs[5].prefix is None
    assert "-single-dash-equal-2=44" == step.inputs[5].path

    assert step.inputs[6].prefix is None
    assert "--double-dash-space-1 v4" == step.inputs[6].path

    assert step.inputs[7].prefix is None
    assert "--double-dash-space-2 42.1" == step.inputs[7].path

    assert step.inputs[8].prefix is None
    assert "--double-dash-equal-1=v5" == step.inputs[8].path

    assert step.inputs[9].prefix is None
    assert "--double-dash-equal-2=42.2" == step.inputs[9].path

    assert step.inputs[10].prefix is None
    assert "-no-value" == step.inputs[10].path


def test_validation_error_invalid_names():
    """Test validating a workflow file with invalid names raises desired errors."""
    WorkflowFile = functools.partial(renku.core.workflow.workflow_file.WorkflowFile, path="")
    Step = functools.partial(renku.core.workflow.workflow_file.Step, command="ls", path="", workflow_file_name="wff")

    with pytest.raises(errors.ParseError, match="Workflow file name is invalid"):
        WorkflowFile(name="name with space", steps=[])

    with pytest.raises(errors.ParseError, match="Step name is invalid"):
        WorkflowFile(name="workflow", steps=[Step(name="has space")])

    with pytest.raises(errors.ParseError, match="Step 'same-name' cannot have the same name as the workflow file"):
        WorkflowFile(name="same-name", steps=[Step(name="same-name")])

    with pytest.raises(errors.ParseError, match="Parameter name in step 'step' is invalid: '--invalid'"):
        WorkflowFile(name="workflow", steps=[Step(name="step", parameters=[Parameter(name="--invalid", value=42)])])

    with pytest.raises(errors.ParseError, match="are reserved and cannot be used as parameter name in step 'step'"):
        WorkflowFile(name="workflow", steps=[Step(name="step", parameters=[Parameter(name="inputs", value=42)])])
    with pytest.raises(errors.ParseError, match="are reserved and cannot be used as parameter name in step 'step'"):
        WorkflowFile(name="workflow", steps=[Step(name="step", parameters=[Parameter(name="outputs", value=42)])])
    with pytest.raises(errors.ParseError, match="are reserved and cannot be used as parameter name in step 'step'"):
        WorkflowFile(name="workflow", steps=[Step(name="step", parameters=[Parameter(name="parameters", value=42)])])

    with pytest.raises(errors.ParseError, match="Duplicate input, output or parameter names found: same-1, same-2"):
        WorkflowFile(
            name="workflow",
            steps=[
                Step(
                    name="step",
                    parameters=[Parameter(name="same-1", value=42)],
                    inputs=[Input(name="same-1", path="42"), Input(name="same-2", path="42")],
                    outputs=[Output(name="same-2", path="42")],
                )
            ],
        )


def test_parse_error_workflow_file_with_no_step():
    """Test error raised when no step is defined in a workflow file."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps: []
        """
    )

    with pytest.raises(errors.ParseError, match="Workflow file must have at least one step"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_parse_error_invalid_attributes():
    """Test parse errors for invalid attributes."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        invalid-1: should not be here
        invalid-2: should not be here
        """
    )

    with pytest.raises(errors.ParseError, match="Invalid attributes for workflow file: invalid-1, invalid-2"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: echo running
            invalid: should not be here
        """
    )

    with pytest.raises(errors.ParseError, match="Invalid attributes for step 'head': invalid"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: head data/collection/models.csv
            inputs:
              models:
                path: data/collection/models.csv
                invalid: should not be here
        """
    )

    with pytest.raises(errors.ParseError, match="Invalid attributes for input 'models': invalid"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: echo data > data/collection/models.csv
            outputs:
              models:
                path: data/collection/models.csv
                invalid: should not be here
        """
    )

    with pytest.raises(errors.ParseError, match="Invalid attributes for output 'models': invalid"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: head -n 10 data/collection/models.csv
            inputs:
              models:
                path: data/collection/models.csv
            parameters:
              n:
                prefix: -n
                value: 10
                invalid: should not be here
        """
    )

    with pytest.raises(errors.ParseError, match="Invalid attributes for parameter 'n': invalid"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_parse_error_missing_required_attributes():
    """Test parse errors for required attributes."""
    workflow_file = textwrap.dedent(
        """
        description: name is required for the workflow file
        steps:
          head:
            command: echo running
        """
    )

    with pytest.raises(errors.ParseError, match="Required attribute 'name' isn't set for workflow file"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            description: command is required for workflow file's steps
        """
    )

    with pytest.raises(errors.ParseError, match="Required attribute 'command' isn't set for step 'head'"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: head data/collection/models.csv
            inputs:
              models:
                description: path is required for inputs
        """
    )

    with pytest.raises(errors.ParseError, match="Required attribute 'path' isn't set for input 'models'"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: echo data > data/collection/models.csv
            outputs:
              models:
                description: path is required for outputs
        """
    )

    with pytest.raises(errors.ParseError, match="Required attribute 'path' isn't set for output 'models'"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: head -n 10 data/collection/models.csv
            inputs:
              models:
                path: data/collection/models.csv
            parameters:
              n:
                prefix: -n
                description: value is required for parameters
        """
    )

    with pytest.raises(errors.ParseError, match="Required attribute 'value' isn't set for parameter 'n'"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_warning_for_unused_arguments(mock_communication):
    """Test a warning is printed if an input/output/parameter is not used in a step's command."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: ls
            inputs:
              models:
                path: data/collection/models.csv
              colors:
                path: data/collection/colors.csv
            outputs:
              temporary-result:
                path: intermediate
            parameters:
              n:
                value: 10
        """
    )

    convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    assert (
        "The following inputs/outputs/parameters didn't appear in the command of step 'head': 'ls'"
        in mock_communication.stdout
    )
    assert "models" in mock_communication.stdout_lines
    assert "colors" in mock_communication.stdout_lines
    assert "temporary-result" in mock_communication.stdout_lines
    assert "n" in mock_communication.stdout_lines


def test_no_warning_for_implicit_unused_arguments(mock_communication):
    """Test no warning is printed if an unused input/output/parameter has ``implicit`` attribute set."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: ls
            inputs:
              models:
                path: data/collection/models.csv
              colors:
                path: data/collection/colors.csv
                implicit: true
            outputs:
              temporary-result:
                path: intermediate
                implicit: true
            parameters:
              n:
                value: 10
                implicit: true
        """
    )

    convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    assert (
        "The following inputs/outputs/parameters didn't appear in the command of step 'head': 'ls'"
        in mock_communication.stdout
    )
    assert "models" in mock_communication.stdout_lines
    assert "colors" not in mock_communication.stdout_lines
    assert "temporary-result" not in mock_communication.stdout_lines
    assert "n" not in mock_communication.stdout_lines


@pytest.mark.parametrize("parser", get_available_workflow_file_parsers())
def test_workflow_file_path_is_relative(runner, workflow_file_project, parser):
    """Test path that is stored in ``WorkflowFile`` instance is relative to project's root."""
    # Use absolute path to check that it's converted to a relative path
    absolute_workflow_file_path = workflow_file_project.path / workflow_file_project.workflow_file

    workflow_file = read_workflow_file(path=absolute_workflow_file_path, parser=parser)

    assert "workflow-file.yml" == workflow_file.path
    assert "workflow-file.yml" == workflow_file.steps[0].path


def test_workflow_file_must_be_inside_project_for_execution(project, with_injection):
    """Test workflow files must be inside the project when executing them."""
    with with_injection():
        with pytest.raises(errors.ParameterError, match="Workflow file must be inside the project for execution"):
            path_outside_project = project.path / ".." / "workflow-file.yml"
            run_workflow_file(path=path_outside_project, steps=[], dry_run=False, workflow_file=None, provider="local")


def test_duplicate_workflow_file_name(workflow_file_project, with_injection):
    """Test workflow file name must be unique if workflow file paths differ."""
    run_workflow_file = functools.partial(
        renku.core.workflow.workflow_file.run_workflow_file,
        steps=[],
        dry_run=False,
        workflow_file=None,
        provider="local",
    )

    with with_injection():
        run_workflow_file(path=workflow_file_project.workflow_file)

        moved_workflow_file = workflow_file_project.path / "moved-workflow-file.yml"
        shutil.move(workflow_file_project.workflow_file, moved_workflow_file)

        with pytest.raises(errors.DuplicateWorkflowNameError, match="Workflow 'workflow-file' already exists."):
            run_workflow_file(path=moved_workflow_file)


def test_parse_error_invalid_inputs_type():
    """Test error raises when inputs have an invalid type."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          head:
            command: echo running
            inputs:
              repository: renku/image
              tag: '0.1.2'
        """
    )

    with pytest.raises(
        errors.ParseError,
        match=re.escape("Expected Dict[str, Any] when processing input 'repository', but found 'str'"),
    ):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_parse_error_invalid_keywords_type():
    """Test error raises when keywords have an invalid type."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        keywords:
          - name: Test
            value: test
            description: something
        steps:
          cp:
            inputs:
              - a
            outputs:
              - b
            command: cp a b
        """
    )

    with pytest.raises(
        errors.ParseError,
        match=re.escape(
            "Expected '(str, int, float)' for elements of attribute 'keywords' in workflow file but got 'dict'"
        ),
    ):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          cp:
            keywords:
              - name: Test
                value: test
                description: something
            inputs:
              - a
            outputs:
              - b
            command: cp a b
        """
    )

    with pytest.raises(
        errors.ParseError,
        match=re.escape(
            "Expected '(str, int, float)' for elements of attribute 'keywords' in step 'cp' but got 'dict'"
        ),
    ):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_parse_error_invalid_success_codes_type():
    """Test error raises when success codes have an invalid type."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          cp:
            success_codes:
              - test: 7
            inputs:
              - a
            outputs:
              - b
            command: cp a b
        """
    )

    with pytest.raises(
        errors.ParseError,
        match=re.escape("Expected 'int' for elements of attribute 'success_codes' in step 'cp' but got 'dict'"),
    ):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_parse_error_invalid_command():
    """Test proper error raises when command has an invalid token."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        steps:
          echo:
            command: if 1 ; then echo hello worlds ; fi
        """
    )

    with pytest.raises(
        errors.ParseError,
        match=re.escape("Unsupported command token: 'Node compound: if 1 ; then echo hello worlds ; fi' in "),
    ):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_no_parse_error_for_non_required_attributes():
    """Test no parse error is raised when a non-required field has ``None`` value."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-test
        description:
        keywords:
        steps:
          step:
            command: cmd foo bar
            description:
            keywords: []
            success_codes:
            inputs:
              foo:
                path: foo
                description:
                prefix:
            outputs:
              bar:
                path: bar
                description:
                prefix:
            parameters:
              baz:
                value: 42
                description:
                prefix:
        """
    )

    assert convert_to_workflow_file(data=load_yaml(workflow_file), path="") is not None


def test_parse_error_when_cannot_match_part_of_command():
    """Test an error is raised when a part of command cannot be found in parameters."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-file
        steps:
          step:
            command: cmd -p foo -q bar -o baz
            inputs:
            - foo
            outputs:
            - bar
            - baz
        """
    )

    with pytest.raises(errors.ParseError, match="Cannot find an argument for '-q'"):
        convert_to_workflow_file(data=load_yaml(workflow_file), path="")


def test_command_is_parsed_correctly_when_ends_with_prefix_like_word():
    """Test that parsing works correctly if commands end with a token that is similar to a command prefix."""
    workflow_file = textwrap.dedent(
        """
        name: workflow-file
        steps:
          step:
            command: cmd -option -- foo
            inputs:
            - foo
        """
    )

    workflow = convert_to_workflow_file(data=load_yaml(workflow_file), path="")

    assert "cmd -option" == workflow.steps[0].command


def test_workflow_file_with_cycle_raises_error(workflow_file_project, with_injection):
    """Test an error is raised when there is a cycle inside a workflow file."""
    workflow = textwrap.dedent(
        """
        name: workflow-file
        steps:
          head:
            command: head $input > $intermediate
            inputs:
              input:
                path: data/collection/models.csv
            outputs:
              intermediate:
                path: intermediate

          tail:
            command: tail $intermediate > $input
            inputs:
              intermediate:
                path: intermediate
            outputs:
              input:
                path: data/collection/models.csv
        """
    )
    workflow_file = workflow_file_project.path / "workflow-file.yml"
    workflow_file.write_text(workflow)

    with with_injection(), pytest.raises(errors.GraphCycleError) as exception:
        run_workflow_file(path=workflow_file, steps=[], dry_run=False, workflow_file=None, provider="local")

        assert "Circular workflows are not supported in Renku" in exception.value.args[0]
        assert "Input: data/collection/models.csv" in exception.value.args[0]
        assert "workflow-file.head" in exception.value.args[0]
        assert "Output: data/collection/models.csv" in exception.value.args[0]
