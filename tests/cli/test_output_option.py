# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Test behavior of ``--output`` option."""

import os
from pathlib import Path

from renku.ui.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


def test_run_succeeds_normally(renku_cli, project, subdirectory):
    """Test when an output is detected."""
    foo = os.path.relpath(project.path / "foo", os.getcwd())
    exit_code, activity = renku_cli("run", "touch", foo)

    assert 0 == exit_code
    plan = activity.association.plan
    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert "foo" == plan.outputs[0].default_value


def test_when_no_change_in_outputs_is_detected(renku_cli, subdirectory):
    """Test when no output is detected."""
    renku_cli("run", "touch", "foo")
    exit_code, _ = renku_cli("run", "ls", "foo")

    assert 1 == exit_code


def test_with_no_output_option(renku_cli, project, subdirectory):
    """Test --no-output option with no output detection."""
    foo = os.path.relpath(project.path / "foo", os.getcwd())
    renku_cli("run", "touch", foo)
    exit_code, activity = renku_cli("run", "--no-output", "touch", foo)

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert "foo" == str(plan.inputs[0].default_value)
    assert 0 == len(plan.outputs)


def test_explicit_outputs_and_normal_outputs(renku_cli, project, subdirectory):
    """Test explicit outputs and normal outputs can co-exist."""
    foo = os.path.relpath(project.path / "foo", os.getcwd())
    os.mkdir(foo)
    bar = os.path.relpath(project.path / "bar", os.getcwd())
    renku_cli("run", "touch", bar)
    baz = os.path.relpath(project.path / "baz", os.getcwd())
    qux = os.path.join(foo, "qux")

    exit_code, activity = renku_cli("run", "--output", foo, "--output", bar, "touch", baz, qux)
    assert 0 == exit_code
    plan = activity.association.plan

    plan.inputs.sort(key=lambda e: e.position)
    assert 4 == len(plan.outputs)
    assert {"foo", "bar", "baz", "foo/qux"} == {str(o.default_value) for o in plan.outputs}


def test_explicit_outputs_and_std_output_streams(renku_cli, project, subdirectory):
    """Test that unchanged std output streams can be marked with explicit outputs."""
    exit_code, _ = renku_cli("run", "echo", "foo", stdout="bar")
    assert 0 == exit_code

    exit_code, _ = renku_cli("run", "echo", "foo", stdout="bar")
    assert 1 == exit_code

    exit_code, _ = renku_cli("run", "--output", "bar", "echo", "foo", stdout="bar")
    assert 0 == exit_code


def test_output_directory_with_output_option(runner, renku_cli, project, subdirectory):
    """Test output directories are not deleted with --output."""
    out_dir = os.path.relpath(project.path / "out-dir", os.getcwd())
    a_script = ("sh", "-c", 'mkdir -p "$0"; touch "$0/$1"')
    assert 0 == runner.invoke(cli, ["run", *a_script, out_dir, "foo"]).exit_code
    result = runner.invoke(cli, ["run", "--output", out_dir, *a_script, out_dir, "bar"], catch_exceptions=False)

    assert 0 == result.exit_code
    assert (project.path / "out-dir" / "foo").exists()
    assert (project.path / "out-dir" / "bar").exists()


def test_output_directory_without_separate_outputs(renku_cli, project):
    """Test output files not listed as separate outputs.

    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    a_script = ("sh", "-c", 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, activity = renku_cli("run", *a_script, "out-dir", "foo")

    assert 0 == exit_code
    assert 1 == len(activity.association.plan.outputs)


def test_explicit_inputs_must_exist(renku_cli):
    """Test explicit inputs exist before run."""
    exit_code, _ = renku_cli("run", "--input", "foo", "touch", "bar")

    assert 2 == exit_code


def test_explicit_inputs_duplicate_value(renku_cli):
    """Test explicit inputs exist before run."""
    exit_code, _ = renku_cli("run", "--input", "foo", "--input", "foo", "touch", "foo")

    assert 2 == exit_code


def test_explicit_inputs_duplicate_name(renku_cli):
    """Test explicit inputs exist before run."""
    exit_code, _ = renku_cli("run", "--input", "my-input=foo", "--input", "my-input=bar", "touch", "foo", "bar")

    assert 2 == exit_code


def test_explicit_inputs_are_inside_repo(renku_cli):
    """Test explicit inputs are inside the Renku repo."""
    exit_code, _ = renku_cli("run", "--input", "/tmp", "touch", "foo")

    assert 2 == exit_code


def test_explicit_outputs_must_exist(renku_cli):
    """Test explicit outputs exist after run."""
    exit_code, _ = renku_cli("run", "--output", "foo", "touch", "bar")

    assert 2 == exit_code


def test_explicit_outputs_duplicate_value(renku_cli):
    """Test explicit outputs exist after run."""
    exit_code, _ = renku_cli("run", "--output", "foo", "--output", "foo", "touch", "foo")

    assert 2 == exit_code


def test_explicit_outputs_duplicate_name(renku_cli):
    """Test explicit outputs exist after run."""
    exit_code, _ = renku_cli("run", "--output", "my-output=foo", "--output", "my-output=bar", "touch", "foo", "bar")

    assert 2 == exit_code


def test_explicit_inputs_and_outputs_are_listed(renku_cli, project):
    """Test explicit inputs and outputs will be in generated CWL file."""
    foo = Path(os.path.relpath(project.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "foo/file")
    renku_cli("run", "touch", "bar", "baz")

    exit_code, activity = renku_cli("run", "--input", "foo", "--input", "bar", "--output", "baz", "echo")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 2 == len(plan.inputs)
    plan.inputs.sort(key=lambda e: e.default_value)

    assert plan.inputs[0].position is None
    assert "bar" == str(plan.inputs[0].default_value)

    assert plan.inputs[1].position is None
    assert "foo" == str(plan.inputs[1].default_value)

    assert plan.outputs[0].position is None
    assert "baz" == plan.outputs[0].default_value


def test_explicit_inputs_and_outputs_are_listed_with_names(renku_cli, project):
    """Test explicit inputs and outputs will be in generated CWL file."""
    foo = Path(os.path.relpath(project.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "foo/file")
    renku_cli("run", "touch", "bar", "baz")

    exit_code, activity = renku_cli(
        "run", "--input", "my-input1=foo", "--input", "my-input2=bar", "--output", "my-output1=baz", "echo"
    )

    assert 0 == exit_code
    plan = activity.association.plan

    assert 2 == len(plan.inputs)
    plan.inputs.sort(key=lambda e: e.default_value)

    assert plan.inputs[0].position is None
    assert "bar" == str(plan.inputs[0].default_value)

    assert plan.inputs[1].position is None
    assert "foo" == str(plan.inputs[1].default_value)

    assert any("my-input1" == i.name for i in plan.inputs)
    assert any("my-input2" == i.name for i in plan.inputs)

    assert plan.outputs[0].position is None
    assert "baz" == plan.outputs[0].default_value
    assert "my-output1" == plan.outputs[0].name


def test_explicit_inputs_can_be_in_inputs(renku_cli, project, subdirectory):
    """Test explicit inputs that are in inputs are treated as normal inputs."""
    foo = os.path.relpath(project.path / "foo", os.getcwd())
    renku_cli("run", "touch", foo)

    exit_code, activity = renku_cli("run", "--input", foo, "--no-output", "ls", foo)

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)

    assert "foo" == str(plan.inputs[0].default_value)

    assert plan.inputs[0].position is not None


def test_explicit_inputs_in_subdirectories(project, runner):
    """Test explicit inputs that are in sub-dirs are made accessible."""
    # Set up a script with hard dependency
    assert 0 == runner.invoke(cli, ["run", "--no-output", "mkdir", "foo"]).exit_code
    assert 0 == runner.invoke(cli, ["run", "echo", "some changes"], stdout="foo/bar").exit_code
    assert 0 == runner.invoke(cli, ["run", "echo", "cat foo/bar"], stdout="script.sh").exit_code

    result = runner.invoke(
        cli, ["run", "--input", "foo/bar", "--input", "script.sh", "sh", "script.sh"], stdout="output"
    )
    assert 0 == result.exit_code, format_result_exception(result)

    # Status must be dirty if foo/bar changes
    write_and_commit_file(project.repository, project.path / "foo" / "bar", "new changes")

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])

    assert 1 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["update", "--all"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert (project.path / "foo" / "bar").exists()
    assert (project.path / "script.sh").exists()
    assert (project.path / "output").exists()


def test_no_explicit_or_detected_output(renku_cli):
    """Test output detection is disabled and no explicit output is passed."""
    exit_code, _ = renku_cli("run", "--no-output-detection", "echo")

    assert 1 == exit_code


def test_no_output_and_disabled_detection(renku_cli):
    """Test --no-output works with no output detection."""
    exit_code, activity = renku_cli("run", "--no-output-detection", "--no-output", "echo")

    assert 0 == exit_code
    plan = activity.association.plan
    assert 0 == len(plan.inputs)
    assert 0 == len(plan.outputs)


def test_disabled_detection(renku_cli):
    """Test disabled auto-detection of inputs and outputs."""
    exit_code, activity = renku_cli(
        "run",
        "--no-input-detection",
        "--no-output-detection",
        "--no-parameter-detection",
        "--output",
        "README.md",
        "touch",
        "some-files",
        "-f",
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert 0 == len(plan.inputs)
    assert 0 == len(plan.parameters)
    assert 1 == len(plan.outputs)
    assert "README.md" == str(plan.outputs[0].default_value)


def test_inputs_must_be_passed_with_no_detection(renku_cli, project):
    """Test when detection is disabled, inputs must be explicitly passed."""
    exit_code, activity = renku_cli(
        "run", "--no-input-detection", "--input", "Dockerfile", "--no-output", "ls", "-l", "README.md", "Dockerfile"
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert plan.inputs[0].position is not None
    assert "Dockerfile" == str(plan.inputs[0].default_value)


def test_overlapping_explicit_outputs(renku_cli, project):
    """Test explicit outputs are not removed even if they overlap."""
    foo = Path(os.path.relpath(project.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "foo/bar")

    exit_code, activity = renku_cli(
        "run", "--no-input-detection", "--no-output-detection", "--output", "foo", "--output", "foo/bar", "echo"
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert 0 == len(plan.inputs)
    assert 2 == len(plan.outputs)
    assert {"foo", "foo/bar"} == {str(o.default_value) for o in plan.outputs}


def test_std_streams_must_be_in_explicits(renku_cli):
    """Test when auto-detection is disabled, std streams must be passed explicitly."""
    exit_code, activity = renku_cli(
        "run", "--no-output-detection", "--output", "Dockerfile", "ls", stdin="README.md", stdout="out", stderr="err"
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert "README.md" == str(plan.inputs[0].default_value)
    assert 1 == len(plan.outputs)
    assert "Dockerfile" == str(plan.outputs[0].default_value)

    exit_code, activity = renku_cli(
        "run",
        "--no-input-detection",
        "--no-output-detection",
        "--input",
        "README.md",
        "--output",
        "out",
        "--output",
        "err",
        "ls",
        stdin="Dockerfile",
        stdout="out",
        stderr="err",
    )

    assert 0 == exit_code
    plan = activity.association.plan
    assert 1 == len(plan.inputs)
    assert "README.md" == str(plan.inputs[0].default_value)
    assert 2 == len(plan.outputs)
    assert {"out", "err"} == {str(o.default_value) for o in plan.outputs}


def test_explicit_input_as_out_streams(renku_cli):
    """Test cannot use explicit inputs as stdout/stderr when auto-detection is disabled."""
    exit_code, _ = renku_cli(
        "run",
        "--no-input-detection",
        "--no-output-detection",
        "--input",
        "README.md",
        "ls",
        stdout="README.md",
        stderr="README.md",
    )

    assert 2 == exit_code


def test_explicit_output_as_stdin(renku_cli):
    """Test cannot use explicit outputs as stdin when auto-detection is disabled."""
    exit_code, _ = renku_cli(
        "run", "--no-input-detection", "--no-output-detection", "--output", "README.md", "ls", stdin="README.md"
    )

    assert 2 == exit_code


def test_explicit_parameter(renku_cli):
    """Test explicit parameters are inside the Renku repo."""
    exit_code, activity = renku_cli("run", "--param", "test", "echo", "test", stdout="target.txt")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert 1 == plan.parameters[0].position
    assert "test" == str(plan.parameters[0].default_value)


def test_explicit_parameter_with_name(renku_cli):
    """Test explicit parameters are inside the Renku repo."""
    exit_code, activity = renku_cli("run", "--param", "my-param=test", "echo", "test", stdout="target.txt")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert 1 == plan.parameters[0].position
    assert "test" == str(plan.parameters[0].default_value)
    assert "my-param" == str(plan.parameters[0].name)


def test_explicit_parameter_is_listed(renku_cli):
    """Test explicit parameters are can be set when not in the command."""
    exit_code, activity = renku_cli("run", "--param", "test", "--no-output", "echo")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 0 == len(plan.inputs)
    assert 0 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert plan.parameters[0].position is None
    assert "test" == str(plan.parameters[0].default_value)


def test_explicit_parameter_with_same_output(renku_cli):
    """Test explicit parameter can coexist with output of same name."""
    exit_code, activity = renku_cli("run", "--param", "test", "echo", "test", stdout="test")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert 2 == plan.outputs[0].position
    assert "test" == str(plan.outputs[0].default_value)

    assert 1 == plan.parameters[0].position
    assert "test" == str(plan.parameters[0].default_value)


def test_explicit_parameter_with_same_input(renku_cli, project):
    """Test explicit parameter can coexist with output of same name."""
    foo = Path(os.path.relpath(project.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "test")
    exit_code, activity = renku_cli("run", "--param", "test", "cat", "test", stdout="target")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert 1 == plan.parameters[0].position
    assert "test" == str(plan.parameters[0].default_value)


def test_explicit_parameter_with_same_explicit_input(renku_cli, project):
    """Test explicit parameter can coexist with output of same name."""
    foo = Path(os.path.relpath(project.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "test")
    exit_code, activity = renku_cli("run", "--param", "test", "--input", "test", "cat", "test", stdout="target")

    assert 0 == exit_code
    plan = activity.association.plan

    assert 1 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert 1 == len(plan.parameters)

    assert 1 == plan.parameters[0].position
    assert "test" == str(plan.parameters[0].default_value)

    assert plan.inputs[0].position is None
    assert "test" == str(plan.inputs[0].default_value)


def test_explicit_parameter_duplicate_value(renku_cli):
    """Test explicit inputs exist before run."""
    exit_code, _ = renku_cli("run", "--param", "foo", "--param", "foo", "touch", "foo")

    assert 2 == exit_code


def test_explicit_parameter_duplicate_name(renku_cli):
    """Test explicit inputs exist before run."""
    exit_code, _ = renku_cli("run", "--param", "my-param=foo", "--param", "my-param=bar", "touch", "foo", "bar")

    assert 2 == exit_code
