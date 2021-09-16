# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Test behavior of ``--output`` option."""

import os
from pathlib import Path

from renku.cli import cli


def test_run_succeeds_normally(renku_cli, client, subdirectory):
    """Test when an output is detected"""
    foo = os.path.relpath(client.path / "foo", os.getcwd())
    exit_code, activity = renku_cli("run", "touch", foo)

    plan = activity.association.plan
    assert 0 == exit_code
    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert "foo" == plan.outputs[0].default_value


def test_when_no_change_in_outputs_is_detected(renku_cli, subdirectory):
    """Test when no output is detected"""
    renku_cli("run", "touch", "foo")
    exit_code, _ = renku_cli("run", "ls", "foo")

    assert 1 == exit_code


def test_with_no_output_option(renku_cli, client, subdirectory):
    """Test --no-output option with no output detection"""
    foo = os.path.relpath(client.path / "foo", os.getcwd())
    renku_cli("run", "touch", foo)
    exit_code, activity = renku_cli("run", "--no-output", "touch", foo)

    plan = activity.association.plan
    assert 0 == exit_code
    assert 1 == len(plan.inputs)
    assert "foo" == str(plan.inputs[0].default_value)
    assert 0 == len(plan.outputs)


def test_explicit_outputs_and_normal_outputs(renku_cli, client, subdirectory):
    """Test explicit outputs and normal outputs can co-exist"""
    foo = os.path.relpath(client.path / "foo", os.getcwd())
    os.mkdir(foo)
    bar = os.path.relpath(client.path / "bar", os.getcwd())
    renku_cli("run", "touch", bar)
    baz = os.path.relpath(client.path / "baz", os.getcwd())
    qux = os.path.join(foo, "qux")

    exit_code, activity = renku_cli("run", "--output", foo, "--output", bar, "touch", baz, qux)

    plan = activity.association.plan
    assert 0 == exit_code
    plan.inputs.sort(key=lambda e: e.position)
    assert 4 == len(plan.outputs)
    assert {"foo", "bar", "baz", "foo/qux"} == {str(o.default_value) for o in plan.outputs}


def test_explicit_outputs_and_std_output_streams(renku_cli, client, subdirectory):
    """Test that unchanged std output streams can be marked with explicit
    outputs"""
    exit_code, _ = renku_cli("run", "echo", "foo", stdout="bar")
    assert 0 == exit_code

    exit_code, _ = renku_cli("run", "echo", "foo", stdout="bar")
    assert 1 == exit_code

    exit_code, _ = renku_cli("run", "--output", "bar", "echo", "foo", stdout="bar")
    assert 0 == exit_code


def test_output_directory_with_output_option(runner, renku_cli, client, subdirectory):
    """Test output directories are not deleted with --output"""
    outdir = os.path.relpath(client.path / "outdir", os.getcwd())
    a_script = ("sh", "-c", 'mkdir -p "$0"; touch "$0/$1"')
    assert 0 == runner.invoke(cli, ["run", *a_script, outdir, "foo"]).exit_code
    result = runner.invoke(cli, ["run", "--output", outdir, *a_script, outdir, "bar"], catch_exceptions=False)

    assert 0 == result.exit_code
    assert (client.path / "outdir" / "foo").exists()
    assert (client.path / "outdir" / "bar").exists()


def test_output_directory_without_separate_outputs(renku_cli, client):
    """Test output files not listed as separate outputs.

    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    a_script = ("sh", "-c", 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, activity = renku_cli("run", *a_script, "outdir", "foo")

    assert 0 == exit_code
    assert 1 == len(activity.association.plan.outputs)


def test_explicit_inputs_must_exist(renku_cli):
    """Test explicit inputs exist before run"""
    exit_code, _ = renku_cli("run", "--input", "foo", "touch", "bar")

    assert 2 == exit_code


def test_explicit_inputs_are_inside_repo(renku_cli):
    """Test explicit inputs are inside the Renku repo"""
    exit_code, _ = renku_cli("run", "--input", "/tmp", "touch", "foo")

    assert 2 == exit_code


def test_explicit_outputs_must_exist(renku_cli):
    """Test explicit outputs exist after run"""
    exit_code, _ = renku_cli("run", "--output", "foo", "touch", "bar")

    assert 2 == exit_code


def test_explicit_inputs_and_outputs_are_listed(renku_cli, client):
    """Test explicit inputs and outputs will be in generated CWL file"""
    foo = Path(os.path.relpath(client.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "foo/file")
    renku_cli("run", "touch", "bar", "baz")

    exit_code, activity = renku_cli("run", "--input", "foo", "--input", "bar", "--output", "baz", "echo")

    plan = activity.association.plan
    assert 0 == exit_code

    assert 2 == len(plan.inputs)
    plan.inputs.sort(key=lambda e: e.default_value)

    assert plan.inputs[0].position is None
    assert "bar" == str(plan.inputs[0].default_value)

    assert plan.inputs[1].position is None
    assert "foo" == str(plan.inputs[1].default_value)

    assert plan.outputs[0].position is None
    assert "baz" == plan.outputs[0].default_value


def test_explicit_inputs_can_be_in_inputs(renku_cli, client, subdirectory):
    """Test explicit inputs that are in inputs are treated as normal inputs"""
    foo = os.path.relpath(client.path / "foo", os.getcwd())
    renku_cli("run", "touch", foo)

    exit_code, activity = renku_cli("run", "--input", foo, "--no-output", "ls", foo)

    plan = activity.association.plan
    assert 0 == exit_code
    assert 1 == len(plan.inputs)

    assert "foo" == str(plan.inputs[0].default_value)

    assert plan.inputs[0].position is not None


def test_explicit_inputs_in_subdirectories(renku_cli, client):
    """Test explicit inputs that are in sub-dirs are made accessible"""

    # Set up a script with hard dependency
    renku_cli("run", "--no-output", "mkdir", "foo")
    renku_cli("run", "echo", "some changes", stdout="foo/bar")
    renku_cli("run", "echo", "cat foo/bar", stdout="script.sh")

    exit_code, _ = renku_cli("run", "--input", "foo/bar", "--input", "script.sh", "sh", "script.sh", stdout="output")
    assert 0 == exit_code

    # Status must be dirty if foo/bar changes
    renku_cli("run", "echo", "new changes", stdout="foo/bar")
    exit_code, _ = renku_cli("status")
    assert 1 == exit_code

    exit_code, _ = renku_cli("update", "--all")
    assert 0 == exit_code
    assert (client.path / "foo" / "bar").exists()
    assert (client.path / "script.sh").exists()
    assert (client.path / "output").exists()


def test_no_explicit_or_detected_output(renku_cli):
    """Test output detection is disbaled and no explicit output is passed."""
    exit_code, _ = renku_cli("run", "--no-output-detection", "echo")

    assert 1 == exit_code


def test_no_output_and_disabled_detection(renku_cli):
    """Test --no-output works with no output detection."""
    exit_code, activity = renku_cli("run", "--no-output-detection", "--no-output", "echo")

    plan = activity.association.plan
    assert 0 == exit_code
    assert 0 == len(plan.inputs)
    assert 0 == len(plan.outputs)


def test_disabled_detection(renku_cli):
    """Test disabled auto-detection of inputs and outputs."""
    exit_code, activity = renku_cli(
        "run", "--no-input-detection", "--no-output-detection", "--output", "README.md", "touch", "some-files"
    )

    plan = activity.association.plan
    assert 0 == exit_code
    assert 0 == len(plan.inputs)
    assert 1 == len(plan.outputs)
    assert "README.md" == str(plan.outputs[0].default_value)


def test_inputs_must_be_passed_with_no_detection(renku_cli, client):
    """Test when detection is disabled, inputs must be explicitly passed."""
    exit_code, activity = renku_cli(
        "run", "--no-input-detection", "--input", "Dockerfile", "--no-output", "ls", "-l", "README.md", "Dockerfile"
    )

    plan = activity.association.plan
    assert 0 == exit_code
    assert 1 == len(plan.inputs)
    assert plan.inputs[0].position is not None
    assert "Dockerfile" == str(plan.inputs[0].default_value)


def test_overlapping_explicit_outputs(renku_cli, client):
    """Test explicit outputs are not removed even if they overlap."""
    foo = Path(os.path.relpath(client.path / "foo", os.getcwd()))
    foo.mkdir()
    renku_cli("run", "touch", "foo/bar")

    exit_code, activity = renku_cli(
        "run", "--no-input-detection", "--no-output-detection", "--output", "foo", "--output", "foo/bar", "echo"
    )

    plan = activity.association.plan
    assert 0 == exit_code
    assert 0 == len(plan.inputs)
    assert 2 == len(plan.outputs)
    assert {"foo", "foo/bar"} == {str(o.default_value) for o in plan.outputs}


def test_std_streams_must_be_in_explicits(renku_cli):
    """Test when auto-detection is disabled, std streams must be passed
    explicitly."""
    exit_code, activity = renku_cli(
        "run", "--no-output-detection", "--output", "Dockerfile", "ls", stdin="README.md", stdout="out", stderr="err"
    )

    plan = activity.association.plan
    assert 0 == exit_code
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

    plan = activity.association.plan
    assert 0 == exit_code
    assert 1 == len(plan.inputs)
    assert "README.md" == str(plan.inputs[0].default_value)
    assert 2 == len(plan.outputs)
    assert {"out", "err"} == {str(o.default_value) for o in plan.outputs}


def test_explicit_input_as_out_streams(renku_cli):
    """Test cannot use explicit inputs as stdout/stderr when auto-detection is
    disabled."""
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
    """Test cannot use explicit outputs as stdin when auto-detection is
    disabled."""
    exit_code, _ = renku_cli(
        "run", "--no-input-detection", "--no-output-detection", "--output", "README.md", "ls", stdin="README.md"
    )

    assert 2 == exit_code
