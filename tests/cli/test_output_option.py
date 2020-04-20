# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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

import pytest
import yaml

from renku.core.models.cwl import CWLClass


def read_all_cwl_files(client, glob='*.cwl'):
    """
    Return an array where its elements are content of CWL file
    found in the Renku project.
    """
    return [read_cwl_file(f) for f in client.workflow_path.glob(glob)]


def read_cwl_file(cwl_filepath):
    with cwl_filepath.open('r') as f:
        return CWLClass.from_cwl(yaml.safe_load(f))


def test_run_succeeds_normally(cli, client, subdirectory):
    """Test when an output is detected"""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    exit_code, cwl = cli('run', 'touch', foo)

    assert 0 == exit_code
    assert 1 == len(cwl.inputs)
    assert 'foo' == cwl.inputs[0].default
    assert 'string' == cwl.inputs[0].type
    assert 1 == len(cwl.outputs)
    assert 'File' == cwl.outputs[0].type
    assert '$(inputs.input_1)' == cwl.outputs[0].outputBinding.glob


def test_when_no_change_in_outputs_is_detected(cli, subdirectory):
    """Test when no output is detected"""
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', 'ls', 'foo')

    assert 1 == exit_code


def test_with_no_output_option(cli, client, subdirectory):
    """Test --no-output option with no output detection"""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    cli('run', 'touch', foo)
    exit_code, cwl = cli('run', '--no-output', 'touch', foo)

    assert 0 == exit_code
    assert 1 == len(cwl.inputs)
    assert 'File' == cwl.inputs[0].type
    assert '../../foo' == str(cwl.inputs[0].default)
    assert 0 == len(cwl.outputs)

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert 2 == len(cwls)
    assert cwls[0].inputs != cwls[1].inputs
    assert cwls[0].outputs != cwls[1].outputs


@pytest.mark.parametrize(
    'command,expected_type', [(('touch', ), 'File'),
                              (('mkdir', '-p'), 'Directory')]
)
def test_explicit_outputs(cli, command, expected_type, client, subdirectory):
    """Test detection of an output file with --output option."""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    cli('run', *command, foo)
    exit_code, cwl = cli('run', '--output', foo, *command, foo)

    assert 0 == exit_code
    assert 1 == len(cwl.inputs)
    assert 'foo' == cwl.inputs[0].default
    assert 'string' == cwl.inputs[0].type
    assert 1 == len(cwl.outputs)
    assert expected_type == cwl.outputs[0].type
    assert '$(inputs.input_1)' == cwl.outputs[0].outputBinding.glob


def test_explicit_output_results(cli, client, subdirectory):
    """Test explicit output yield same results as normal run"""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    cli('run', 'touch', foo)
    cli('run', '--output', foo, 'touch', foo)

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert 2 == len(cwls)
    assert cwls[0].inputs == cwls[1].inputs
    assert cwls[0].outputs == cwls[1].outputs


def test_explicit_outputs_and_normal_outputs(cli, client, subdirectory):
    """Test explicit outputs and normal outputs can both exist"""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    bar = os.path.relpath(client.path / 'bar', os.getcwd())
    cli('run', 'touch', foo)
    exit_code, cwl = cli('run', '--output', foo, 'touch', foo, bar)

    assert 0 == exit_code
    cwl.inputs.sort(key=lambda e: e.default)
    assert 2 == len(cwl.inputs)
    assert 'string' == cwl.inputs[0].type
    assert 'bar' == str(cwl.inputs[0].default)
    assert 'string' == cwl.inputs[1].type
    assert 'foo' == str(cwl.inputs[1].default)

    assert 2 == len(cwl.outputs)
    assert cwl.outputs[0].outputBinding != cwl.outputs[1].outputBinding


def test_explicit_outputs_and_std_output_streams(cli, client, subdirectory):
    """Test that unchanged std output streams can be marked with explicit
    outputs"""
    exit_code, _ = cli('run', 'sh', '-c', 'echo foo > bar')
    assert 0 == exit_code

    exit_code, _ = cli('run', 'sh', '-c', 'echo foo > bar')
    assert 1 == exit_code

    exit_code, _ = cli('run', '--output', 'bar', 'sh', '-c', 'echo foo > bar')
    assert 0 == exit_code


def test_output_directory_with_output_option(cli, client, subdirectory):
    """Test output directories are not deleted with --output"""
    outdir = os.path.relpath(client.path / 'outdir', os.getcwd())
    a_script = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    cli('run', *a_script, outdir, 'foo')

    exit_code, _ = cli('run', '--output', outdir, *a_script, outdir, 'bar')

    assert 0 == exit_code
    assert (client.path / 'outdir' / 'foo').exists()
    assert (client.path / 'outdir' / 'bar').exists()


def test_output_directory_without_separate_outputs(cli, client):
    """Test output files not listed as separate outputs.

    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    a_script = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, cwl = cli('run', *a_script, 'outdir', 'foo')

    assert 0 == exit_code
    assert 1 == len(cwl.outputs)
    assert 'Directory' == cwl.outputs[0].type


def test_explicit_inputs_must_exist(cli):
    """Test explicit inputs exist before run"""
    exit_code, _ = cli('run', '--input', 'foo', 'touch', 'bar')

    assert 1 == exit_code


def test_explicit_inputs_are_inside_repo(cli):
    """Test explicit inputs are inside the Renku repo"""
    exit_code, _ = cli('run', '--input', '/tmp', 'touch', 'foo')

    assert 1 == exit_code


def test_explicit_inputs_and_outputs_are_listed(cli, client):
    """Test explicit inputs and outputs will be in generated CWL file"""
    cli('run', 'mkdir', 'foo')
    cli('run', 'touch', 'bar', 'baz')

    exit_code, cwl = cli(
        'run', '--input', 'foo', '--input', 'bar', '--output', 'baz', 'echo'
    )

    assert 0 == exit_code
    assert 2 == len(cwl.inputs)
    cwl.inputs.sort(key=lambda e: e.type)

    assert '../../foo' == str(cwl.inputs[0].default)
    assert 'Directory' == cwl.inputs[0].type

    assert cwl.inputs[0].inputBinding is None
    assert '../../bar' == str(cwl.inputs[1].default)

    assert cwl.inputs[1].inputBinding is None
    assert 'File' == cwl.inputs[1].type
    assert 'baz' == cwl.outputs[0].outputBinding.glob


def test_explicit_inputs_can_be_in_inputs(cli, client, subdirectory):
    """Test explicit inputs that are in inputs are treated as normal inputs"""
    foo = os.path.relpath(client.path / 'foo', os.getcwd())
    cli('run', 'touch', foo)

    exit_code, cwl = cli('run', '--input', foo, '--no-output', 'ls', foo)

    assert 0 == exit_code
    assert 1 == len(cwl.inputs)

    assert '../../foo' == str(cwl.inputs[0].default)
    assert 'File' == cwl.inputs[0].type

    assert cwl.inputs[0].inputBinding is not None


def test_explicit_inputs_in_subdirectories(cli, client):
    """Test explicit inputs that are in sub-dirs are made accessible"""

    # Set up a script with hard dependency
    cli('run', '--no-output', 'mkdir', 'foo')
    cli('run', 'sh', '-c', 'echo "some changes" > foo/bar')
    cli('run', 'sh', '-c', 'echo "cat foo/bar" > script.sh')

    exit_code, _ = cli(
        'run', '--input', 'foo/bar', '--input', 'script.sh', 'sh', '-c',
        'sh script.sh > output'
    )
    assert 0 == exit_code

    # Status must be dirty if foo/bar changes
    cli('run', 'sh', '-c', 'echo "new changes" > foo/bar')
    exit_code, _ = cli('status')
    assert 1 == exit_code

    exit_code, cwl = cli('update')
    assert 0 == exit_code
    assert (client.path / 'foo' / 'bar').exists()
    assert (client.path / 'script.sh').exists()
    assert (client.path / 'output').exists()
