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


def test_run_succeeds_normally(cli):
    """Test when an output is detected"""
    exit_code, cwl = cli('run', 'touch', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert cwl.inputs[0].default == 'foo'
    assert cwl.inputs[0].type == 'string'
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].type == 'File'
    assert cwl.outputs[0].outputBinding.glob == '$(inputs.input_1)'


def test_when_no_change_in_outputs_is_detected(cli):
    """Test when no output is detected"""
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', 'ls', 'foo')

    assert exit_code == 1


def test_with_no_output_option(cli, client):
    """Test --no-output option with no output detection"""
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', '--no-output', 'touch', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert cwl.inputs[0].type == 'File'
    assert str(cwl.inputs[0].default) == '../../foo'
    assert len(cwl.outputs) == 0

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert len(cwls) == 2
    assert cwls[0].inputs != cwls[1].inputs
    assert cwls[0].outputs != cwls[1].outputs


@pytest.mark.parametrize(
    'command,expected_type', [(('touch', ), 'File'),
                              (('mkdir', '-p'), 'Directory')]
)
def test_explicit_outputs(cli, command, expected_type):
    """Test detection of an output file with --output option."""
    cli('run', *command, 'foo')
    exit_code, cwl = cli('run', '--output', 'foo', *command, 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert cwl.inputs[0].default == 'foo'
    assert cwl.inputs[0].type == 'string'
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].type == expected_type
    assert cwl.outputs[0].outputBinding.glob == '$(inputs.input_1)'


def test_explicit_output_results(cli, client):
    """Test explicit output yield same results as normal run"""
    cli('run', 'touch', 'foo')
    cli('run', '--output', 'foo', 'touch', 'foo')

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert len(cwls) == 2
    assert cwls[0].inputs == cwls[1].inputs
    assert cwls[0].outputs == cwls[1].outputs


def test_explicit_outputs_and_normal_outputs(cli, client):
    """Test explicit outputs and normal outputs can both exist"""
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', '--output', 'foo', 'touch', 'foo', 'bar')

    assert exit_code == 0
    cwl.inputs.sort(key=lambda e: e.default)
    assert len(cwl.inputs) == 2
    assert cwl.inputs[0].type == 'string'
    assert str(cwl.inputs[0].default) == 'bar'
    assert cwl.inputs[1].type == 'string'
    assert str(cwl.inputs[1].default) == 'foo'

    assert len(cwl.outputs) == 2
    assert cwl.outputs[0].outputBinding != cwl.outputs[1].outputBinding


def test_explicit_outputs_and_std_output_streams(cli, client):
    """Test that unchanged std output streams can be marked with explicit
    outputs"""
    exit_code, _ = cli('run', 'sh', '-c', 'echo foo > bar')
    assert exit_code == 0

    exit_code, _ = cli('run', 'sh', '-c', 'echo foo > bar')
    assert exit_code == 1

    exit_code, _ = cli('run', '--output', 'bar', 'sh', '-c', 'echo foo > bar')
    assert exit_code == 0


def test_output_directory_with_output_option(cli, client):
    """Test output directories are not deleted with --output"""
    A_SCRIPT = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    cli('run', *A_SCRIPT, 'outdir', 'foo')

    exit_code, _ = cli('run', '--output', 'outdir', *A_SCRIPT, 'outdir', 'bar')

    assert exit_code == 0
    assert (client.path / 'outdir' / 'foo').exists()
    assert (client.path / 'outdir' / 'bar').exists()


def test_output_directory_without_separate_outputs(cli, client):
    """Output files in directory are not listed as separate outputs.

    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    A_SCRIPT = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, cwl = cli('run', *A_SCRIPT, 'outdir', 'foo')

    assert exit_code == 0
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].type == 'Directory'


def test_explicit_inputs_must_exist(cli):
    """Test explicit inputs exist before run"""
    exit_code, _ = cli('run', '--input', 'foo', 'touch', 'bar')

    assert exit_code == 1


def test_explicit_inputs_are_inside_repo(cli):
    """Test explicit inputs are inside the Renku repo"""
    exit_code, _ = cli('run', '--input', '/tmp', 'touch', 'foo')

    assert exit_code == 1


def test_explicit_inputs_and_outputs_are_listed(cli, client):
    """Test explicit inputs and outputs will be in generated CWL file"""
    cli('run', 'mkdir', 'foo')
    cli('run', 'touch', 'bar', 'baz')

    exit_code, cwl = cli(
        'run', '--input', 'foo', '--input', 'bar', '--output', 'baz', 'echo'
    )

    assert exit_code == 0
    assert len(cwl.inputs) == 2
    cwl.inputs.sort(key=lambda e: e.type)
    assert str(cwl.inputs[0].default) == '../../foo'
    assert cwl.inputs[0].type == 'Directory'
    assert cwl.inputs[0].inputBinding is None
    assert str(cwl.inputs[1].default) == '../../bar'
    assert cwl.inputs[1].type == 'File'
    assert cwl.inputs[1].inputBinding is None
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].outputBinding.glob == 'baz'


def test_explicit_inputs_can_be_in_inputs(cli):
    """Test explicit inputs that are in inputs are treated as normal inputs"""
    cli('run', 'touch', 'foo')

    exit_code, cwl = cli('run', '--input', 'foo', '--no-output', 'ls', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert str(cwl.inputs[0].default) == '../../foo'
    assert cwl.inputs[0].type == 'File'
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
    assert exit_code == 0

    # Status must be dirty if foo/bar changes
    cli('run', 'sh', '-c', 'echo "new changes" > foo/bar')
    exit_code, _ = cli('status')
    assert exit_code == 1

    exit_code, cwl = cli('update')
    assert exit_code == 0
    assert (client.path / 'foo' / 'bar').exists()
    assert (client.path / 'script.sh').exists()
    assert (client.path / 'output').exists()
