# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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

from renku.models.cwl import CWLClass


@pytest.fixture
def cli(client, run):
    """
    Return a function that runs the provided command and returns the exit code
    and content of the resulting CWL command line tool.
    """

    def renku_cli(*args):
        before_cwl_files = set(client.workflow_path.glob('*.cwl'))
        exit_code = run(args)
        after_cwl_files = set(client.workflow_path.glob('*.cwl'))
        new_files = after_cwl_files - before_cwl_files
        assert len(new_files) <= 1
        content = read_cwl_file(new_files.pop()) if new_files else None
        return exit_code, content

    return renku_cli


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
    exit_code, cwl = cli('run', 'touch', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert cwl.inputs[0].default == 'foo'
    assert cwl.inputs[0].type == 'string'
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].type == 'File'
    assert cwl.outputs[0].outputBinding.glob == '$(inputs.input_1)'


def test_run_fails_when_no_change_in_outputs_is_detected(cli):
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', 'ls', 'foo')

    assert exit_code == 1


def test_run_succeeds_with_no_output_option_even_if_no_change_in_outputs_is_detected(  # noqa: E501
    cli
):
    cli('run', 'touch', 'foo')
    exit_code, cwl = cli('run', '--no-output', 'touch', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert cwl.inputs[0].type == 'File'
    assert str(cwl.inputs[0].default) == '../../foo'
    assert len(cwl.outputs) == 0


def test_runs_with_and_without_no_output_option_yield_different_cwl_tools(
    cli, client
):
    cli('run', 'touch', 'foo')
    cli('run', '--no-output', 'touch', 'foo')

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert len(cwls) == 2
    assert cwls[0].inputs != cwls[1].inputs
    assert cwls[0].outputs != cwls[1].outputs


@pytest.mark.parametrize(
    'command,expected_type', [(('touch', ), 'File'),
                              (('mkdir', '-p'), 'Directory')]
)
def test_run_succeeds_with_explicit_output_option_even_if_no_change_in_outputs_is_detected(  # noqa: E501
    cli, command, expected_type
):
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


def test_run_with_explicit_output_yield_same_results_as_normal_run_with_detected_output(  # noqa: E501
    cli, client
):
    cli('run', 'touch', 'foo')
    cli('run', '--output', 'foo', 'touch', 'foo')

    cwls = read_all_cwl_files(client)

    # There should be two command line tool.
    assert len(cwls) == 2
    assert cwls[0].inputs == cwls[1].inputs
    assert cwls[0].outputs == cwls[1].outputs


def test_some_test(cli, client):
    A_SCRIPT = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, cwl = cli('run', *A_SCRIPT, 'outdir', 'foo')

    assert exit_code == 0
    assert (client.path / 'outdir' / 'foo').exists()


def test_explicit_directory_outputs_are_not_deleted_before_run(cli, client):
    A_SCRIPT = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    cli('run', *A_SCRIPT, 'outdir', 'foo')

    exit_code, _ = cli('run', '--output', 'outdir', *A_SCRIPT, 'outdir', 'bar')

    assert exit_code == 0
    assert (client.path / 'outdir' / 'foo').exists()
    assert (client.path / 'outdir' / 'bar').exists()


def test_output_files_in_directory_are_not_listed_as_separate_outputs(
    cli, client
):
    """
    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    A_SCRIPT = ('sh', '-c', 'mkdir -p "$0"; touch "$0/$1"')
    exit_code, cwl = cli('run', *A_SCRIPT, 'outdir', 'foo')

    assert exit_code == 0
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].type == 'Directory'


def test_run_fails_if_explicit_inputs_do_not_exist(cli):
    exit_code, _ = cli('run', '--input', 'foo', 'touch', 'bar')

    assert exit_code == 1


def test_run_fails_if_explicit_inputs_are_outside_the_renku_repo(cli):
    exit_code, _ = cli('run', '--input', '/tmp', 'touch', 'foo')

    assert exit_code == 1


def test_all_explicit_inputs_and_outputs_are_list_in_cwl_tool(cli, client):
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


def test_explicit_inputs_that_are_also_in_inputs_are_treated_as_normal_inputs(
    cli
):
    cli('run', 'touch', 'foo')

    exit_code, cwl = cli('run', '--input', 'foo', '--no-output', 'ls', 'foo')

    assert exit_code == 0
    assert len(cwl.inputs) == 1
    assert str(cwl.inputs[0].default) == '../../foo'
    assert cwl.inputs[0].type == 'File'
    assert cwl.inputs[0].inputBinding is not None


def test_run_succeeds_when_repo_is_dirty_but_explicit_output_is_passed(
    cli, client
):
    # make repo dirty by adding an untracked file
    with (client.path / 'untracked').open('w') as fp:
        fp.write('something')

    exit_code, cwl = cli('run', '--output', 'foo', 'touch', 'foo')

    assert exit_code == 0
    assert len(cwl.outputs) == 1
    assert cwl.outputs[0].outputBinding.glob == 'foo'
