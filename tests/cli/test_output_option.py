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

import yaml

from renku.models.cwl import CWLClass


def test_without_an_output_option(runner, client, run):
    """Test detection of an output file without an output option."""
    assert 0 == run(args=('run', 'touch', 'hello.txt'))
    assert 1 == run(args=('run', 'touch', 'hello.txt'))
    assert 0 == run(args=('run', '--no-output', 'touch', 'hello.txt'))

    # There should be two command line tool.
    tools = list(client.workflow_path.glob('*_touch.cwl'))
    assert 2 == len(tools)

    cwls = []
    for tool_path in tools:
        with tool_path.open('r') as f:
            cwls.append(CWLClass.from_cwl(yaml.safe_load(f)))

    assert cwls[0].inputs != cwls[1].inputs
    assert cwls[0].outputs != cwls[1].outputs


def test_with_an_output_option(runner, client, run):
    """Test detection of an output file with an output option."""
    assert 0 == run(args=('run', 'touch', 'hello.txt'))
    assert 0 == run(
        args=('run', '--output', 'hello.txt', 'touch', 'hello.txt')
    )

    # There should be two command line tool.
    tools = list(client.workflow_path.glob('*_touch.cwl'))
    assert 2 == len(tools)

    cwls = []
    for tool_path in tools:
        with tool_path.open('r') as f:
            cwls.append(CWLClass.from_cwl(yaml.safe_load(f)))

    assert cwls[0].inputs == cwls[1].inputs
    assert cwls[0].outputs == cwls[1].outputs


def test_output_directory_with_output_option(runner, client, run):
    """Test directory cleanup with the output option."""
    base_sh = ['sh', '-c', 'mkdir -p "$0"; touch "$0/$1"']

    assert 0 == run(args=['run'] + base_sh + ['output', 'foo'])
    assert (client.path / 'output' / 'foo').exists()

    # The output directory is wronly detected.
    assert 1 == run(args=['run'] + base_sh + ['output', 'foo'])
    assert 0 == run(args=['run', '--no-output'] + base_sh + ['output', 'foo'])

    # There should be two command line tools.
    tools = list(client.workflow_path.glob('*_sh.cwl'))
    assert 2 == len(tools)

    cwls = []
    for tool_path in tools:
        with tool_path.open('r') as f:
            cwls.append(CWLClass.from_cwl(yaml.safe_load(f)))

    assert cwls[0].inputs != cwls[1].inputs
    assert cwls[0].outputs != cwls[1].outputs

    # The output directory is cleaned.
    assert 0 == run(
        args=['run', '--output', 'output'] + base_sh + ['output', 'bar']
    )
    assert not (client.path / 'output' / 'foo').exists()
    assert (client.path / 'output' / 'bar').exists()


def test_output_directory_without_separate_outputs(runner, client, run):
    """Output files in directory are not listed as separate outputs.

    See https://github.com/SwissDataScienceCenter/renku-python/issues/387
    """
    base_sh = ['sh', '-c', 'mkdir -p "$0"; touch "$0/$1"']

    assert 0 == run(args=['run'] + base_sh + ['output', 'foo'])
    assert (client.path / 'output' / 'foo').exists()

    # There should be only one command line tool.
    tools = list(client.workflow_path.glob('*_sh.cwl'))
    assert 1 == len(tools)

    with tools[0].open('r') as f:
        cwl = CWLClass.from_cwl(yaml.safe_load(f))

    assert 1 == len(cwl.outputs)
    assert 'Directory' == cwl.outputs[0].type
