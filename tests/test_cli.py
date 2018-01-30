# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""CLI tests."""

from __future__ import absolute_import, print_function

import os

import pytest
import responses

from renga import __version__, cli
from renga.cli._config import read_config, write_config


def test_version(base_runner):
    """Test cli version."""
    result = base_runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


@pytest.mark.parametrize('arg', (('help', ), ('-h', ), ('--help', )))
def test_help(arg, base_runner):
    """Test cli help."""
    result = base_runner.invoke(cli.cli, [arg])
    assert result.exit_code == 0
    assert 'Show this message and exit.' in result.output


def test_config_path(instance_path, base_runner):
    """Test config path."""
    result = base_runner.invoke(cli.cli, ['--config-path'])
    output = result.output.split('\n')[0]
    assert 'config.yml' in output
    assert instance_path in output


def test_init(base_runner):
    """Test project initialization."""
    runner = base_runner

    # 1. the directory must exist
    result = runner.invoke(cli.cli, ['init', 'test-project'])
    assert result.exit_code == 2

    # 2. test project directory creation
    os.mkdir('test-project')
    result = runner.invoke(cli.cli, ['init', 'test-project'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('test-project', '.git'))
    assert os.stat(os.path.join('test-project', '.renga'))

    # 3. test project init from directory
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init'])
    assert result.exit_code != 0

    result = runner.invoke(cli.cli, ['init', '--force'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('.git'))
    assert os.stat(os.path.join('.renga'))


def test_workon(runner):
    """Test switching branches."""
    # Create first issue
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['deactivate'])
    assert result.exit_code == 0

    # Enter existing
    result = runner.invoke(cli.cli, ['workon', '1'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['deactivate'])
    assert result.exit_code == 0


def test_run_simple(runner):
    """Test tracking of run command."""
    cmd = ['echo', 'test']
    result = runner.invoke(cli.cli, ['run', '--no-output'] + cmd)
    assert result.exit_code == 0


def test_dataset_import(base_runner, sample_file, test_project):
    """Test importing data into a dataset."""
    runner = base_runner

    os.mkdir('data')

    # providing a bad data directory should fail
    result = runner.invoke(cli.cli,
                           ['import', 'file', 'dataset', '--datadir', 'bla'])
    assert result.exit_code == 2

    # import data
    result = runner.invoke(cli.cli, ['import', str(sample_file), 'dataset'])
    assert result.exit_code == 0
    assert os.stat('data/dataset/sample_file')
    assert os.stat('data/dataset/dataset.meta.json')
