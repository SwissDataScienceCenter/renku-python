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

import responses

from renga import __version__, cli
from renga.cli._config import APP_NAME


def test_version(base_runner):
    """Test cli version."""
    result = base_runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


def test_config_path(base_runner):
    """Test config path."""
    result = base_runner.invoke(cli.cli, ['--config-path'])
    assert APP_NAME.lower() in result.output.split('\n')[0].lower()


def test_login(base_runner, auth_responses):
    """Test login."""
    runner = base_runner
    result = runner.invoke(cli.cli, [
        'login', 'https://example.com', '--username', 'demo', '--password',
        'demo'
    ])
    assert result.exit_code == 0
    assert 'stored' in result.output

    result = runner.invoke(cli.cli, ['tokens'])
    assert result.exit_code == 0
    assert 'https://example.com: demodemo' in result.output.split('\n')

    result = runner.invoke(cli.cli, ['tokens', 'access'])
    assert result.exit_code == 0
    assert 'accessdemo' in result.output.split('\n')


def test_env(runner):
    """Test client creation."""
    result = runner.invoke(cli.cli, ['env'])
    assert result.exit_code == 0
    assert 'RENGA_ENDPOINT=https://example.com' in result.output


def test_init(runner, auth_responses, projects_responses):
    """Test project initialization."""
    # 0. must autosync
    result = runner.invoke(cli.cli, ['init'])
    assert result.exit_code == 2

    # 1. the directory must exist
    result = runner.invoke(cli.cli, ['init', '--autosync', 'test-project'])
    assert result.exit_code == 2

    # 2. test project directory creation
    os.mkdir('test-project')
    result = runner.invoke(cli.cli, ['init', '--autosync', 'test-project'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('test-project', '.renga'))

    # 3. test project init from directory
    os.chdir('test-project')
    result = runner.invoke(cli.cli, ['init', '--autosync'])
    assert result.exit_code == 2

    result = runner.invoke(cli.cli, ['init', '--autosync', '--force'])
    assert result.exit_code == 0
    assert os.stat(os.path.join('.renga'))


def test_storage_backends(runner, storage_responses):
    """Test storage backends."""
    result = runner.invoke(cli.cli, ['io', 'backends'])
    assert result.exit_code == 0
    assert 'local' in result.output


def test_storage_buckets(runner, storage_responses):
    """Test storage buckets."""
    result = runner.invoke(cli.cli, ['io', 'buckets', 'create'])
    assert result.exit_code == 2

    result = runner.invoke(cli.cli, ['io', 'buckets', 'create', 'bucket1'])
    assert result.exit_code == 0
    assert '1234' in result.output


def test_storage_buckets_in_project(runner, projects_responses,
                                    storage_responses, explorer_responses):
    """Test bucket creation in the project directory."""
    os.mkdir('test-project')
    os.chdir('test-project')

    result = runner.invoke(cli.cli, ['init', '--autosync'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['io', 'buckets', 'create', 'bucket1'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['io', 'buckets', 'list'])
    assert result.exit_code == 0
    assert '1234' in result.output

    result = runner.invoke(cli.cli, ['add', 'hello'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['add', 'hello'])
    assert result.exit_code == 2

    result = runner.invoke(cli.cli, ['io', 'buckets', 'files', '1234'])
    assert result.exit_code == 0
    assert 'hello' in result.output
