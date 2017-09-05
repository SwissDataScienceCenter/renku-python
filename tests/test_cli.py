# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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


def test_version(runner):
    """Test cli version."""
    result = runner.invoke(cli.cli, ['--version'])
    assert __version__ in result.output.split('\n')


def test_config_path(runner):
    """Test config path."""
    result = runner.invoke(cli.cli, ['--config-path'])
    assert APP_NAME.lower() in result.output.split('\n')[0].lower()


def test_login(runner, auth_responses):
    """Test login."""
    result = runner.invoke(cli.cli, [
        'login', 'http://example.com',
        '--username', 'demo', '--password', 'demo'
    ])
    assert result.exit_code == 0
    assert 'stored' in result.output

    result = runner.invoke(cli.cli, ['tokens'])
    assert result.exit_code == 0
    assert 'http://example.com: demodemo' in result.output.split('\n')


def test_init(runner, auth_responses, projects_responses):
    """Test project initialization."""
    result = runner.invoke(cli.cli, [
        'login', 'http://example.com', '--username', 'demo', '--password',
        'demo'
    ])

    assert result.exit_code == 0
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
