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

import click
import pytest
from click.testing import CliRunner

from renga import cli


def test_init():
    """Test project initialization."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        # 0. must autosync
        result = runner.invoke(cli.cli, ['init', '--project', 'project'])
        assert result.exit_code == 2

        # 1. test projet directory creation
        result = runner.invoke(cli.cli, ['init', '--autosync', 'test-project'])
        assert result.exit_code == 0
        assert os.stat('test-project')
