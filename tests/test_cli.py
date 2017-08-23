# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#
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
