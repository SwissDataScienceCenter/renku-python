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
from click.testing import CliRunner
import pytest

from renga import cli


def test_init():
    """Test project initialization."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        # 0. must autosync
        with pytest.raises(RuntimeError):
            runner.invoke(cli.init, ['--project', 'project'])

        # 1. test projet directory creation
        result = runner.invoke(cli.init,
                               ['--autosync', '--project', 'test-project'])
        assert os.stat('test-project')

        with pytest.raises(FileExistsError):
            runner.invoke(cli.init,
                          ['--autosync', '--project', 'test-project'])
