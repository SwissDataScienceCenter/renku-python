# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#

"""Pytest configuration."""

from __future__ import absolute_import, print_function

import shutil
import tempfile

import pytest


@pytest.yield_fixture()
def instance_path():
    """Temporary instance path."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)
