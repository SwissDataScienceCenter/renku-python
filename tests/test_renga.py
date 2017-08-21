# -*- coding: utf-8 -*-
#
# This file is part of SDSC Platform.
# Copyright (C) 2017 Swiss Data Science Center.
#
# ADD LICENSE SHORT TEXT
#

"""Module tests."""

from __future__ import absolute_import, print_function


def test_version():
    """Test version import."""
    from renga import __version__
    assert __version__
