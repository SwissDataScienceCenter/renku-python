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

"""Module tests."""

from __future__ import absolute_import, print_function

from flask import Flask

from renga import Renga


def test_version():
    """Test version import."""
    from renga import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = Renga(app)
    assert 'renga' in app.extensions

    app = Flask('testapp')
    ext = Renga()
    assert 'renga' not in app.extensions
    ext.init_app(app)
    assert 'renga' in app.extensions


def test_view(app):
    """Test view."""
    Renga(app)
    with app.test_client() as client:
        res = client.get("/")
        assert res.status_code == 200
        assert 'Welcome to Renga' in str(res.data)
