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
"""Pytest configuration."""

from __future__ import absolute_import, print_function

import shutil
import tempfile

import pytest
import requests
from click.testing import CliRunner


@pytest.fixture()
def instance_path():
    """Temporary instance path."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


@pytest.fixture()
def runner():
    """Create a runner on isolated filesystem."""
    cli_runner = CliRunner()
    with cli_runner.isolated_filesystem():
        yield cli_runner

r_get = requests.get
r_post = requests.post


class Response(object):
    """Fake response."""

    def __init__(self, data, status_code):
        """Initialize fake response object with a json."""
        self.data = data
        self.status_code = status_code

    def json(self):
        """Return json."""
        return self.data


@pytest.fixture()
def kg_requests(monkeypatch):
    """Monkeypatch requests to immitate the KnowledgeGraph."""
    mutation_url = 'http://localhost/api/mutation/mutation'
    named_type_url = 'http://localhost/api/types/management/named_type'

    def kg_post(*args, **kwargs):
        """Override requests.post for KG urls."""
        if mutation_url in args[0]:
            """Override /api/mutation/mutation."""
            return Response({'uuid': '1234'}, 201)
        else:
            return r_post(*args, **kwargs)

    def kg_get(*args, **kwargs):
        """Overrides requests.get for KG URLs."""
        if mutation_url in args[0]:
            """Override /api/mutation/mutation/uuid."""
            return Response({
                'status': 'completed',
                'response': {
                    'event': {
                        'status': 'success',
                        'results': [{
                            'id': 1234
                        }]
                    }
                }
            }, 200)

        elif named_type_url in args[0]:
            """Override /api/types/management/named_type."""
            return Response([{
                'name':
                'context',
                'properties': [{
                    'name': 'context_id',
                    'data_type': 'string',
                    'cardinality': 'single'
                }, {
                    'name': 'context_spec_image',
                    'data_type': 'string',
                    'cardinality': 'single'
                }, {
                    'name': 'context_spec_ports',
                    'data_type': 'string',
                    'cardinality': 'single'
                }]
            }, {
                'name':
                'execution',
                'properties': [{
                    'name': 'execution_id',
                    'data_type': 'string',
                    'cardinality': 'single'
                }, {
                    'name': 'execution_engine',
                    'data_type': 'string',
                    'cardinality': 'single'
                }, {
                    'name': 'execution_namespace',
                    'data_type': 'string',
                    'cardinality': 'single'
                }]
            }, {
                "name":
                "project",
                "properties": [{
                    "name": "project_name",
                    "data_type": "string",
                    "cardinality": "single"
                }]
            }], 200)
        else:
            return r_get(*args, **kwargs)

    monkeypatch.setattr(requests, 'get', kg_get)
    monkeypatch.setattr(requests, 'post', kg_post)
