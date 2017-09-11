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
"""Test Python SDK client."""

import pytest

import renga


def test_client(renga_client, monkeypatch):
    """Test client creation."""
    client = renga_client
    assert hasattr(client, 'api')

    monkeypatch.setenv('RENGA_ENDPOINT', 'https://example.com')
    monkeypatch.setenv('RENGA_ACCESS_TOKEN', 'accessdemo')

    env_client = renga.from_env()
    assert client.api.endpoint == env_client.api.endpoint
    assert client.api.headers == env_client.api.headers


def test_client_projects(renga_client, projects_responses):
    """Test client for managing projects."""
    project = renga_client.projects.create('test-project')

    assert project.id == '1234'
    assert project.name == 'test-project'


def test_client_contexts(renga_client, deployer_responses):
    """Test client for managing contexts."""
    context = renga_client.contexts.create(image='hello-world')

    assert context.id == 'abcd'
    assert context.spec['image'] == 'hello-world'


def test_client_buckets(renga_client, storage_responses):
    """Test client for managing contexts."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    file_ = bucket.create_file(file_name='hello')
    assert file_.id == 1234

    with file_.open('w') as fp:
        fp.write(b'hello world')

    with file_.open('r') as fp:
        assert fp.read() == b'hello world'
