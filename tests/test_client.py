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

    monkeypatch.setenv('RENGA_ENDPOINT', client.api.endpoint)
    monkeypatch.setenv('RENGA_ACCESS_TOKEN', client.api.token['access_token'])

    env_client = renga.from_env()
    assert client.api.endpoint == env_client.api.endpoint
    assert client.api.headers == env_client.api.headers


def test_auto_refresh(projects_responses):
    """Test automatic token refresh."""
    client = renga.RengaClient(
        'https://example.com',
        token={
            'access_token': 'expired',
            'expires_at': 1,
            'refresh_token': 'refreshtoken',
        }, )

    url = 'https://example.com/api/projects'
    data = b'{"name": "test-project"}'
    response = client.api.request('POST', url, data=data)

    assert client.api.token['access_token'] == 'accessdemo'


def test_client_projects(renga_client, projects_responses):
    """Test client for managing projects."""
    project = renga_client.projects.create('test-project')

    assert "<Project '1234'>" == str(project)
    assert project.id == '1234'
    assert project.name == 'test-project'

    projects = renga_client.projects.list()
    assert projects[0].id == project.id
    assert project.id == renga_client.projects[project.id].id


def test_client_invalid_requests(renga_client, projects_responses):
    """Test invalid client responses."""
    with pytest.raises(renga.errors.RengaException):
        renga_client.projects[0]


def test_client_contexts(renga_client, deployer_responses, storage_responses,
                         monkeypatch):
    """Test client for managing contexts."""
    monkeypatch.setenv('RENGA_CONTEXT_ID', 'abcd')

    context = renga_client.contexts.create(image='hello-world')
    assert context.id == 'abcd'
    assert context.spec['image'] == 'hello-world'

    # TODO separate context.inputs to new test case
    with pytest.raises(KeyError):
        context.inputs['invalid']

    with pytest.raises(KeyError):
        context.inputs['no_default']

    with pytest.raises(KeyError):
        context.outputs['result']

    monkeypatch.setenv('RENGA_CONTEXT_INPUTS_NO_DEFAULT', '9876')
    assert context.inputs['no_default'].id == 9876
    assert context.inputs['with_default'].id == 9876

    monkeypatch.setenv('RENGA_CONTEXT_OUTPUTS_RESULT', '9876')
    assert context.outputs['result'].id == 9876

    assert context.id == renga_client.contexts['abcd'].id
    assert context.id == renga_client.current_context.id

    contexts = renga_client.contexts.list()
    assert contexts
    assert contexts[0].id == 'abcd'
    assert contexts[0].spec['image'] == 'hello-world'

    execution = context.run(engine='docker')
    assert execution.id == 'efgh'
    assert execution.engine == 'docker'
    assert 'Hello world!' in execution.logs()
    assert execution.ports == []
    assert execution.stop()

    executions = context.executions.list()
    assert executions[0].id == 'efgh'
    assert executions[0].engine == 'docker'


def test_client_buckets(renga_client, storage_responses):
    """Test client for managing buckets and files."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    file_ = bucket.files.create(filename='hello')
    assert file_.id == 9876
    assert file_.filename == 'hello'

    with file_.open('w') as fp:
        fp.write(b'hello world')

    with file_.open('r') as fp:
        assert fp.read() == b'hello world'


def test_client_buckets_shortcut(renga_client, storage_responses):
    """Test shortcut for creating file on a bucket."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    with bucket.files.open('hello', 'w') as fp:
        fp.write(b'hello world')

    file_ = bucket.files[fp.id]

    with file_.open('r') as fp:
        assert fp.read() == b'hello world'


def test_bucket_listing(renga_client, explorer_responses):
    """Test storage explorer client."""
    buckets = renga_client.buckets.list()
    assert buckets[0].id == 1234
    assert buckets[1].id == 5678

    assert renga_client.buckets[1234].id == 1234


def test_file_renaming(renga_client, storage_responses):
    """Test file renaming."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    file_ = bucket.files.create(filename='hello')
    assert file_.id == 9876
    assert file_.filename == 'hello'

    file_.filename = 'hello-2'
    assert file_.filename == 'hello-2'

    file_ = bucket.files[9876]
    assert file_.filename == 'hello-2'


def test_file_cloning(renga_client, storage_responses):
    """Test file cloning."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    file_ = bucket.files.create(filename='hello')

    with file_.open('w') as fp:
        fp.write(b'hello world')

    cloned_file = file_.clone()

    assert file_.id != cloned_file.id

    with cloned_file.open('r') as fp:
        assert fp.read() == b'hello world'


def test_file_versioning(renga_client, storage_responses, explorer_responses):
    """Test shortcut for creating file on a bucket."""
    bucket = renga_client.buckets.create(name='world', backend='local')
    assert bucket.id == 1234

    with bucket.files.open('hello', 'w') as fp:
        fp.write(b'hello world')
        file_id = fp.id

    file_ = bucket.files[file_id]

    assert len(file_.versions.list()) == 1

    with file_.open('w') as fp:
        fp.write(b'hello second')

    file_ = bucket.files[file_id]
    assert len(file_.versions.list()) == 2
    assert file_.versions.list()[-1].open('r').read() == b'hello world'
    assert file_.open('r').read() == b'hello second'
