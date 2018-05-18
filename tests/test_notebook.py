# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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
from nbformat.notebooknode import from_dict

from renku.notebook import RenkuStorageManager


@pytest.fixture
def notebook():
    """Return a content of Jupyter notebook."""
    return {
        u'cells': [{
            u'metadata': {
                u'trusted': True,
            },
            u'cell_type': u'code',
            u'source': u'█',
            u'execution_count': None,
            u'outputs': []
        }],
        u'metadata': {},
        u'nbformat': 4,
        u'nbformat_minor': 2,
    }


def test_file_manager_browse(
    instance_path, renku_client, monkeypatch, deployer_responses,
    storage_responses, notebook
):
    """Test browsing in file manager."""
    client = renku_client
    client.buckets.create('bucket1')

    monkeypatch.setenv('RENKU_ENDPOINT', client.api.endpoint)
    monkeypatch.setenv('RENKU_ACCESS_TOKEN', client.api.token['access_token'])
    monkeypatch.setenv('RENKU_CONTEXT_ID', 'abcd')

    contents_manager = RenkuStorageManager()
    # Top level
    assert contents_manager.get('/')['name'] == 'Renku'
    # First level
    assert contents_manager.get('/buckets')['name'] == 'Buckets'
    assert contents_manager.get('/current_context')['name'
                                                    ] == 'Current Context'
    # Second level
    assert contents_manager.get('/buckets/1234')['name'] == 'bucket1'
    assert contents_manager.get('/current_context/inputs')['name'] == 'Inputs'
    assert contents_manager.get('/current_context/outputs')['name'
                                                            ] == 'Outputs'


def test_file_manager(
    instance_path, renku_client, monkeypatch, deployer_responses,
    storage_responses, notebook
):
    """Test file manager."""
    client = renku_client
    client.buckets.create('bucket1')

    monkeypatch.setenv('RENKU_ENDPOINT', client.api.endpoint)
    monkeypatch.setenv('RENKU_ACCESS_TOKEN', client.api.token['access_token'])
    monkeypatch.setenv('RENKU_CONTEXT_ID', 'abcd')

    contents_manager = RenkuStorageManager()
    contents_manager._save_notebook(
        'current_context/inputs/notebook', from_dict(notebook)
    )

    notebook_model = contents_manager.get('current_context/inputs/notebook')
    assert notebook_model['content']['cells'][0]['source'] == notebook[
        'cells'
    ][0]['source']

    new_path = '1.ipynb'
    notebook_model['path'] = 'current_context/inputs/' + new_path
    contents_manager.update(notebook_model, 'current_context/inputs/notebook')
    assert client.buckets[1234].files[9876].filename == new_path

    notebook_model = contents_manager.get('buckets/1234/9876')
    assert notebook_model['name'] == new_path

    new_path = '2.ipynb'
    notebook_model['path'] = 'buckets/1234/' + new_path
    contents_manager.update(notebook_model, 'buckets/1234/9876')
    assert client.buckets[1234].files[9876].filename == new_path

    # Move to different bucket:
    new_path = '3.ipynb'
    notebook_model['path'] = 'buckets/2345/' + new_path
    with pytest.raises(RuntimeError):
        contents_manager.update(notebook_model, 'buckets/1234/9876')


def test_file_manager_save(
    instance_path, renku_client, monkeypatch, deployer_responses,
    storage_responses, notebook
):
    """Test file saving manager."""
    client = renku_client

    monkeypatch.setenv('RENKU_ENDPOINT', client.api.endpoint)
    monkeypatch.setenv('RENKU_ACCESS_TOKEN', client.api.token['access_token'])
    monkeypatch.setenv('RENKU_CONTEXT_ID', 'abcd')

    contents_manager = RenkuStorageManager()

    with pytest.raises(Exception):
        bucket_model = contents_manager.save({
            'type': 'directory',
        }, 'world')

    bucket_model = contents_manager.save({
        'type': 'directory',
    }, 'buckets/world')
    assert bucket_model['path'] == '/buckets/1234'

    with pytest.raises(Exception):
        bucket_model = contents_manager.save({
            'type': 'directory',
        }, 'buckets/1234')

    # Not in a bucket
    with pytest.raises(Exception):
        file_model = contents_manager.save({
            'type': 'file',
            'format': 'text',
            'content': u'█',
        }, 'hello.ipydb')

    file_model = contents_manager.save({
        'type': 'file',
        'format': 'text',
        'content': u'█',
    }, 'buckets/1234/hello.ipydb')
    assert file_model['path'] == '/buckets/1234/9876'

    contents_manager.save({
        'type': 'file',
        'format': 'text',
        'content': u'██'.encode('utf-8'),
    }, 'buckets/1234/9876')

    # Missing type
    with pytest.raises(Exception):
        contents_manager.save({}, 'buckets/1234/9876')

    # Invalid type
    with pytest.raises(Exception):
        contents_manager.save({
            'type': 'invalid',
            'content': None
        }, 'buckets/1234/9876')

    # Missing content
    with pytest.raises(Exception):
        contents_manager.save({'type': 'notebook'}, 'buckets/1234/9876')

    # Save the raw file
    with pytest.raises(Exception):
        contents_manager.save({
            'type': 'file',
            'content': u'█',
        }, 'buckets/1234/9876')

    # Can not store outside a bucket
    with pytest.raises(Exception):
        contents_manager.save({
            'type': 'notebook',
            'content': notebook
        }, 'buckets/')

    # Can not store outside a bucket
    with pytest.raises(Exception):
        contents_manager.save({
            'type': 'notebook',
            'content': notebook
        }, 'buckets/1234')

    notebook_model = contents_manager.save({
        'type': 'notebook',
        'content': notebook
    }, 'buckets/1234/9876')
    assert notebook_model['content'] is None

    notebook_model = contents_manager.save({
        'type': 'notebook',
        'content': notebook
    }, 'buckets/1234/new_notebook.ipynb')
    assert notebook_model['path'] == '/buckets/1234/9875'
    assert notebook_model['content'] is None
