# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
"""Renku service dataset view tests."""
import io
import json
import re
import uuid

import pytest
from flaky import flaky

from renku.service.config import INVALID_HEADERS_ERROR_CODE, \
    INVALID_PARAMS_ERROR_CODE, RENKU_EXCEPTION_ERROR_CODE


def assert_rpc_response(response, with_key='result'):
    """Check rpc result in response."""
    response_text = re.sub(
        r'http\S+',
        '',
        json.dumps(response.json),
    )

    assert with_key in response_text


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_commit_msg(svc_client_with_repo):
    """Create a new dataset successfully with custom commit message."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
        'commit_message': 'my awesome dataset'
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view_dataset_exists(svc_client_with_repo):
    """Create a new dataset which already exists."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': 'mydataset',
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response, with_key='error')

    assert RENKU_EXCEPTION_ERROR_CODE == response.json['error']['code']
    assert 'Dataset exists' in response.json['error']['reason']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view_unknown_param(svc_client_with_repo):
    """Create new dataset by specifying unknown parameters."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': 'mydata',
        'remote_name': 'origin'
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response, with_key='error')

    assert INVALID_PARAMS_ERROR_CODE == response.json['error']['code']
    assert {'remote_name'} == set(response.json['error']['reason'].keys())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_with_no_identity(svc_client_with_repo):
    """Create a new dataset with no identification provided."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': 'mydata',
        'remote_name': 'origin',
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers={'Content-Type': headers['Content-Type']}
        # no user identity, expect error
    )

    assert response
    assert_rpc_response(response, with_key='error')

    assert INVALID_HEADERS_ERROR_CODE == response.json['error']['code']

    err_message = 'user identification is incorrect or missing'
    assert err_message == response.json['error']['reason']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_view_with_no_identity(svc_client_with_repo):
    """Check identity error raise in dataset add."""
    svc_client, headers, project_id = svc_client_with_repo
    payload = {
        'project_id': project_id,
        'dataset_name': 'mydata',
        'remote_name': 'origin',
    }

    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers={'Content-Type': headers['Content-Type']}
        # no user identity, expect error
    )
    assert response
    assert_rpc_response(response, with_key='error')

    assert INVALID_HEADERS_ERROR_CODE == response.json['error']['code']

    err_message = 'user identification is incorrect or missing'
    assert err_message == response.json['error']['reason']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_view(svc_client_with_repo):
    """Check adding of uploaded file to dataset."""
    svc_client, headers, project_id = svc_client_with_repo
    content_type = headers.pop('Content-Type')

    response = svc_client.post(
        '/cache.files_upload',
        data=dict(file=(io.BytesIO(b'this is a test'), 'datafile1.txt'), ),
        query_string={'override_existing': True},
        headers=headers
    )

    assert response
    assert 200 == response.status_code
    assert_rpc_response(response)

    assert 1 == len(response.json['result']['files'])

    file_id = response.json['result']['files'][0]['file_id']
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
        'create_dataset': True,
        'files': [{
            'file_id': file_id,
        }, ]
    }
    headers['Content-Type'] = content_type

    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'project_id',
            'files'} == set(response.json['result'].keys())

    assert 1 == len(response.json['result']['files'])
    assert file_id == response.json['result']['files'][0]['file_id']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_commit_msg(svc_client_with_repo):
    """Check adding of uploaded file to dataset with custom commit message."""
    svc_client, headers, project_id = svc_client_with_repo
    content_type = headers.pop('Content-Type')

    response = svc_client.post(
        '/cache.files_upload',
        data=dict(file=(io.BytesIO(b'this is a test'), 'datafile1.txt'), ),
        query_string={'override_existing': True},
        headers=headers
    )

    file_id = response.json['result']['files'][0]['file_id']
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        'commit_message': 'my awesome data file',
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
        'create_dataset': True,
        'files': [{
            'file_id': file_id,
        }, ]
    }
    headers['Content-Type'] = content_type
    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'project_id',
            'files'} == set(response.json['result'].keys())

    assert 1 == len(response.json['result']['files'])
    assert file_id == response.json['result']['files'][0]['file_id']


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_datasets_view(svc_client_with_repo):
    """Check listing of existing datasets."""
    svc_client, headers, project_id = svc_client_with_repo

    params = {
        'project_id': project_id,
    }

    response = svc_client.get(
        '/datasets.list',
        query_string=params,
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'datasets'} == set(response.json['result'].keys())
    assert 0 != len(response.json['result']['datasets'])
    assert {'identifier', 'name', 'version',
            'created'} == set(response.json['result']['datasets'][0].keys())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_datasets_view_no_auth(svc_client_with_repo):
    """Check listing of existing datasets with no auth."""
    svc_client, headers, project_id = svc_client_with_repo

    params = {
        'project_id': project_id,
    }

    response = svc_client.get(
        '/datasets.list',
        query_string=params,
    )

    assert response
    assert_rpc_response(response, with_key='error')


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_and_list_datasets_view(svc_client_with_repo):
    """Create and list created dataset."""
    svc_client, headers, project_id = svc_client_with_repo

    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response

    assert_rpc_response(response)
    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']

    params_list = {
        'project_id': project_id,
    }

    response = svc_client.get(
        '/datasets.list',
        query_string=params_list,
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'datasets'} == set(response.json['result'].keys())
    assert 0 != len(response.json['result']['datasets'])
    assert {'identifier', 'name', 'version',
            'created'} == set(response.json['result']['datasets'][0].keys())

    assert payload['dataset_name'] in [
        ds['name'] for ds in response.json['result']['datasets']
    ]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_dataset_files(svc_client_with_repo):
    """Check listing of dataset files"""
    svc_client, headers, project_id = svc_client_with_repo
    content_type = headers.pop('Content-Type')

    file_name = '{0}'.format(uuid.uuid4().hex)
    response = svc_client.post(
        '/cache.files_upload',
        data=dict(file=(io.BytesIO(b'this is a test'), file_name), ),
        query_string={'override_existing': True},
        headers=headers
    )

    assert response
    assert 200 == response.status_code
    assert_rpc_response(response)

    assert 1 == len(response.json['result']['files'])
    file_id = response.json['result']['files'][0]['file_id']
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        'project_id': project_id,
        'dataset_name': 'mydata',
        'files': [{
            'file_id': file_id
        }, ],
    }
    headers['Content-Type'] = content_type

    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files',
            'project_id'} == set(response.json['result'].keys())
    assert file_id == response.json['result']['files'][0]['file_id']

    params = {
        'project_id': project_id,
        'dataset_name': 'mydata',
    }

    response = svc_client.get(
        '/datasets.files_list',
        query_string=params,
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files'} == set(response.json['result'].keys())

    assert params['dataset_name'] == response.json['result']['dataset_name']
    assert file_name in [
        file['name'] for file in response.json['result']['files']
    ]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_with_unpacked_archive(datapack_zip, svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id = svc_client_with_repo
    content_type = headers.pop('Content-Type')

    response = svc_client.post(
        '/cache.files_upload',
        data=dict(
            file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),
        ),
        query_string={
            'unpack_archive': True,
            'override_existing': True,
        },
        headers=headers
    )

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert response.json['result']['files']

    mm = {}
    for file_ in response.json['result']['files']:
        assert not file_['is_archive']
        assert not file_['unpack_archive']

        file_id = file_['file_id']
        assert file_id
        mm[file_['file_name']] = file_

    file_ = mm['file2']
    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
    }

    headers['Content-Type'] = content_type
    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']

    payload = {
        'project_id': project_id,
        'dataset_name': payload['dataset_name'],
        'files': [{
            'file_id': file_['file_id']
        }, ]
    }

    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files',
            'project_id'} == set(response.json['result'].keys())
    assert file_['file_id'] == response.json['result']['files'][0]['file_id']

    params = {
        'project_id': project_id,
        'dataset_name': payload['dataset_name'],
    }

    response = svc_client.get(
        '/datasets.files_list',
        query_string=params,
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files'} == set(response.json['result'].keys())

    assert params['dataset_name'] == response.json['result']['dataset_name']
    assert file_['file_name'] in [
        file['name'] for file in response.json['result']['files']
    ]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_with_unpacked_archive_all(datapack_zip, svc_client_with_repo):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, project_id = svc_client_with_repo
    content_type = headers.pop('Content-Type')

    response = svc_client.post(
        '/cache.files_upload',
        data=dict(
            file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),
        ),
        query_string={
            'unpack_archive': True,
            'override_existing': True,
        },
        headers=headers
    )

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert response.json['result']['files']

    mm = {}
    for file_ in response.json['result']['files']:
        assert not file_['is_archive']
        assert not file_['unpack_archive']

        file_id = file_['file_id']
        assert file_id
        mm[file_['file_name']] = file_

    file_ = mm['file2']

    files = [{
        'file_id': file_['file_id']
    } for file_ in response.json['result']['files']]

    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
    }

    headers['Content-Type'] = content_type
    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']

    payload = {
        'project_id': project_id,
        'dataset_name': payload['dataset_name'],
        'files': files,
    }

    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files',
            'project_id'} == set(response.json['result'].keys())
    assert files == response.json['result']['files']

    params = {
        'project_id': project_id,
        'dataset_name': payload['dataset_name'],
    }

    response = svc_client.get(
        '/datasets.files_list',
        query_string=params,
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files'} == set(response.json['result'].keys())

    assert params['dataset_name'] == response.json['result']['dataset_name']
    assert file_['file_name'] in [
        file['name'] for file in response.json['result']['files']
    ]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_existing_file(svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id = svc_client_with_repo
    payload = {
        'project_id': project_id,
        'dataset_name': '{0}'.format(uuid.uuid4().hex),
    }

    response = svc_client.post(
        '/datasets.create',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert {'dataset_name'} == set(response.json['result'].keys())
    assert payload['dataset_name'] == response.json['result']['dataset_name']

    files = [{'file_path': 'README.md'}]
    payload = {
        'project_id': project_id,
        'dataset_name': payload['dataset_name'],
        'files': files,
    }
    response = svc_client.post(
        '/datasets.add',
        data=json.dumps(payload),
        headers=headers,
    )
    assert response
    assert_rpc_response(response)

    assert {'dataset_name', 'files',
            'project_id'} == set(response.json['result'].keys())

    assert files == response.json['result']['files']
