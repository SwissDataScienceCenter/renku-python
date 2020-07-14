# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service job tests."""
import io
import os
import uuid

import pytest
from flaky import flaky

from renku.service.jobs.cleanup import cache_files_cleanup, \
    cache_project_cleanup
from tests.service.views.test_dataset_views import assert_rpc_response


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.jobs
@flaky(max_runs=30, min_passes=1)
def test_cleanup_old_files(
    datapack_zip, svc_client_with_repo, service_job, mock_redis
):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _, _ = svc_client_with_repo
    headers.pop('Content-Type')

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
    assert 3 == len(response.json['result']['files'])

    cache_files_cleanup()
    response = svc_client.get('/cache.files_list', headers=headers)

    assert response
    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 0 == len(response.json['result']['files'])


@pytest.mark.service
@pytest.mark.jobs
def test_cleanup_files_old_keys(svc_client_cache, service_job, tmp_path):
    """Cleanup old project."""
    svc_client, headers, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'user'})
    mydata = tmp_path / 'mydata.json'
    mydata.write_text('1,2,3')

    file_upload = {
        'file_id': uuid.uuid4().hex,
        'content_type': 'application/json',
        'file_name': 'mydata.json',
        'file_size': 6,
        'is_archive': False,
        'is_dir': False,
        'unpack_archive': False,
        'relative_path': str(mydata)
    }
    cache.set_file(user, file_upload)

    response = svc_client.get('/cache.files_list', headers=headers)
    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json['result']['files'])

    cache_files_cleanup()
    response = svc_client.get('/cache.files_list', headers=headers)

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert 0 == len(response.json['result']['files'])


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_cleanup_old_project(
    datapack_zip, svc_client_with_repo, service_job, mock_redis
):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _, _ = svc_client_with_repo
    headers.pop('Content-Type')

    response = svc_client.get('/cache.project_list', headers=headers)

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json['result']['projects'])

    cache_project_cleanup()

    response = svc_client.get('/cache.project_list', headers=headers)

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 0 == len(response.json['result']['projects'])


@pytest.mark.service
@pytest.mark.jobs
def test_cleanup_project_old_keys(svc_client_cache, service_job):
    """Cleanup old project with old hset keys."""
    svc_client, headers, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'user'})
    project = {
        'project_id': uuid.uuid4().hex,
        'name': 'my-project',
        'fullname': 'full project name',
        'email': 'my@email.com',
        'owner': 'me',
        'token': 'awesome token',
        'git_url': 'git@gitlab.com'
    }
    project = cache.make_project(user, project)
    os.makedirs(str(project.abs_path), exist_ok=True)

    response = svc_client.get('/cache.project_list', headers=headers)
    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json['result']['projects'])

    cache_project_cleanup()
    response = svc_client.get('/cache.project_list', headers=headers)

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert 0 == len(response.json['result']['projects'])
