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

import pytest
from flaky import flaky
from tests.service.test_dataset_views import assert_rpc_response

from renku.service.jobs.cleanup import cache_files_cleanup, \
    cache_project_cleanup


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.jobs
@flaky(max_runs=30, min_passes=1)
def test_cleanup_old_files(
    datapack_zip, svc_client_with_repo, service_job, mock_redis
):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _ = svc_client_with_repo
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
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_cleanup_old_project(
    datapack_zip, svc_client_with_repo, service_job, mock_redis
):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _ = svc_client_with_repo
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
