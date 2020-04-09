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
"""Renku service jobs view tests."""
import copy
import uuid
from datetime import datetime

import pytest
from marshmallow.utils import isoformat


@pytest.mark.service
def test_jobs_view_identity_protected(svc_client):
    """Check serving of user requested jobs."""
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }
    response = svc_client.get('/jobs', headers=headers)

    assert {'error'} == set(response.json.keys())
    assert ('user identification is incorrect or missing'
            ) == response.json['error']['reason']


@pytest.mark.service
def test_jobs_view_empty_result(svc_client):
    """Check empty result for user requested jobs."""
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
        'Renku-User-Id': 'user'
    }
    response = svc_client.get('/jobs', headers=headers)

    assert {'result'} == set(response.json.keys())
    assert [] == response.json['result']['jobs']


@pytest.mark.service
def test_jobs_view_expected_job(svc_client_cache):
    """Check non-empty result for user requested job."""
    svc_client, headers, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'user'})
    job_data = {
        'job_id': uuid.uuid4().hex,
        'state': 'CREATED',
        'created_at': isoformat(datetime.now()),
        'updated_at': isoformat(datetime.now()),
        'extras': {
            'progress': 42
        }
    }
    job = cache.make_job(user, job_data)

    response = svc_client.get('/jobs', headers=headers)
    assert 1 == len(response.json['result']['jobs'])
    assert {
        'job_id',
        'state',
        'created_at',
        'updated_at',
        'extras',
    } == set(response.json['result']['jobs'][0].keys())

    cache.invalidate_job(user, job.job_id)
    response = svc_client.get('/jobs', headers=headers)
    assert 0 == len(response.json['result']['jobs'])


@pytest.mark.service
def test_jobs_view_check_exclusion(svc_client_cache):
    """Check non-empty result for user requested jobs."""
    svc_client, headers, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'user'})
    excluded_user = cache.ensure_user({'user_id': 'excluded_user'})

    for _ in range(10):
        job_data = {
            'job_id': uuid.uuid4().hex,
            'state': 'CREATED',
            'created_at': isoformat(datetime.utcnow()),
            'updated_at': isoformat(datetime.utcnow()),
            'extras': {
                'progress': 42
            }
        }
        job1 = cache.make_job(user, job_data)
        assert job1

        new_job = copy.deepcopy(job_data)
        new_job['job_id'] = uuid.uuid4().hex

        job2 = cache.make_job(excluded_user, new_job)
        assert job2

        assert job1.job_id != job2.job_id

    response = svc_client.get('/jobs', headers=headers)

    assert {'result'} == set(response.json.keys())
    assert 10 == len(response.json['result']['jobs'])
    for job in response.json['result']['jobs']:
        assert {'job_id', 'state', 'created_at', 'updated_at',
                'extras'} == set(job.keys())


@pytest.mark.service
def test_job_details_auth(svc_client):
    """Check authorization for listing a specific job."""
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }
    response = svc_client.get('/jobs/myjob', headers=headers)

    assert {'error'} == set(response.json.keys())
    assert ('user identification is incorrect or missing'
            ) == response.json['error']['reason']


@pytest.mark.service
def test_job_details_empty(svc_client):
    """Check job details for a user."""
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
        'Renku-User-Id': 'user'
    }
    response = svc_client.get('/jobs/myjob', headers=headers)

    assert {'result'} == set(response.json.keys())
    assert response.json['result'] is None


@pytest.mark.service
def test_job_details_2user(svc_client_cache):
    """Check job details for a user."""
    svc_client, headers, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'user'})
    jobs = [{
        'job_id': uuid.uuid4().hex,
        'state': 'CREATED',
        'created_at': isoformat(datetime.now()),
        'updated_at': isoformat(datetime.now()),
        'extras': {
            'progress': 42
        }
    } for _ in range(10)]

    for job_data in jobs:
        cache.make_job(user, job_data)

    user_headers = copy.deepcopy(headers)
    headers['Renku-User-Id'] = 'excluded_user'
    excluded_user_headers = copy.deepcopy(headers)

    for job in jobs:
        response = svc_client.get(
            '/jobs/{0}'.format(job['job_id']), headers=user_headers
        )
        assert response
        assert job['job_id'] == response.json['result']['job_id']

        response = svc_client.get(
            '/jobs/{0}'.format(job['job_id']), headers=excluded_user_headers
        )
        assert response.json['result'] is None
