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
"""Test service cache."""
import datetime
import uuid

import pytest


def test_service_cache_ensure_user(svc_client_cache):
    """Test service cache user creation."""
    client, _, cache = svc_client_cache
    expected_id = uuid.uuid4().hex
    user = cache.ensure_user({'user_id': expected_id})

    assert user
    assert expected_id == user.user_id


def test_service_cache_get_users(svc_client_cache):
    """Test getting multiple users."""
    client, _, cache = svc_client_cache

    expected_users = set([
        cache.ensure_user({
            'user_id': uuid.uuid4().hex
        }).user_id for _ in range(10)
    ])

    received_users = set([user.user_id for user in cache.get_users()])

    assert expected_users.issubset(received_users)


def test_service_cache_get_user(svc_client_cache):
    """Test getting a single user."""
    client, _, cache = svc_client_cache
    expected_id = uuid.uuid4().hex
    expected_user = cache.ensure_user({'user_id': expected_id})
    assert expected_user

    received_user = cache.get_user(expected_id)
    assert received_user

    assert expected_user.user_id == received_user.user_id
    assert expected_user.fullname == received_user.fullname
    assert expected_user.email == received_user.email
    assert expected_user.token == received_user.token


def test_service_cache_make_job(svc_client_cache):
    """Test service cache jobs."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'testuser'})
    job_data = {
        'job_id': uuid.uuid4().hex,
    }

    job = cache.make_job(user, job_data)

    assert job

    assert job_data['job_id'] == job.job_id
    assert job_data['user_id'] == job.user_id

    assert isinstance(job.created_at, datetime.datetime)
    assert isinstance(job.updated_at, datetime.datetime)

    assert 'ENQUEUED' == job.state
    assert job.extras is None


def test_service_cache_get_job(svc_client_cache):
    """Test service get user job."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'testuser'})
    job_data = {
        'job_id': uuid.uuid4().hex,
    }
    job = cache.make_job(user, job_data)
    assert job

    retrieved_job = cache.get_job(user, job.job_id)
    assert retrieved_job

    assert job.created_at == retrieved_job.created_at
    assert job.updated_at == retrieved_job.updated_at
    assert job.job_id == retrieved_job.job_id
    assert job.user_id == retrieved_job.user_id
    assert job.state == retrieved_job.state
    assert job.extras == retrieved_job.extras


def test_service_cache_get_jobs(svc_client_cache):
    """Test service get user jobs."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'testuser1'})
    user2 = cache.ensure_user({'user_id': 'testuser2'})

    for _ in range(10):
        job = cache.make_job(user, {
            'job_id': uuid.uuid4().hex,
        })
        assert job

    job2 = cache.make_job(user2, {
        'job_id': uuid.uuid4().hex,
    })
    assert job2

    retrieved_jobs = cache.get_jobs(user)
    assert retrieved_jobs
    assert 10 == len([job for job in retrieved_jobs])
    assert 1 == len([job for job in cache.get_jobs(user2)])


def test_service_cache_get_job_none(svc_client_cache):
    """Test service get user jobs."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': 'testuser'})
    job_data = {
        'job_id': uuid.uuid4().hex,
    }
    job = cache.make_job(user, job_data)

    assert job
    assert cache.get_job(user, None) is None

    with pytest.raises(AttributeError):
        cache.get_job(None, job_data['job_id'])


def test_service_cache_set_file(svc_client_cache):
    """Test service set user file."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': uuid.uuid4().hex})

    file = cache.set_file(
        user, {
            'file_name': uuid.uuid4().hex,
            'file_size': 0,
            'relative_path': '/tmp/renku-core',
            'is_dir': False
        }
    )

    assert user.user_id == file.user_id


def test_service_cache_set_files(svc_client_cache):
    """Test service set user files."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': uuid.uuid4().hex})

    files = [{
        'file_name': uuid.uuid4().hex,
        'file_size': 0,
        'relative_path': '/tmp/renku-core',
        'is_dir': False
    } for _ in range(10)]

    expected = set([file_['file_name'] for file_ in files])

    received_files = cache.set_files(user, files)
    received_names = set([file_.file_name for file_ in received_files])

    received_users = set([file_.user_id for file_ in received_files])

    assert expected.issubset(received_names)
    assert user.user_id in received_users


def test_service_cache_get_file(svc_client_cache):
    """Test service get user file."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': uuid.uuid4().hex})
    file_obj = cache.set_file(
        user, {
            'file_name': uuid.uuid4().hex,
            'file_size': 0,
            'relative_path': '/tmp/renku-core',
            'is_dir': False
        }
    )

    file = cache.get_file(user, file_obj.file_id)

    assert user.user_id == file.user_id


def test_service_cache_get_files(svc_client_cache):
    """Test service get user files."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': uuid.uuid4().hex})

    files_data = [{
        'file_name': uuid.uuid4().hex,
        'file_size': 0,
        'relative_path': '/tmp/renku-core',
        'is_dir': False
    } for _ in range(10)]

    expected_files = {f.file_id for f in cache.set_files(user, files_data)}

    received = [f for f in cache.get_files(user)]
    received_files = {f.file_id for f in received}
    received_user = {f.user_id for f in received}

    assert 10 == len(expected_files)
    assert 10 <= len(received_files)

    assert received_files.issubset(expected_files)
    assert user.user_id in received_user


def test_service_cache_invalidate_file(svc_client_cache):
    """Test service invalidate user file."""
    client, _, cache = svc_client_cache

    user = cache.ensure_user({'user_id': uuid.uuid4().hex})
    file_obj = cache.set_file(
        user, {
            'file_name': uuid.uuid4().hex,
            'file_size': 0,
            'relative_path': '/tmp/renku-core',
            'is_dir': False
        }
    )
    cache.invalidate_file(user, file_obj.file_id)

    file = cache.get_file(user, file_obj.file_id)
    assert file is None
