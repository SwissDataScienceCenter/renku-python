# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 - Swiss Data Science Center (SDSC)
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
import time
import uuid

import pytest
from flaky import flaky
from marshmallow import EXCLUDE

from renku.service.controllers.utils.project_clone import user_project_clone
from renku.service.jobs.cleanup import cache_files_cleanup, cache_project_cleanup
from renku.service.serializers.templates import ManifestTemplatesRequest
from tests.service.views.test_dataset_views import assert_rpc_response


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.jobs
@flaky(max_runs=30, min_passes=1)
def test_cleanup_old_files(datapack_zip, svc_client_with_repo, service_job):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )
    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 4 == len(response.json["result"]["files"])

    cache_files_cleanup()
    response = svc_client.get("/cache.files_list", headers=headers)

    assert response
    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 0 == len(response.json["result"]["files"])


@pytest.mark.service
@pytest.mark.jobs
def test_cleanup_files_old_keys(svc_client_with_user, service_job, tmp_path):
    """Cleanup old project."""
    svc_client, headers, cache, user = svc_client_with_user

    mydata = tmp_path / "mydata.json"
    mydata.write_text("1,2,3")

    file_upload = {
        "file_id": uuid.uuid4().hex,
        "content_type": "application/json",
        "file_name": "mydata.json",
        "file_size": 6,
        "is_archive": False,
        "is_dir": False,
        "unpack_archive": False,
        "relative_path": str(mydata),
    }
    cache.set_file(user, file_upload)

    response = svc_client.get("/cache.files_list", headers=headers)
    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json["result"]["files"])

    cache_files_cleanup()
    response = svc_client.get("/cache.files_list", headers=headers)

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert 0 == len(response.json["result"]["files"])


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_cleanup_old_project(datapack_zip, svc_client_with_repo, service_job):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.get("/cache.project_list", headers=headers)

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json["result"]["projects"])

    cache_project_cleanup()

    response = svc_client.get("/cache.project_list", headers=headers)

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 0 == len(response.json["result"]["projects"])


@pytest.mark.service
@pytest.mark.jobs
def test_cleanup_project_old_keys(svc_client_with_user, service_job):
    """Cleanup old project with old hset keys."""
    svc_client, headers, cache, user = svc_client_with_user

    project = {
        "project_id": uuid.uuid4().hex,
        "name": "my-project",
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "git@gitlab.com",
        "initialized": True,
    }
    project = cache.make_project(user, project)
    os.makedirs(str(project.abs_path), exist_ok=True)

    response = svc_client.get("/cache.project_list", headers=headers)
    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert 1 == len(response.json["result"]["projects"])

    cache_project_cleanup()
    response = svc_client.get("/cache.project_list", headers=headers)

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert 0 == len(response.json["result"]["projects"])


@pytest.mark.service
@pytest.mark.jobs
def test_job_constructor_lock(svc_client_with_user, service_job):
    """Test correct locking construction."""
    svc_client, headers, cache, user = svc_client_with_user

    project = {
        "project_id": uuid.uuid4().hex,
        "name": "my-project",
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "git@gitlab.com",
        "initialized": True,
    }

    project = cache.make_project(user, project)
    os.makedirs(str(project.abs_path), exist_ok=True)

    job = cache.make_job(user, project=project)

    assert job
    assert job.job_id
    assert project.project_id == job.project_id
    assert user.user_id == job.user_id

    assert project.project_id in {_id.decode("utf-8") for _id in job.locked.members()}


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_project_cleanup_success(svc_client_cache):
    """Test project cleanup through the job."""
    client, _, cache = svc_client_cache

    user_data = {
        "user_id": uuid.uuid4().hex,
        "email": "contact@justsam.io",
        "fullname": "renku the frog",
        "token": "None",
    }
    project_data = {
        "name": "renku-project-template",
        "depth": 1,
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "owner": "SwissDataScienceCenter",
    }
    project_data = ManifestTemplatesRequest().load({**user_data, **project_data}, unknown=EXCLUDE)
    assert "user_id" not in project_data.keys()
    project_one = user_project_clone(user_data, project_data)

    assert project_one.age >= 0
    assert not project_one.ttl_expired()
    assert project_one.exists()

    os.environ["RENKU_SVC_CLEANUP_TTL_PROJECTS"] = "1"
    time.sleep(1)

    assert project_one.age >= 1
    assert project_one.ttl_expired()

    cache_project_cleanup()

    project_data = ManifestTemplatesRequest().load({**user_data, **project_data}, unknown=EXCLUDE)
    assert "user_id" not in project_data.keys()
    user = cache.get_user(user_data["user_id"])
    projects = cache.get_projects(user)
    assert [] == [p.project_id for p in projects]

    project_two = user_project_clone(user_data, project_data)
    os.environ["RENKU_SVC_CLEANUP_TTL_PROJECTS"] = "1800"

    assert project_two.age >= 0
    assert not project_two.ttl_expired()
    assert project_two.exists()

    assert project_one.project_id != project_two.project_id
