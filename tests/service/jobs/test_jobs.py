#
# Copyright 2020-2023 - Swiss Data Science Center (SDSC)
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

from renku.ui.service.jobs.cleanup import cache_files_cleanup
from tests.utils import assert_rpc_response, retry_failed


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.jobs
@retry_failed
def test_cleanup_old_files(datapack_zip, svc_client_with_repo, service_job):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, _, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name)),
        query_string={"unpack_archive": True, "override_existing": True},
        headers=headers,
    )
    assert response

    assert_rpc_response(response)
    assert 4 == len(response.json["result"]["files"])

    cache_files_cleanup()
    response = svc_client.get("/cache.files_list", headers=headers)

    assert response
    assert_rpc_response(response)
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

    chunk_id = uuid.uuid4().hex
    chunk_folder = tmp_path / chunk_id
    chunk_folder.mkdir()
    chunk_data = chunk_folder / "0"
    chunk_data.write_text("abcdefg")

    chunk = {
        "chunked_id": chunk_id,
        "file_name": "0",
        "relative_path": "0",
    }
    cache.set_file_chunk(user, chunk)

    response = svc_client.get("/cache.files_list", headers=headers)
    assert_rpc_response(response)
    assert 1 == len(response.json["result"]["files"])
    assert 1 == len(list(cache.get_chunks(user, chunk_id)))

    cache_files_cleanup()
    response = svc_client.get("/cache.files_list", headers=headers)
    assert_rpc_response(response)
    assert 0 == len(response.json["result"]["files"])
    assert 0 == len(list(cache.get_chunks(user, chunk_id)))


@pytest.mark.service
@pytest.mark.jobs
def test_job_constructor_lock(svc_client_with_user, service_job):
    """Test correct locking construction."""
    svc_client, headers, cache, user = svc_client_with_user

    project = {
        "project_id": uuid.uuid4().hex,
        "name": "my-project",
        "slug": "my-project",
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
