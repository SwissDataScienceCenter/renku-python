# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Renku service cache view tests."""
import copy
import io
import json
import uuid

import jwt
import pytest
from flaky import flaky
from git import Repo

from conftest import IT_REMOTE_REPO_URL
from renku.core.models.git import GitURL
from renku.service.config import INVALID_HEADERS_ERROR_CODE, RENKU_EXCEPTION_ERROR_CODE
from renku.service.serializers.headers import JWT_TOKEN_SECRET


@pytest.mark.service
def test_serve_api_spec(svc_client):
    """Check serving of service spec."""
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    response = svc_client.get("/api/v1/spec", headers=headers)

    assert 0 != len(response.json.keys())
    assert 200 == response.status_code


@pytest.mark.service
def test_list_upload_files_all(svc_client, identity_headers):
    """Check list uploaded files view."""
    response = svc_client.get("/cache.files_list", headers=identity_headers)

    assert {"result"} == set(response.json.keys())

    assert 0 == len(response.json["result"]["files"])
    assert 200 == response.status_code


@pytest.mark.service
def test_list_upload_files_all_no_auth(svc_client):
    """Check error response on list uploaded files view."""
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    response = svc_client.get("/cache.files_list", headers=headers,)

    assert 200 == response.status_code

    assert {"error"} == set(response.json.keys())
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]


@pytest.mark.service
def test_file_upload(svc_client, identity_headers):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), uuid.uuid4().hex),), headers=headers,
    )

    assert response
    assert 200 == response.status_code

    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)


@pytest.mark.service
def test_file_upload_override(svc_client, identity_headers):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    filename = uuid.uuid4().hex

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)
    old_file_id = response.json["result"]["files"][0]["file_id"]

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers,
    )

    assert response
    assert 200 == response.status_code

    assert {"error"} == set(response.json.keys())
    assert RENKU_EXCEPTION_ERROR_CODE == response.json["error"]["code"]
    assert "file exists" == response.json["error"]["reason"]

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), filename),),
        query_string={"override_existing": True},
        headers=headers,
    )

    assert response
    assert 200 == response.status_code

    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)
    assert old_file_id != response.json["result"]["files"][0]["file_id"]


@pytest.mark.service
def test_file_upload_same_file(svc_client, identity_headers):
    """Check successful file upload with same file uploaded twice."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    filename = uuid.uuid4().hex

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers,
    )

    assert response
    assert 200 == response.status_code

    assert {"result"} == set(response.json.keys())

    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert RENKU_EXCEPTION_ERROR_CODE == response.json["error"]["code"]
    assert "file exists" == response.json["error"]["reason"]


@pytest.mark.service
def test_file_upload_no_auth(svc_client):
    """Check failed file upload."""
    response = svc_client.post("/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), "datafile.txt"),),)

    assert response
    assert 200 == response.status_code

    assert {"error"} == set(response.json.keys())
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]


@pytest.mark.service
def test_file_upload_with_users(svc_client, identity_headers):
    """Check successful file upload and listing based on user auth header."""
    headers_user1 = copy.deepcopy(identity_headers)
    headers_user1.pop("Content-Type")

    filename = uuid.uuid4().hex

    jwt_data = {
        "aud": ["renku"],
        "email_verified": False,
        "preferred_username": "user1@platform2.com",
        "given_name": "user",
        "family_name": "user one",
        "name": "User One",
        "email": "user1@platform2.com",
        "sub": "8d1f08e2-b136-4c93-a38f-d5f36a5919d9",
    }

    headers_user2 = {
        "Renku-User": jwt.encode(jwt_data, JWT_TOKEN_SECRET, algorithm="HS256"),
        "Authorization": identity_headers["Authorization"],
    }

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers_user1
    )

    assert {"result"} == set(response.json.keys())

    file_id = response.json["result"]["files"][0]["file_id"]
    assert file_id
    assert 200 == response.status_code

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename),), headers=headers_user2
    )

    assert response
    assert {"result"} == set(response.json.keys())

    response = svc_client.get("/cache.files_list", headers=headers_user1)

    assert response

    assert {"result"} == set(response.json.keys())
    assert 0 < len(response.json["result"]["files"])

    assert file_id in [file["file_id"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_clone_projects_no_auth(svc_client, identity_headers):
    """Check error on cloning of remote repository."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
    }

    response = svc_client.post(
        "/cache.project_clone", data=json.dumps(payload), headers={"Content-Type": "application/json"}
    )

    assert {"error"} == set(response.json.keys())
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    err_message = "user identification is incorrect or missing"
    assert err_message == response.json["error"]["reason"]

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert response
    assert {"result"} == set(response.json.keys())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_clone_projects_with_auth(svc_client, identity_headers):
    """Check cloning of remote repository."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["initialized"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_clone_projects_multiple(svc_client, identity_headers):
    """Check multiple cloning of remote repository."""
    project_ids = []

    payload = {
        "git_url": IT_REMOTE_REPO_URL,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert response

    assert {"result"} == set(response.json.keys())
    project_ids.append(response.json["result"])

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    project_ids.append(response.json["result"])

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    project_ids.append(response.json["result"])

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    last_pid = response.json["result"]["project_id"]

    response = svc_client.get("/cache.project_list", headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())

    pids = [p["project_id"] for p in response.json["result"]["projects"]]
    assert last_pid in pids

    for inserted in project_ids:
        assert inserted["project_id"] not in pids


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_clone_projects_list_view_errors(svc_client, identity_headers):
    """Check cache state of cloned projects with no headers."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert response
    assert {"result"} == set(response.json.keys())

    assert isinstance(uuid.UUID(response.json["result"]["project_id"]), uuid.UUID)

    response = svc_client.get(
        "/cache.project_list",
        # no auth headers, expected error
    )
    assert response

    assert {"error"} == set(response.json.keys())
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    response = svc_client.get("/cache.project_list", headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["projects"])

    project = response.json["result"]["projects"][0]
    assert isinstance(uuid.UUID(project["project_id"]), uuid.UUID)
    assert isinstance(GitURL.parse(project["git_url"]), GitURL)


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_clone_projects_invalid_headers(svc_client, identity_headers):
    """Check cache state of cloned projects with invalid headers."""
    payload = {
        "git_url": IT_REMOTE_REPO_URL,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)
    assert response

    assert {"result"} == set(response.json.keys())

    response = svc_client.get(
        "/cache.project_list",
        # no auth headers, expected error
    )
    assert response
    assert {"error"} == set(response.json.keys())
    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    response = svc_client.get("/cache.project_list", headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["projects"])


@pytest.mark.service
@pytest.mark.integration
def test_upload_zip_unpack_archive(datapack_zip, svc_client_with_repo):
    """Upload zip archive with unpack."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]


@pytest.mark.service
@pytest.mark.integration
def test_upload_zip_archive(datapack_zip, svc_client_with_repo):
    """Upload zip archive."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),),
        query_string={"unpack_archive": False, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["files"])

    for file_ in response.json["result"]["files"]:
        assert file_["is_archive"]
        assert not file_["unpack_archive"]


@pytest.mark.service
@pytest.mark.integration
def test_upload_tar_unpack_archive(datapack_tar, svc_client_with_repo):
    """Upload tar archive with unpack."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

    response = svc_client.get("/cache.files_list", headers=headers,)

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    dirs = filter(lambda x: x["is_dir"], response.json["result"]["files"])
    assert list(dirs)

    files = filter(lambda x: not x["is_dir"], response.json["result"]["files"])
    assert list(files)

    paths = [_file["relative_path"] for _file in files]
    assert sorted(paths) == paths


@pytest.mark.service
@pytest.mark.integration
def test_upload_tar_archive(datapack_tar, svc_client_with_repo):
    """Upload tar archive."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name),),
        query_string={"unpack_archive": False, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["files"])

    for file_ in response.json["result"]["files"]:
        assert file_["is_archive"]
        assert not file_["unpack_archive"]


@pytest.mark.service
@pytest.mark.integration
def test_upload_gz_unpack_archive(datapack_gz, svc_client_with_repo):
    """Upload gz archive with unpack."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_gz.read_bytes()), datapack_gz.name, "application/x-gzip"),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

    response = svc_client.get("/cache.files_list", headers=headers,)

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    dirs = filter(lambda x: x["is_dir"], response.json["result"]["files"])
    assert list(dirs)

    files = filter(lambda x: not x["is_dir"], response.json["result"]["files"])
    assert list(files)

    paths = [_file["relative_path"] for _file in files]
    assert sorted(paths) == paths


@pytest.mark.service
@pytest.mark.integration
def test_upload_gz_archive(datapack_gz, svc_client_with_repo):
    """Upload gz archive."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_gz.read_bytes()), datapack_gz.name, "application/x-gzip"),),
        query_string={"unpack_archive": False, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["files"])

    for file_ in response.json["result"]["files"]:
        assert file_["is_archive"]
        assert not file_["unpack_archive"]


@pytest.mark.service
@pytest.mark.integration
def test_field_upload_resp_fields(datapack_tar, svc_client_with_repo):
    """Check response fields."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code

    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    assert {
        "content_type",
        "file_id",
        "file_name",
        "file_size",
        "is_archive",
        "created_at",
        "is_dir",
        "unpack_archive",
        "relative_path",
    } == set(response.json["result"]["files"][0].keys())

    assert not response.json["result"]["files"][0]["is_archive"]
    assert not response.json["result"]["files"][0]["unpack_archive"]

    rel_path = response.json["result"]["files"][0]["relative_path"]
    assert rel_path.startswith(datapack_tar.name) and "unpacked" in rel_path


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _ = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_job(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _ = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, is_delayed=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local(svc_client_setup):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _ = svc_client_setup

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_version"]
    assert response.json["result"]["latest_version"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote(svc_client_setup, it_remote_repo):
    """Check if migrations are required for a remote project."""
    svc_client, headers, _, _ = svc_client_setup

    response = svc_client.get("/cache.migrations_check", query_string=dict(git_url=it_remote_repo), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_version"]
    assert response.json["result"]["latest_version"]


@pytest.mark.service
@pytest.mark.integration
def test_check_no_migrations(svc_client_with_repo):
    """Check if migrations are not required."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

    assert 200 == response.status_code
    assert not response.json["result"]["migration_required"]
    assert not response.json["result"]["template_update_possible"]
    assert not response.json["result"]["docker_update_possible"]
    assert response.json["result"]["project_supported"]


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@flaky(max_runs=10, min_passes=1)
def test_cache_is_reset_after_failing_push(svc_protected_old_repo):
    """Check cache state is reset after pushing to a protected branch fails."""
    svc_client, headers, project_id, cache, user = svc_protected_old_repo

    project = cache.get_project(user, project_id)
    repo = Repo(path=project.abs_path)
    commit_sha_before = repo.head.object.hexsha
    active_branch_before = repo.active_branch.name

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]

    project = cache.get_project(user, project_id)
    repo = Repo(path=project.abs_path)

    assert commit_sha_before == repo.head.object.hexsha
    assert active_branch_before == repo.active_branch.name


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@flaky(max_runs=10, min_passes=1)
def test_migrating_protected_branch(svc_protected_old_repo):
    """Check migrating on a protected branch does not change cache state."""
    svc_client, headers, project_id, _, _ = svc_protected_old_repo

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code
    assert response.json["result"]["migration_required"]

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code
    assert response.json["result"]["migration_required"]
