# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
from unittest.mock import MagicMock

import jwt
import pytest

from renku.core.dataset.context import DatasetContext
from renku.domain_model.git import GitURL
from renku.domain_model.project import Project
from renku.domain_model.provenance.agent import Person
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.infrastructure.repository import Repository
from renku.ui.service.errors import (
    IntermittentFileExistsError,
    IntermittentProjectTemplateUnavailable,
    UserAnonymousError,
    UserProjectTemplateReferenceError,
    UserRepoUrlInvalidError,
)
from renku.ui.service.serializers.headers import JWT_TOKEN_SECRET
from tests.utils import assert_rpc_response, retry_failed


@pytest.mark.service
def test_serve_api_spec(svc_client):
    """Check serving of service spec."""
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    response = svc_client.get("/spec.json", headers=headers)

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
    response = svc_client.get("/cache.files_list", headers=headers)

    assert 200 == response.status_code

    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]


@pytest.mark.service
def test_file_upload(svc_client, identity_headers):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), uuid.uuid4().hex)), headers=headers
    )

    assert response
    assert 200 == response.status_code

    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)


@pytest.mark.service
def test_file_chunked_upload(svc_client, identity_headers, svc_cache_dir):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    upload_id = uuid.uuid4().hex
    filename = uuid.uuid4().hex

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(
            file=(io.BytesIO(b"chunk1"), filename),
            dzuuid=upload_id,
            dzchunkindex=0,
            dztotalchunkcount=3,
            dztotalfilesize=18,
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert "files" not in response.json["result"]

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(
            file=(io.BytesIO(b"chunk2"), filename),
            dzuuid=upload_id,
            dzchunkindex=1,
            dztotalchunkcount=3,
            dztotalfilesize=18,
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert "files" not in response.json["result"]

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(
            file=(io.BytesIO(b"chunk3"), filename),
            dzuuid=upload_id,
            dzchunkindex=2,
            dztotalchunkcount=3,
            dztotalfilesize=18,
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["files"])
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)

    file = next(svc_cache_dir[1].rglob("*")) / filename

    assert "chunk1chunk2chunk3" == file.read_text()


@pytest.mark.service
def test_file_chunked_upload_delete(svc_client, identity_headers, svc_cache_dir):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    content_type = headers.pop("Content-Type")

    upload_id = uuid.uuid4().hex
    filename = uuid.uuid4().hex

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(
            file=(io.BytesIO(b"chunk1"), filename),
            dzuuid=upload_id,
            dzchunkindex=0,
            dztotalchunkcount=3,
            dztotalfilesize=18,
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert "files" not in response.json["result"]

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(
            file=(io.BytesIO(b"chunk2"), filename),
            dzuuid=upload_id,
            dzchunkindex=1,
            dztotalchunkcount=3,
            dztotalfilesize=18,
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert "files" not in response.json["result"]
    upload_path = next(svc_cache_dir[1].rglob("*")) / upload_id
    assert upload_path.exists()

    headers["Content-Type"] = content_type
    response = svc_client.post(
        "/cache.files_delete_chunks",
        data=json.dumps(
            dict(
                dzuuid=upload_id,
            )
        ),
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert f"Deleted chunks for {upload_id}" == response.json["result"]

    assert not upload_path.exists()


@pytest.mark.service
def test_file_upload_override(svc_client, identity_headers):
    """Check successful file upload."""
    headers = copy.deepcopy(identity_headers)
    headers.pop("Content-Type")

    filename = uuid.uuid4().hex

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers
    )

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)
    old_file_id = response.json["result"]["files"][0]["file_id"]

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers
    )

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert IntermittentFileExistsError.code == response.json["error"]["code"]

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), filename)),
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
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers
    )

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert isinstance(uuid.UUID(response.json["result"]["files"][0]["file_id"]), uuid.UUID)

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers
    )

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert IntermittentFileExistsError.code == response.json["error"]["code"]


@pytest.mark.service
def test_file_upload_no_auth(svc_client):
    """Check failed file upload."""
    response = svc_client.post("/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), "datafile.txt")))

    assert response
    assert 200 == response.status_code

    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]


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
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers_user1
    )

    assert {"result"} == set(response.json.keys())

    file_id = response.json["result"]["files"][0]["file_id"]
    assert file_id
    assert 200 == response.status_code

    response = svc_client.post(
        "/cache.files_upload", data=dict(file=(io.BytesIO(b"this is a test"), filename)), headers=headers_user2
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
@retry_failed
def test_clone_projects_no_auth(svc_client, identity_headers, it_remote_repo_url):
    """Check error on cloning of remote repository."""
    payload = {
        "git_url": it_remote_repo_url,
    }

    response = svc_client.post(
        "/cache.project_clone", data=json.dumps(payload), headers={"Content-Type": "application/json"}
    )

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_clone_projects_with_auth(svc_client, identity_headers, it_remote_repo_url):
    """Check cloning of remote repository."""
    payload = {
        "git_url": it_remote_repo_url,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["initialized"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_clone_projects_multiple(svc_client, identity_headers, it_remote_repo_url):
    """Check multiple cloning of remote repository."""
    project_ids = []

    payload = {
        "git_url": it_remote_repo_url,
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
@retry_failed
def test_clone_projects_list_view_errors(svc_client, identity_headers, it_remote_repo_url):
    """Check cache state of cloned projects with no headers."""
    payload = {
        "git_url": it_remote_repo_url,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert response
    assert {"result"} == set(response.json.keys())

    assert isinstance(uuid.UUID(response.json["result"]["project_id"]), uuid.UUID)

    response = svc_client.get(
        "/cache.project_list",
        # no auth headers, expected error
    )
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]

    response = svc_client.get("/cache.project_list", headers=identity_headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert 1 == len(response.json["result"]["projects"])

    project = response.json["result"]["projects"][0]
    assert isinstance(uuid.UUID(project["project_id"]), uuid.UUID)
    assert isinstance(GitURL.parse(project["git_url"]), GitURL)


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_clone_projects_invalid_headers(svc_client, identity_headers, it_remote_repo_url):
    """Check cache state of cloned projects with invalid headers."""
    payload = {
        "git_url": it_remote_repo_url,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    assert response

    assert {"result"} == set(response.json.keys())

    response = svc_client.get(
        "/cache.project_list",
        # no auth headers, expected error
    )
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]

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
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name)),
        query_string={"unpack_archive": True, "override_existing": True},
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
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name)),
        query_string={"unpack_archive": False, "override_existing": True},
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
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name)),
        query_string={"unpack_archive": True, "override_existing": True},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

    response = svc_client.get("/cache.files_list", headers=headers)

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
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name)),
        query_string={"unpack_archive": False, "override_existing": True},
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
        data=dict(file=(io.BytesIO(datapack_gz.read_bytes()), datapack_gz.name, "application/x-gzip")),
        query_string={"unpack_archive": True, "override_existing": True},
        headers=headers,
    )

    assert response

    assert 200 == response.status_code
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["files"]

    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

    response = svc_client.get("/cache.files_list", headers=headers)

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
        data=dict(file=(io.BytesIO(datapack_gz.read_bytes()), datapack_gz.name, "application/x-gzip")),
        query_string={"unpack_archive": False, "override_existing": True},
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
        data=dict(file=(io.BytesIO(datapack_tar.read_bytes()), datapack_tar.name)),
        query_string={"unpack_archive": True, "override_existing": True},
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
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )
    assert "warnings" in response.json["result"]
    assert "errors" in response.json["result"]
    assert not response.json["result"]["errors"]


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_job(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, is_delayed=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_remote(svc_client, identity_headers, it_remote_repo_url):
    """Check execution of all migrations."""

    response = svc_client.post(
        "/cache.migrate",
        data=json.dumps(dict(git_url=it_remote_repo_url, skip_docker_update=True)),
        headers=identity_headers,
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local(svc_client_setup):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]
    assert "template_source" in response.json["result"]["template_status"]
    assert "template_ref" in response.json["result"]["template_status"]
    assert "template_id" in response.json["result"]["template_status"]
    assert "automated_template_update" in response.json["result"]["template_status"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote(svc_client, identity_headers, it_remote_repo_url):
    """Check if migrations are required for a remote project."""
    response = svc_client.get(
        "/cache.migrations_check", query_string=dict(git_url=it_remote_repo_url), headers=identity_headers
    )

    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote_errors(
    svc_client, identity_headers, it_remote_repo_url, it_remote_public_renku_repo_url
):
    """Check migrations throws the correct errors."""

    # NOTE: repo doesn't exist
    fake_url = f"{it_remote_repo_url}FAKE_URL"
    response = svc_client.get("/cache.migrations_check", query_string=dict(git_url=fake_url), headers=identity_headers)

    assert_rpc_response(response, "error")
    assert UserRepoUrlInvalidError.code == response.json["error"]["code"]

    # NOTE: token errors
    response = svc_client.get(
        "/cache.migrations_check", query_string=dict(git_url=it_remote_repo_url), headers=identity_headers
    )
    assert_rpc_response(response)

    headers = copy.copy(identity_headers)
    headers["Authorization"] = "Bearer 123abc"
    response = svc_client.get("/cache.migrations_check", query_string=dict(git_url=it_remote_repo_url), headers=headers)
    assert_rpc_response(response, "error")
    assert UserRepoUrlInvalidError.code == response.json["error"]["code"]

    response = svc_client.get(
        "/cache.migrations_check", query_string=dict(git_url=it_remote_public_renku_repo_url), headers=headers
    )
    assert_rpc_response(response)


@pytest.mark.service
@pytest.mark.integration
def test_migrate_wrong_template_source(svc_client_setup, monkeypatch):
    """Check if migrations gracefully fail when the project template is not available."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    # NOTE: fake source
    with monkeypatch.context() as monkey:
        import renku.core.template.usecase

        monkey.setattr(
            renku.core.template.usecase.TemplateMetadata, "source", property(MagicMock(return_value="https://FAKE_URL"))
        )

        response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

        assert_rpc_response(response, "error")
        assert IntermittentProjectTemplateUnavailable.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
def test_migrate_wrong_template_ref(svc_client_setup, template, monkeypatch):
    """Check if migrations gracefully fail when the project template points to a wrong ref."""
    svc_client, headers, project_id, _, _ = svc_client_setup
    # NOTE: fake reference
    with monkeypatch.context() as monkey:
        from renku.domain_model.template import TemplateMetadata

        monkey.setattr(TemplateMetadata, "source", property(MagicMock(return_value=template["url"])))
        monkey.setattr(TemplateMetadata, "reference", property(MagicMock(return_value="FAKE_REF")))

        response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

        assert_rpc_response(response, "error")
        assert UserProjectTemplateReferenceError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_cache_is_reset_after_failing_push(svc_protected_old_repo):
    """Check cache state is reset after pushing to a protected branch fails."""
    svc_client, headers, project_id, cache, user = svc_protected_old_repo

    project = cache.get_project(user, project_id)
    repository = Repository(path=project.abs_path)
    commit_sha_before = repository.head.commit.hexsha
    active_branch_before = repository.active_branch.name

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]

    project = cache.get_project(user, project_id)
    repository = Repository(path=project.abs_path)

    assert commit_sha_before == repository.head.commit.hexsha
    assert active_branch_before == repository.active_branch.name


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_migrating_protected_branch(svc_protected_old_repo):
    """Check migrating on a protected branch does not change cache state."""
    svc_client, headers, project_id, _, _ = svc_protected_old_repo

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code
    assert response.json["result"]["core_compatibility_status"]["migration_required"]

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
    assert response.json["result"]["core_compatibility_status"]["migration_required"]


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.serial
@retry_failed
def test_cache_gets_synchronized(
    local_remote_repository, directory_tree, quick_cache_synchronization, client_database_injection_manager
):
    """Test that the cache stays synchronized with the remote repository."""
    from renku.core.management.client import LocalClient
    from renku.domain_model.provenance.agent import Person

    svc_client, identity_headers, project_id, remote_repo, remote_repo_checkout = local_remote_repository

    client = LocalClient(remote_repo_checkout.path)

    with client_database_injection_manager(client):
        with client.commit(commit_message="Create dataset"):
            with DatasetContext(name="my_dataset", create=True, commit_database=True) as dataset:
                dataset.creators = [Person(name="me", email="me@example.com", id="me_id")]

    remote_repo_checkout.push()
    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params, headers=identity_headers)
    assert response
    assert 200 == response.status_code

    assert {"datasets"} == set(response.json["result"].keys()), response.json
    assert 1 == len(response.json["result"]["datasets"])

    payload = {
        "project_id": project_id,
        "name": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=identity_headers)

    assert response
    assert 200 == response.status_code
    assert {"name", "remote_branch"} == set(response.json["result"].keys())

    remote_repo_checkout.pull()

    with client_database_injection_manager(client):
        datasets = DatasetGateway().get_all_active_datasets()
        assert 2 == len(datasets)

    assert any(d.name == "my_dataset" for d in datasets)
    assert any(d.name == payload["name"] for d in datasets)


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_remote_anonymous(svc_client, it_remote_public_repo_url):
    """Test anonymous users can check for migration of public projects."""
    response = svc_client.get(
        "/cache.migrations_check", query_string={"git_url": it_remote_public_repo_url}, headers={}
    )

    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]["migration_required"] is True


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local_minimum_version(svc_client_setup, mocker):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    def _mock_database_project(project):
        def mocked_getter(self, key):
            if key == "project":
                return project
            return getattr(self, key)

        return mocked_getter

    mocker.patch("renku.domain_model.project.Project.minimum_renku_version", "2.0.0")
    project = Project(creator=Person(name="John Doe", email="jd@example.com"), name="testproject")
    mocker.patch(
        "renku.command.command_builder.database_dispatcher.Database.__getitem__", _mock_database_project(project)
    )
    mocker.patch("renku.version.__version__", "1.0.0")

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert response.json["result"]["core_compatibility_status"]
    assert response.json["result"]["template_status"]
    assert response.json["result"]["dockerfile_renku_status"]
    assert not response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert ">=2.0.0" == response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]
    assert "1.0.0" == response.json["result"]["core_renku_version"]
