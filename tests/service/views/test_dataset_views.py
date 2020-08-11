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
import os
import re
import shutil
import uuid
from pathlib import Path

import pytest
from flaky import flaky

from renku.service.config import INVALID_HEADERS_ERROR_CODE, INVALID_PARAMS_ERROR_CODE, RENKU_EXCEPTION_ERROR_CODE
from tests.utils import make_dataset_add_payload


def assert_rpc_response(response, with_key="result"):
    """Check rpc result in response."""
    assert response and 200 == response.status_code

    response_text = re.sub(r"http\S+", "", json.dumps(response.json),)
    assert with_key in response_text


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_wrong_ref_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, _, _ = svc_client_with_repo

    payload = {
        "project_id": "ref does not exist",
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert {"error": {"code": -32100, "reason": 'project_id "ref does not exist" not found'}} == response.json


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_remove_dataset_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    svc_client.post(
        "/datasets.create", data=json.dumps(payload), headers=headers,
    )

    response = svc_client.post("/datasets.remove", data=json.dumps(payload), headers=headers)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_with_metadata(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
        "title": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "keywords": ["keyword1", "keyword2"],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    params = {
        "project_id": project_id,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers,)

    assert response
    assert_rpc_response(response)

    ds = next(ds for ds in response.json["result"]["datasets"] if ds["name"] == payload["name"])

    assert payload["title"] == ds["title"]
    assert payload["name"] == ds["name"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert payload["keywords"] == ds["keywords"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_invalid_creator(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
        "title": "my little dataset",
        "creators": [{"name": None, "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert INVALID_PARAMS_ERROR_CODE == response.json["error"]["code"]

    expected_err = {"creators": {"0": {"name": ["Field may not be null."]}}}
    assert expected_err == response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_create_dataset_commit_msg(svc_client_with_repo):
    """Create a new dataset successfully with custom commit message."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {"project_id": project_id, "name": "{0}".format(uuid.uuid4().hex), "commit_message": "my awesome dataset"}

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view_dataset_exists(svc_client_with_repo):
    """Create a new dataset which already exists."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "mydataset",
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)
    assert response
    assert "result" in response.json.keys()

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)
    assert response
    assert_rpc_response(response, with_key="error")

    assert RENKU_EXCEPTION_ERROR_CODE == response.json["error"]["code"]
    assert "Dataset exists" in response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_view_unknown_param(svc_client_with_repo):
    """Create new dataset by specifying unknown parameters."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {"project_id": project_id, "name": "mydata", "remote_name": "origin"}

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response, with_key="error")

    assert INVALID_PARAMS_ERROR_CODE == response.json["error"]["code"]
    assert {"remote_name"} == set(response.json["error"]["reason"].keys())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_create_dataset_with_no_identity(svc_client_with_repo):
    """Create a new dataset with no identification provided."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "mydata",
        "remote_name": "origin",
    }

    response = svc_client.post(
        "/datasets.create",
        data=json.dumps(payload),
        headers={"Content-Type": headers["Content-Type"]}
        # no user identity, expect error
    )

    assert response
    assert_rpc_response(response, with_key="error")

    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    err_message = "user identification is incorrect or missing"
    assert err_message == response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_view_with_no_identity(svc_client_with_repo):
    """Check identity error raise in dataset add."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    payload = {
        "project_id": project_id,
        "name": "mydata",
        "remote_name": "origin",
    }

    response = svc_client.post(
        "/datasets.add",
        data=json.dumps(payload),
        headers={"Content-Type": headers["Content-Type"]}
        # no user identity, expect error
    )
    assert response
    assert_rpc_response(response, with_key="error")

    assert INVALID_HEADERS_ERROR_CODE == response.json["error"]["code"]

    err_message = "user identification is incorrect or missing"
    assert err_message == response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_view(svc_client_with_repo):
    """Check adding of uploaded file to dataset."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), "datafile1.txt"),),
        query_string={"override_existing": True},
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert_rpc_response(response)

    assert 1 == len(response.json["result"]["files"])

    file_id = response.json["result"]["files"][0]["file_id"]
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
        "create_dataset": True,
        "files": [{"file_id": file_id,},],
    }
    headers["Content-Type"] = content_type

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "project_id", "files", "remote_branch"} == set(response.json["result"].keys())

    assert 1 == len(response.json["result"]["files"])
    assert file_id == response.json["result"]["files"][0]["file_id"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_commit_msg(svc_client_with_repo):
    """Check adding of uploaded file to dataset with custom commit message."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), "datafile1.txt"),),
        query_string={"override_existing": True},
        headers=headers,
    )

    file_id = response.json["result"]["files"][0]["file_id"]
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        "commit_message": "my awesome data file",
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
        "create_dataset": True,
        "files": [{"file_id": file_id,},],
    }
    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "project_id", "files", "remote_branch"} == set(response.json["result"].keys())

    assert 1 == len(response.json["result"]["files"])
    assert file_id == response.json["result"]["files"][0]["file_id"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_file_failure(svc_client_with_repo):
    """Check adding of uploaded file to dataset with non-existing file."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), "datafile1.txt"),),
        query_string={"override_existing": True},
        headers=headers,
    )

    file_id = response.json["result"]["files"][0]["file_id"]
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        "commit_message": "my awesome data file",
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
        "create_dataset": True,
        "files": [{"file_id": file_id,}, {"file_path": "my problem right here"}],
    }
    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response, with_key="error")

    assert {"code", "reason"} == set(response.json["error"].keys())
    assert "invalid file reference" in response.json["error"]["reason"]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_datasets_view(svc_client_with_repo):
    """Check listing of existing datasets."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params, headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"datasets"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["datasets"])

    assert {"version", "description", "created_at", "name", "title", "creators", "keywords"} == set(
        response.json["result"]["datasets"][0].keys()
    )


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_datasets_view_no_auth(svc_client_with_repo):
    """Check listing of existing datasets with no auth."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    params = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params,)

    assert response
    assert_rpc_response(response, with_key="error")


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_create_and_list_datasets_view(svc_client_with_repo):
    """Create and list created dataset."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)
    assert response

    assert_rpc_response(response)
    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    params_list = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"datasets"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["datasets"])
    assert {"creators", "name", "version", "title", "description", "created_at", "keywords"} == set(
        response.json["result"]["datasets"][0].keys()
    )

    assert payload["name"] in [ds["name"] for ds in response.json["result"]["datasets"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_list_dataset_files(svc_client_with_repo):
    """Check listing of dataset files"""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    file_name = "{0}".format(uuid.uuid4().hex)
    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"this is a test"), file_name),),
        query_string={"override_existing": True},
        headers=headers,
    )

    assert response
    assert 200 == response.status_code
    assert_rpc_response(response)

    assert 1 == len(response.json["result"]["files"])
    file_id = response.json["result"]["files"][0]["file_id"]
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    payload = {
        "project_id": project_id,
        "name": "mydata",
        "files": [{"file_id": file_id},],
    }
    headers["Content-Type"] = content_type

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert file_id == response.json["result"]["files"][0]["file_id"]

    params = {
        "project_id": project_id,
        "name": "mydata",
    }

    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files"} == set(response.json["result"].keys())

    assert params["name"] == response.json["result"]["name"]
    assert file_name in [file["name"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_with_unpacked_archive(datapack_zip, svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response
    assert_rpc_response(response)

    assert 200 == response.status_code
    assert response.json["result"]["files"]

    mm = {}
    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

        file_id = file_["file_id"]
        assert file_id
        mm[file_["file_name"]] = file_

    file_ = mm["file2"]
    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    payload = {"project_id": project_id, "name": payload["name"], "files": [{"file_id": file_["file_id"]},]}

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert file_["file_id"] == response.json["result"]["files"][0]["file_id"]

    params = {
        "project_id": project_id,
        "name": payload["name"],
    }

    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files"} == set(response.json["result"].keys())

    assert params["name"] == response.json["result"]["name"]
    assert file_["file_name"] in [file["name"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_add_with_unpacked_archive_all(datapack_zip, svc_client_with_repo):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name),),
        query_string={"unpack_archive": True, "override_existing": True,},
        headers=headers,
    )

    assert response

    assert_rpc_response(response)
    assert 200 == response.status_code
    assert response.json["result"]["files"]

    mm = {}
    for file_ in response.json["result"]["files"]:
        assert not file_["is_archive"]
        assert not file_["unpack_archive"]

        file_id = file_["file_id"]
        assert file_id
        mm[file_["file_name"]] = file_

    file_ = mm["file2"]

    files = [{"file_id": file_["file_id"]} for file_ in response.json["result"]["files"]]

    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    payload = {
        "project_id": project_id,
        "name": payload["name"],
        "files": files,
    }

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert files == response.json["result"]["files"]

    params = {
        "project_id": project_id,
        "name": payload["name"],
    }

    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files"} == set(response.json["result"].keys())

    assert params["name"] == response.json["result"]["name"]
    assert file_["file_name"] in [file["name"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_add_existing_file(svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    payload = {
        "project_id": project_id,
        "name": "{0}".format(uuid.uuid4().hex),
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)
    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    files = [{"file_path": "README.md"}]
    payload = {
        "project_id": project_id,
        "name": payload["name"],
        "files": files,
    }

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())

    assert files == response.json["result"]["files"]


@pytest.mark.parametrize(
    "doi",
    [
        "10.5281/zenodo.3239980",
        "10.7910/DVN/TJCLKP"
        # TODO: add http uri
    ],
)
@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=30, min_passes=1)
def test_import_dataset_job_enqueue(doi, svc_client_cache, project, mock_redis):
    """Test import a dataset."""
    client, headers, cache = svc_client_cache
    user = cache.ensure_user({"user_id": "user"})

    project_meta = {
        "project_id": uuid.uuid4().hex,
        "name": Path(project).name,
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "git@gitlab.com",
    }

    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project / dest).exists():
        shutil.copytree(project, dest)

    response = client.post(
        "/datasets.import",
        data=json.dumps({"project_id": project_meta["project_id"], "dataset_uri": doi,}),
        headers=headers,
    )

    assert_rpc_response(response)
    assert {"created_at", "job_id",} == set(response.json["result"])

    user_job = cache.get_job(user, response.json["result"]["job_id"])
    assert response.json["result"]["job_id"] == user_job.job_id

    response = client.get("/jobs", headers=headers)
    assert_rpc_response(response)
    assert response.json["result"]["jobs"]

    assert user_job.job_id in [job["job_id"] for job in response.json["result"]["jobs"]]


@pytest.mark.parametrize("url", ["https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3",])
@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=30, min_passes=1)
def test_dataset_add_remote(url, svc_client_cache, project_metadata, mock_redis):
    """Test import a dataset."""
    project, project_meta = project_metadata
    client, headers, cache = svc_client_cache

    user = cache.ensure_user({"user_id": "user"})
    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project / dest).exists():
        shutil.copytree(project, dest)

    payload = make_dataset_add_payload(project_meta["project_id"], [url])
    response = client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert_rpc_response(response)
    assert {"files", "name", "project_id"} == set(response.json["result"])
    job_id = response.json["result"]["files"][0]["job_id"]

    user_job = cache.get_job(user, job_id)
    assert job_id == user_job.job_id

    response = client.get("/jobs", headers=headers)
    assert_rpc_response(response)
    assert response.json["result"]["jobs"]

    assert user_job.job_id in [job["job_id"] for job in response.json["result"]["jobs"]]


@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=30, min_passes=1)
def test_dataset_add_multiple_remote(svc_client_cache, project_metadata, mock_redis):
    """Test dataset add multiple remote files."""
    project, project_meta = project_metadata
    url_gist = "https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3"
    url_dbox = "https://www.dropbox.com/s/qcpts6fc81x6j4f/addme?dl=0"

    client, headers, cache = svc_client_cache
    user = cache.ensure_user({"user_id": "user"})
    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project / dest).exists():
        shutil.copytree(project, dest)

    payload = make_dataset_add_payload(project_meta["project_id"], [url_gist, url_dbox])
    response = client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert_rpc_response(response)
    assert {"files", "name", "project_id"} == set(response.json["result"])

    for file in response.json["result"]["files"]:
        job_id = file["job_id"]

        user_job = cache.get_job(user, job_id)
        assert job_id == user_job.job_id

        response = client.get("/jobs", headers=headers)
        assert_rpc_response(response)
        assert response.json["result"]["jobs"]

        assert user_job.job_id in [job["job_id"] for job in response.json["result"]["jobs"]]


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_add_remote_and_local_file(svc_client_with_repo):
    """Test dataset add remote and local files."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = make_dataset_add_payload(
        project_id, [("file_path", "README.md"), "https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3"],
    )
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())

    for pair in zip(response.json["result"]["files"], payload["files"]):
        if "job_id" in pair[0]:
            assert pair[0].pop("job_id")

        assert set(pair[0].values()) == set(pair[1].values())


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_edit_datasets_view(svc_client_with_repo):
    """Test editing dataset metadata."""
    svc_client, headers, project_id, _ = svc_client_with_repo
    name = "{0}".format(uuid.uuid4().hex)

    payload = {
        "project_id": project_id,
        "name": name,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)

    assert response
    assert_rpc_response(response)

    assert {"name", "remote_branch"} == set(response.json["result"].keys())
    assert payload["name"] == response.json["result"]["name"]

    params_list = {
        "project_id": project_id,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers,)

    assert response
    assert_rpc_response(response)

    edit_payload = {"project_id": project_id, "name": name, "title": "my new title", "keywords": ["keyword1"]}
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert response
    assert_rpc_response(response)

    assert {"warnings", "edited"} == set(response.json["result"])
    assert {"title": "my new title", "keywords": ["keyword1"]} == response.json["result"]["edited"]


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_protected_branch(svc_protected_repo):
    """Test adding a file to protected branch."""
    svc_client, headers, payload, response = svc_protected_repo
    assert response
    assert {"result"} == set(response.json.keys())

    payload = {
        "project_id": response.json["result"]["project_id"],
        "name": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers,)
    assert response

    if "error" in response.json.keys() and response.json["error"]["migration_required"]:
        # TODO: Fix this test to work with new project versions
        return
    assert {"result"} == set(response.json.keys())
    assert "master" != response.json["result"]["remote_branch"]


@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=10, min_passes=1)
def test_unlink_file(unlink_file_setup):
    """Check unlinking of a file from a dataset."""
    svc_client, headers, unlink_payload = unlink_file_setup

    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers,)

    assert {"result": {"unlinked": ["README.md"]}} == response.json


@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=10, min_passes=1)
def test_unlink_file_no_filter_error(unlink_file_setup):
    """Check for correct exception raise when no filters specified."""
    svc_client, headers, unlink_payload = unlink_file_setup
    unlink_payload.pop("include_filters")

    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers,)

    assert {"error": {"code": -32602, "reason": {"_schema": ["one of the filters must be specified"]}}} == response.json


@pytest.mark.integration
@pytest.mark.service
@flaky(max_runs=10, min_passes=1)
def test_unlink_file_exclude(unlink_file_setup):
    """Check unlinking of a file from a dataset with exclude."""
    svc_client, headers, unlink_payload = unlink_file_setup
    unlink_payload["exclude_filters"] = unlink_payload.pop("include_filters")

    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers,)

    assert {"error": {"code": -32100, "reason": "Invalid parameter value - No records found."}} == response.json
