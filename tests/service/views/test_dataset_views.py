#
# Copyright 2019-2023 - Swiss Data Science Center (SDSC)
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
import shutil
import uuid

import pytest
from werkzeug.utils import secure_filename

from renku.core.util.os import normalize_to_ascii
from renku.ui.service.errors import (
    IntermittentDatasetExistsError,
    IntermittentFileNotExistsError,
    ProgramInvalidGenericFieldsError,
    ProgramRepoUnknownError,
    UserAnonymousError,
    UserDatasetsMultipleImagesError,
    UserDatasetsUnlinkError,
    UserDatasetsUnreachableImageError,
    UserInvalidGenericFieldsError,
    UserMissingFieldError,
    UserOutdatedProjectError,
    UserRepoNoAccessError,
)
from renku.ui.service.serializers.headers import encode_b64
from tests.utils import assert_rpc_response, make_dataset_add_payload, retry_failed


def upload_file(svc_client, headers, filename) -> str:
    """Upload a file to the service cache."""
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(b"Test file content"), filename)),
        query_string={"override_existing": True},
        headers=headers,
    )

    assert_rpc_response(response)
    assert 1 == len(response.json["result"]["files"])

    file_id = response.json["result"]["files"][0]["file_id"]
    assert isinstance(uuid.UUID(file_id), uuid.UUID)

    headers["Content-Type"] = content_type

    return file_id


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)

    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_view_with_datadir(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {"git_url": url_components.href, "slug": uuid.uuid4().hex, "data_directory": "my-folder/"}

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)

    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)

    assert_rpc_response(response)
    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])
    assert ds["data_directory"] == "my-folder"


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_remote_create_dataset_view(svc_client_cache, it_remote_repo_url):
    """Create a new dataset successfully."""
    svc_client, headers, cache = svc_client_cache

    payload = {
        "git_url": it_remote_repo_url,
        "slug": f"{uuid.uuid4().hex}",
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_delay_create_dataset_view(svc_client_cache, it_remote_repo_url):
    """Create a new job for dataset create operation."""
    svc_client, headers, cache = svc_client_cache

    payload = {
        "git_url": it_remote_repo_url,
        "slug": f"{uuid.uuid4().hex}",
        "is_delayed": True,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"job_id", "created_at"} == set(response.json["result"].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_wrong_ref_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, _, _ = svc_client_with_repo

    payload = {
        "git_url": "http://doesnotexistanywhere994455/a/b.git",
        "slug": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert ProgramRepoUnknownError.code == response.json["error"]["code"], response.json


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_remove_dataset_view(svc_client_with_repo):
    """Create a new dataset successfully."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }

    svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    response = svc_client.post("/datasets.remove", data=json.dumps(payload), headers=headers)

    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    # NOTE: Ensure that dataset does not exist in this project anymore!
    response = svc_client.get("/datasets.list", query_string={"git_url": url_components.href}, headers=headers)
    assert_rpc_response(response)
    datasets = [ds["slug"] for ds in response.json["result"]["datasets"]]
    assert payload["slug"] not in datasets


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_remove_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed remove."""
    response = svc_client.post(
        "/datasets.remove",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True, slug="mydata")),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_metadata(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "keywords": ["keyword1", "keyword2"],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)

    assert_rpc_response(response)
    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])
    assert payload["name"] == ds["name"]
    assert payload["slug"] == ds["slug"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert payload["keywords"] == ds["keywords"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_images(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [
            {"content_url": "https://example.com/image1.jpg", "position": 1},
            {"content_url": "data/renku_logo.png", "position": 1},
        ],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert UserDatasetsMultipleImagesError.code == response.json["error"]["code"]

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [
            {"content_url": "https://example.com/image1.jpg", "position": 1},
            {"content_url": "data/renku_logo.png", "position": 2},
        ],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)

    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)
    assert_rpc_response(response)

    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])

    assert payload["name"] == ds["name"]
    assert payload["slug"] == ds["slug"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert len(ds["images"]) == 2
    img1 = next(img for img in ds["images"] if img["position"] == 1)
    img2 = next(img for img in ds["images"] if img["position"] == 2)

    assert img1["content_url"] == "https://example.com/image1.jpg"
    assert img2["content_url"].startswith(".renku/dataset_images/")
    assert img2["content_url"].endswith("/2.png")


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_custom_metadata(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "custom_metadata": {
            "@id": "http://example.com/metadata12",
            "@type": "https://schema.org/myType",
            "https://schema.org/property1": 1,
            "https://schema.org/property2": "test",
        },
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)

    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)
    assert_rpc_response(response)

    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])

    assert payload["name"] == ds["name"]
    assert payload["slug"] == ds["slug"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert payload["custom_metadata"] == ds["annotations"][0]["body"]


@pytest.mark.parametrize(
    "img_url",
    ["https://raw.githubusercontent.com/SwissDataScienceCenter/calamus/master/docs/reed.png", "https://bit.ly/2ZoutNn"],
)
@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_image_download(svc_client_with_repo, img_url):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [{"content_url": "https://renkulab.io/api/doesnt_exist.png", "position": 1, "mirror_locally": True}],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert UserDatasetsUnreachableImageError.code == response.json["error"]["code"]

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [{"content_url": img_url, "position": 1, "mirror_locally": True}],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)
    assert_rpc_response(response)

    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])
    assert len(ds["images"]) == 1
    img1 = next(img for img in ds["images"] if img["position"] == 1)

    assert img1["content_url"].startswith(".renku/dataset_images/")
    assert img1["content_url"].endswith("/1.png")


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_uploaded_images(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    file_id1 = upload_file(svc_client, headers, "image1.jpg")
    file_id2 = upload_file(svc_client, headers, "image2.png")

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [{"file_id": file_id1, "position": 1}, {"file_id": file_id2, "position": 2}],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)
    assert_rpc_response(response)

    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])

    assert payload["name"] == ds["name"]
    assert payload["slug"] == ds["slug"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert len(ds["images"]) == 2
    img1 = next(img for img in ds["images"] if img["position"] == 1)
    img2 = next(img for img in ds["images"] if img["position"] == 2)

    assert img1["content_url"].startswith(".renku/dataset_images/")
    assert img1["content_url"].endswith("/1.jpg")
    assert img2["content_url"].startswith(".renku/dataset_images/")
    assert img2["content_url"].endswith("/2.png")


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_invalid_creator(svc_client_with_repo):
    """Create a new dataset with metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "name": "my little dataset",
        "creators": [{"name": None, "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response, "error")
    assert UserMissingFieldError.code == response.json["error"]["code"]
    assert "creators.0.name" in response.json["error"]["userMessage"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_view_dataset_exists(svc_client_with_repo):
    """Create a new dataset which already exists."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": "mydataset",
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert IntermittentDatasetExistsError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_view_unknown_param(svc_client_with_repo):
    """Create new dataset by specifying unknown parameters."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    unknown_field = "remote_name"
    payload = {"git_url": url_components.href, "slug": "mydata", unknown_field: "origin"}

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response, "error")
    assert ProgramInvalidGenericFieldsError.code == response.json["error"]["code"]
    assert unknown_field in response.json["error"]["devMessage"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_dataset_with_no_identity(svc_client_with_repo):
    """Create a new dataset with no identification provided."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": "mydata",
        "remote_name": "origin",
    }

    response = svc_client.post(
        "/datasets.create", data=json.dumps(payload), headers={"Content-Type": headers["Content-Type"]}
    )

    assert_rpc_response(response, "error")
    assert UserAnonymousError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_file_view_with_no_identity(svc_client_with_repo):
    """Check identity error raise in dataset add."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    payload = {
        "git_url": url_components.href,
        "slug": "mydata",
        "remote_name": "origin",
    }

    response = svc_client.post(
        "/datasets.add", data=json.dumps(payload), headers={"Content-Type": headers["Content-Type"]}
    )

    assert_rpc_response(response, "error")
    assert UserAnonymousError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_file_view(svc_client_with_repo):
    """Check adding of uploaded file to dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    file_id = upload_file(svc_client, headers, "datafile1.txt")

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "create_dataset": True,
        "files": [{"file_id": file_id}],
    }

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "project_id", "files", "remote_branch"} == set(response.json["result"].keys())
    assert 1 == len(response.json["result"]["files"])
    assert file_id == response.json["result"]["files"][0]["file_id"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_add_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed add."""
    response = svc_client.post(
        "/datasets.add",
        data=json.dumps(
            dict(git_url=it_remote_repo_url, is_delayed=True, slug="mydata", files=[{"file_path": "somefile.txt"}])
        ),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_file_failure(svc_client_with_repo):
    """Check adding of uploaded file to dataset with non-existing file."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    file_id = upload_file(svc_client, headers, "datafile1.txt")

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
        "create_dataset": True,
        "files": [{"file_id": file_id}, {"file_path": "my problem right here"}],
    }
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response, "error")
    assert IntermittentFileNotExistsError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_datasets_view(svc_client_with_repo):
    """Check listing of existing datasets."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    params = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params, headers=headers)
    assert_rpc_response(response)
    assert {"datasets"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["datasets"])
    assert {
        "version",
        "description",
        "identifier",
        "images",
        "created_at",
        "slug",
        "name",
        "creators",
        "keywords",
        "annotations",
        "storage",
        "data_directory",
    } == set(response.json["result"]["datasets"][0].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_datasets_anonymous(svc_client_with_repo, it_remote_repo_url):
    """Check listing of existing datasets."""
    svc_client, _, _, _ = svc_client_with_repo

    params = {
        "git_url": it_remote_repo_url,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers={})
    assert_rpc_response(response, "error")
    assert UserRepoNoAccessError.code == response.json["error"]["code"]

    params = {
        "git_url": "https://gitlab.dev.renku.ch/renku-python-integration-tests/no-renku",
    }

    response = svc_client.get("/datasets.list", query_string=params, headers={})
    assert_rpc_response(response, "error")
    # NOTE: We don't migrate remote projects; the fact that this operation fails with a migration error means that the
    # project could be cloned for the anonymous user
    assert UserOutdatedProjectError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_datasets_view_remote(svc_client_with_repo, it_remote_repo_url):
    """Check listing of existing datasets."""
    svc_client, headers, _, _ = svc_client_with_repo

    params = dict(git_url=it_remote_repo_url)

    response = svc_client.get("/datasets.list", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {"datasets"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["datasets"])
    assert {
        "version",
        "description",
        "identifier",
        "images",
        "created_at",
        "slug",
        "name",
        "creators",
        "keywords",
        "annotations",
        "storage",
        "data_directory",
    } == set(response.json["result"]["datasets"][0].keys())


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_datasets_view_no_auth(svc_client_with_repo):
    """Check listing of existing datasets with no auth."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    params = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params)
    assert_rpc_response(response, "error")
    assert UserRepoNoAccessError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_dataset_files_anonymous(svc_client_with_repo, it_remote_repo_url):
    """Check listing of existing dataset files."""
    svc_client, _, _, _ = svc_client_with_repo

    params = {"git_url": it_remote_repo_url, "slug": "ds1"}

    response = svc_client.get("/datasets.files_list", query_string=params, headers={})
    assert_rpc_response(response, "error")
    assert UserRepoNoAccessError.code == response.json["error"]["code"]

    params = {"git_url": "https://gitlab.dev.renku.ch/renku-python-integration-tests/no-renku", "slug": "mydata"}

    response = svc_client.get("/datasets.files_list", query_string=params, headers={})
    assert_rpc_response(response, "error")
    # NOTE: We don't migrate remote projects; the fact that this operation fails with a migration error means that the
    # project could be cloned for the anonymous user
    assert UserOutdatedProjectError.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_datasets_files_remote(svc_client_with_repo, it_remote_repo_url):
    """Check listing of existing dataset files."""
    svc_client, headers, _, _ = svc_client_with_repo

    params = dict(git_url=it_remote_repo_url, slug="ds1")

    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {"files", "slug"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["files"])
    assert "ds1" == response.json["result"]["slug"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_create_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed dataset create."""
    response = svc_client.post(
        "/datasets.create",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True, slug=uuid.uuid4().hex)),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_and_list_datasets_view(svc_client_with_repo):
    """Create and list created dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params_list = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)
    assert_rpc_response(response)
    assert {"datasets"} == set(response.json["result"].keys())
    assert 0 != len(response.json["result"]["datasets"])
    assert {
        "creators",
        "slug",
        "identifier",
        "images",
        "version",
        "name",
        "description",
        "created_at",
        "keywords",
        "annotations",
        "storage",
        "data_directory",
    } == set(response.json["result"]["datasets"][0].keys())

    assert payload["slug"] in [ds["slug"] for ds in response.json["result"]["datasets"]]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_list_dataset_files(svc_client_with_repo):
    """Check listing of dataset files."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    file_name = uuid.uuid4().hex
    file_id = upload_file(svc_client, headers, file_name)

    payload = {
        "git_url": url_components.href,
        "slug": "mydata",
        "files": [{"file_id": file_id}],
    }

    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert file_id == response.json["result"]["files"][0]["file_id"]

    params = {
        "git_url": url_components.href,
        "slug": "mydata",
    }

    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers)
    assert_rpc_response(response)
    assert {"slug", "files"} == set(response.json["result"].keys())
    assert params["slug"] == response.json["result"]["slug"]
    assert file_name in [file["name"] for file in response.json["result"]["files"]]
    assert {"name", "path", "added"} == response.json["result"]["files"][0].keys()


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_with_unpacked_archive(datapack_zip, svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name)),
        query_string={"unpack_archive": True, "override_existing": True},
        headers=headers,
    )

    assert_rpc_response(response)
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
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }

    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    payload = {"git_url": url_components.href, "slug": payload["slug"], "files": [{"file_id": file_["file_id"]}]}
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert file_["file_id"] == response.json["result"]["files"][0]["file_id"]

    params = {
        "git_url": url_components.href,
        "slug": payload["slug"],
    }
    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers)

    assert_rpc_response(response)
    assert {"slug", "files"} == set(response.json["result"].keys())
    assert params["slug"] == response.json["result"]["slug"]
    assert file_["file_name"] in [file["name"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_with_unpacked_archive_all(datapack_zip, svc_client_with_repo):
    """Upload archive and add its contents to a dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    content_type = headers.pop("Content-Type")

    response = svc_client.post(
        "/cache.files_upload",
        data=dict(file=(io.BytesIO(datapack_zip.read_bytes()), datapack_zip.name)),
        query_string={"unpack_archive": True, "override_existing": True},
        headers=headers,
    )

    assert_rpc_response(response)
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
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }
    headers["Content-Type"] = content_type
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    payload = {
        "git_url": url_components.href,
        "slug": payload["slug"],
        "files": files,
    }
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    assert files == response.json["result"]["files"]

    params = {
        "git_url": url_components.href,
        "slug": payload["slug"],
    }
    response = svc_client.get("/datasets.files_list", query_string=params, headers=headers)
    assert_rpc_response(response)
    assert {"slug", "files"} == set(response.json["result"].keys())
    assert params["slug"] == response.json["result"]["slug"]
    assert file_["file_name"] in [file["name"] for file in response.json["result"]["files"]]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_add_existing_file(svc_client_with_repo):
    """Upload archive and add it to a dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    files = [{"file_path": "README.md"}]
    payload = {
        "git_url": url_components.href,
        "slug": payload["slug"],
        "files": files,
    }
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
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
@retry_failed
@pytest.mark.vcr
def test_cached_import_dataset_job(doi, svc_client_cache, project):
    """Test import a dataset."""
    client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    name = project.path.name

    project_meta = {
        "project_id": uuid.uuid4().hex,
        "name": name,
        "slug": normalize_to_ascii(name),
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "https://example.com/a/b.git",
        "initialized": True,
    }

    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project.path / dest).exists():
        shutil.copytree(project.path, dest)

    response = client.post(
        "/datasets.import",
        data=json.dumps({"git_url": project_meta["git_url"], "dataset_uri": doi}),
        headers=headers,
    )

    assert_rpc_response(response)
    assert {"created_at", "job_id"} == set(response.json["result"])

    user_job = cache.get_job(user, response.json["result"]["job_id"])
    assert response.json["result"]["job_id"] == user_job.job_id

    response = client.get("/jobs", headers=headers)
    assert_rpc_response(response)
    assert response.json["result"]["jobs"]

    assert user_job.job_id in [job["job_id"] for job in response.json["result"]["jobs"]]


@pytest.mark.parametrize("doi", ["10.5281/zenodo.3239980"])
@pytest.mark.integration
@pytest.mark.service
@retry_failed
@pytest.mark.vcr
def test_remote_import_dataset_job(doi, svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed import of a dataset."""
    response = svc_client.post(
        "/datasets.import",
        data=json.dumps(dict(git_url=it_remote_repo_url, dataset_uri=doi, is_delayed=True)),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.parametrize("url", ["https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3"])
@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_dataset_add_remote(url, svc_client_cache, project_metadata):
    """Test import a dataset."""
    project, project_meta = project_metadata
    client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})
    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project.path / dest).exists():
        shutil.copytree(project.path, dest)

    payload = make_dataset_add_payload(project_meta["git_url"], [url])
    response = client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"files", "slug", "project_id", "remote_branch"} == set(response.json["result"])
    job_id = response.json["result"]["files"][0]["job_id"]

    user_job = cache.get_job(user, job_id)
    assert job_id == user_job.job_id

    response = client.get("/jobs", headers=headers)
    assert_rpc_response(response)
    assert response.json["result"]["jobs"]

    assert user_job.job_id in [job["job_id"] for job in response.json["result"]["jobs"]]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_dataset_add_multiple_remote(svc_client_cache, project_metadata):
    """Test dataset add multiple remote files."""
    project, project_meta = project_metadata
    url_gist = "https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3"
    url_dbox = "https://www.dropbox.com/s/qcpts6fc81x6j4f/addme?dl=0"

    client, headers, cache = svc_client_cache
    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})
    project_obj = cache.make_project(user, project_meta)

    dest = project_obj.abs_path
    os.makedirs(dest.parent, exist_ok=True)
    if not (project.path / dest).exists():
        shutil.copytree(project.path, dest)

    payload = make_dataset_add_payload(project_meta["git_url"], [url_gist, url_dbox])
    response = client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"files", "slug", "project_id", "remote_branch"} == set(response.json["result"])

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
@retry_failed
def test_add_remote_and_local_file(svc_client_with_repo):
    """Test dataset add remote and local files."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = make_dataset_add_payload(
        url_components.href,
        [("file_path", "README.md"), "https://gist.github.com/jsam/d957f306ed0fe4ff018e902df6a1c8e3"],
    )
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "files", "project_id", "remote_branch"} == set(response.json["result"].keys())
    for pair in zip(response.json["result"]["files"], payload["files"]):
        if "job_id" in pair[0]:
            assert pair[0].pop("job_id")
        assert set(pair[0].values()) == set(pair[1].values())


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.parametrize(
    "custom_metadata",
    [
        [
            {
                "@id": "http://example.com/metadata12",
                "@type": "https://schema.org/myType",
                "https://schema.org/property1": 1,
                "https://schema.org/property2": "test",
            },
        ],
        [
            {
                "@id": "http://example.com/metadata12",
                "@type": "https://schema.org/myType",
                "https://schema.org/property1": 1,
                "https://schema.org/property2": "test",
            },
            {
                "@id": "http://example.com/metadata1",
                "@type": "https://schema.org/myType1",
                "https://schema.org/property4": 3,
                "https://schema.org/property5": "test1",
            },
        ],
    ],
)
@pytest.mark.parametrize("custom_metadata_source", [None, "testSource"])
@retry_failed
def test_edit_datasets_view(svc_client_with_repo, custom_metadata, custom_metadata_source):
    """Test editing dataset metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    slug = uuid.uuid4().hex

    payload = {
        "git_url": url_components.href,
        "slug": slug,
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params_list = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)

    assert_rpc_response(response)

    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "name": "my new name",
        "keywords": ["keyword1"],
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "custom_metadata": custom_metadata,
    }
    if custom_metadata_source is not None:
        edit_payload["custom_metadata_source"] = custom_metadata_source
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)
    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert {
        "name": "my new name",
        "keywords": ["keyword1"],
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "custom_metadata": custom_metadata,
    } == response.json["result"]["edited"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_edit_datasets_view_without_modification(svc_client_with_repo):
    """Test editing dataset metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    slug = uuid.uuid4().hex

    payload = {
        "git_url": url_components.href,
        "slug": slug,
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "name": "my-name",
        "description": "my description",
        "keywords": ["keywords"],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params_list = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)

    assert_rpc_response(response)
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert {} == response.json["result"]["edited"]

    params_list = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)

    assert_rpc_response(response)
    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])
    assert payload["name"] == ds["name"]
    assert payload["slug"] == ds["slug"]
    assert payload["description"] == ds["description"]
    assert payload["creators"] == ds["creators"]
    assert payload["keywords"] == ds["keywords"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_edit_datasets_view_unset_values(svc_client_with_repo):
    """Test editing dataset metadata."""
    svc_client, headers, project_id, url_components = svc_client_with_repo
    slug = uuid.uuid4().hex

    payload = {
        "git_url": url_components.href,
        "slug": slug,
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "name": "my-name",
        "description": "my description",
        "keywords": ["keywords"],
        "images": [
            {"content_url": "https://example.com/image1.jpg", "position": 1},
        ],
        "custom_metadata": {"test": "test"},
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)
    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params_list = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)

    assert_rpc_response(response)
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "keywords": None,
        "images": None,
        "custom_metadata": None,
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert {
        "keywords": [],
        "custom_metadata": None,
        "images": [],
    } == response.json[
        "result"
    ]["edited"]

    params_list = {
        "git_url": url_components.href,
    }

    response = svc_client.get("/datasets.list", query_string=params_list, headers=headers)

    assert_rpc_response(response)
    ds = next(ds for ds in response.json["result"]["datasets"] if ds["slug"] == payload["slug"])
    assert edit_payload["slug"] == ds["slug"]
    assert 0 == len(ds["keywords"])
    assert 0 == len(ds["annotations"])
    assert 0 == len(ds["images"])


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_edit_dataset_with_images(svc_client_with_repo):
    """Edit images of a dataset."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    slug = uuid.uuid4().hex

    payload = {
        "git_url": url_components.href,
        "slug": slug,
        "name": "my little dataset",
        "creators": [{"name": "name123", "email": "name123@ethz.ch", "affiliation": "ethz"}],
        "description": "my little description",
        "images": [
            {"content_url": "https://example.com/image1.jpg", "position": 1},
            {"content_url": "data/renku_logo.png", "position": 2},
        ],
    }

    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"slug", "remote_branch"} == set(response.json["result"].keys())
    assert payload["slug"] == response.json["result"]["slug"]

    params = {
        "git_url": url_components.href,
    }
    response = svc_client.get("/datasets.list", query_string=params, headers=headers)

    assert_rpc_response(response)
    file_id = upload_file(svc_client, headers, "image2.jpg")

    # NOTE: test edit reordering and add
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "images": [
            {"content_url": "data/renku_logo.png", "position": 1},
            {"content_url": "https://example.com/image1.jpg", "position": 2},
            {"content_url": "https://example.com/other_image.jpg", "position": 3},
            {"file_id": file_id, "position": 4},
        ],
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert {"images"} == response.json["result"]["edited"].keys()

    images = response.json["result"]["edited"]["images"]
    assert len(images) == 4
    images.sort(key=lambda x: x["position"])

    assert images[0]["content_url"].startswith(".renku/dataset_images/")
    assert images[0]["content_url"].endswith("/1.png")
    assert images[1]["content_url"] == "https://example.com/image1.jpg"
    assert images[2]["content_url"] == "https://example.com/other_image.jpg"
    assert images[3]["content_url"].startswith(".renku/dataset_images/")
    assert images[3]["content_url"].endswith("/4.jpg")

    # NOTE: test edit with duplicate position
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "images": [
            {"content_url": "data/renku_logo.png", "position": 1},
            {"content_url": "https://example.com/image1.jpg", "position": 2},
            {"content_url": "https://example.com/other_image.jpg", "position": 2},
        ],
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response, "error")
    assert UserDatasetsMultipleImagesError.code == response.json["error"]["code"]

    # NOTE: test edit remove images
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "images": [],
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert {"images"} == response.json["result"]["edited"].keys()
    assert 0 == len(response.json["result"]["edited"]["images"])

    # NOTE: test edit no change
    edit_payload = {
        "git_url": url_components.href,
        "slug": slug,
        "images": [],
    }
    response = svc_client.post("/datasets.edit", data=json.dumps(edit_payload), headers=headers)

    assert_rpc_response(response)
    assert {"warnings", "edited", "remote_branch"} == set(response.json["result"])
    assert 0 == len(response.json["result"]["edited"].keys())


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_edit_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed edit."""
    response = svc_client.post(
        "/datasets.edit",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True, slug="mydata")),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.remote_repo("protected")
@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_protected_branch(svc_client_with_repo):
    """Test adding a file to protected branch."""
    svc_client, headers, project_id, url_components = svc_client_with_repo

    payload = {
        "git_url": url_components.href,
        "slug": uuid.uuid4().hex,
    }
    response = svc_client.post("/datasets.create", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    assert {"result"} == set(response.json.keys())
    assert "master" not in response.json["result"]["remote_branch"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_unlink_file(unlink_file_setup):
    """Check unlinking of a file from a dataset."""
    svc_client, headers, unlink_payload = unlink_file_setup
    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers)

    assert_rpc_response(response)
    assert {"unlinked", "remote_branch"} == set(response.json["result"].keys())
    assert any(p.endswith("README.md") for p in response.json["result"]["unlinked"])


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_unlink_view(svc_client, it_remote_repo_url, identity_headers):
    """Test creating a delayed unlink."""
    response = svc_client.post(
        "/datasets.unlink",
        data=json.dumps(dict(git_url=it_remote_repo_url, is_delayed=True, slug="mydata", include_filters=["data1"])),
        headers=identity_headers,
    )

    assert_rpc_response(response)
    assert response.json["result"]["created_at"]
    assert response.json["result"]["job_id"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_unlink_file_no_filter_error(unlink_file_setup):
    """Check for correct exception raise when no filters specified."""
    svc_client, headers, unlink_payload = unlink_file_setup

    unlink_payload.pop("include_filters")
    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers)

    assert_rpc_response(response, "error")
    assert UserInvalidGenericFieldsError.code == response.json["error"]["code"]


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_unlink_file_exclude(unlink_file_setup):
    """Check unlinking of a file from a dataset with exclude."""
    svc_client, headers, unlink_payload = unlink_file_setup
    unlink_payload["exclude_filters"] = unlink_payload.pop("include_filters")

    response = svc_client.post("/datasets.unlink", data=json.dumps(unlink_payload), headers=headers)

    assert_rpc_response(response, "error")
    assert UserDatasetsUnlinkError.code == response.json["error"]["code"]
