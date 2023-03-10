#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku CLI fixtures for providers testing."""
import json
import os
import posixpath
import re
import urllib
import warnings

import pytest

from renku.core import errors
from renku.core.config import set_value


@pytest.fixture
def zenodo_sandbox(project):
    """Configure environment to use Zenodo sandbox environment."""
    os.environ["ZENODO_USE_SANDBOX"] = "true"

    access_token = os.getenv("ZENODO_ACCESS_TOKEN", "")
    set_value("zenodo", "access_token", access_token)

    project.repository.add(".renku/renku.ini")
    project.repository.commit("update renku.ini")


@pytest.fixture
def olos_sandbox(project):
    """Configure environment to use Zenodo sandbox environment."""
    access_token = os.getenv("OLOS_ACCESS_TOKEN", "")
    set_value("olos", "access_token", access_token)
    project.repository.add(".renku/renku.ini")
    project.repository.commit("update renku.ini")


@pytest.fixture(scope="module")
def dataverse_demo_cleanup(request):
    """Delete all Dataverse datasets at the end of the test session."""
    from renku.core.util import requests

    server_url = "https://demo.dataverse.org"
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    headers = {"X-Dataverse-key": access_token}

    def remove_datasets():
        url = f"{server_url}/api/v1/dataverses/sdsc-test-dataverse/contents"
        try:
            response = requests.get(url=url, headers=headers)
        except errors.RequestError:
            warnings.warn("Cannot clean up Dataverse datasets")
            return

        if response.status_code != 200:
            warnings.warn("Cannot clean up Dataverse datasets")
            return

        datasets = response.json().get("data", [])

        for dataset in datasets:
            id = dataset.get("id")
            if id is not None:
                url = f"https://demo.dataverse.org/api/v1/datasets/{id}"
                try:
                    requests.delete(url=url, headers=headers)
                except errors.RequestError:
                    pass

    request.addfinalizer(remove_datasets)


@pytest.fixture
def dataverse_demo(project, dataverse_demo_cleanup):
    """Configure environment to use Dataverse demo environment."""
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    set_value("dataverse", "access_token", access_token)
    set_value("dataverse", "server_url", "https://demo.dataverse.org")

    project.repository.add(".renku/renku.ini")
    project.repository.commit("renku.ini")


@pytest.fixture
def cloud_storage_credentials(project):
    """Set credentials for all cloud storages."""
    # S3
    s3_access_key_id = os.getenv("CLOUD_STORAGE_S3_ACCESS_KEY_ID", "")
    s3_secret_access_key = os.getenv("CLOUD_STORAGE_S3_SECRET_ACCESS_KEY", "")
    s3_section = "os.zhdk.cloud.switch.ch"
    set_value(section=s3_section, key="access-key-id", value=s3_access_key_id, global_only=True)
    set_value(section=s3_section, key="secret-access-key", value=s3_secret_access_key, global_only=True)

    # Azure
    azure_account = "renkupythontest1"
    azure_key = os.getenv("CLOUD_STORAGE_AZURE_KEY", "")
    azure_section = f"{azure_account}.blob.core.windows.net"
    set_value(section=azure_section, key="account", value=azure_account, global_only=True)
    set_value(section=azure_section, key="key", value=azure_key, global_only=True)


@pytest.fixture
def doi_responses():
    """Responses for doi.org requests."""
    import responses

    from renku.core.dataset.providers.dataverse import DATAVERSE_API_PATH, DATAVERSE_VERSION_API
    from renku.core.dataset.providers.doi import DOI_BASE_URL

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def doi_callback(request):
            response_url = "https://dataverse.harvard.edu/citation" "?persistentId=doi:10.11588/data/xyz12345"
            if "zenodo" in request.url:
                response_url = "https://zenodo.org/record/3363060"
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    {
                        "type": "dataset",
                        "id": request.url,
                        "author": [{"family": "Doe", "given": "John"}],
                        "contributor": [{"contributorType": "ContactPerson", "family": "Doe", "given": "John"}],
                        "issued": {"date-parts": [[2019]]},
                        "abstract": "Test Dataset",
                        "DOI": "10.11588/data/xyz12345",
                        "publisher": "heiDATA",
                        "title": "dataset",
                        "URL": response_url,
                    }
                ),
            )

        response.add_callback(method="GET", url=re.compile(f"{DOI_BASE_URL}/.*"), callback=doi_callback)

        def version_callback(_):
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps({"status": "OK", "data": {"version": "4.1.3", "build": "abc123"}}),
            )

        base_url = "https://dataverse.harvard.edu"

        url_parts = list(urllib.parse.urlparse(base_url))
        url_parts[2] = posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSION_API)
        pattern = f"{urllib.parse.urlunparse(url_parts)}.*"

        response.add_callback(method="GET", url=re.compile(pattern), callback=version_callback)
        yield response
