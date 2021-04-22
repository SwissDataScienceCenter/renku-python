# -*- coding: utf-8 -*-
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
import pathlib
import re
import urllib
import warnings

import pytest
import requests


@pytest.fixture
def zenodo_sandbox(client):
    """Configure environment to use Zenodo sandbox environment."""
    os.environ["ZENODO_USE_SANDBOX"] = "true"

    access_token = os.getenv("ZENODO_ACCESS_TOKEN", "")
    client.set_value("zenodo", "access_token", access_token)

    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")


@pytest.fixture
def olos_sandbox(client):
    """Configure environment to use Zenodo sandbox environment."""
    access_token = os.getenv("OLOS_ACCESS_TOKEN", "")
    client.set_value("olos", "access_token", access_token)
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")


@pytest.fixture(scope="module")
def dataverse_demo_cleanup(request):
    """Delete all Dataverse datasets at the end of the test session."""
    from renku.core.utils.requests import retry

    server_url = "https://demo.dataverse.org"
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    headers = {"X-Dataverse-key": access_token}

    def remove_datasets():
        url = f"{server_url}/api/v1/dataverses/sdsc-test-dataverse/contents"
        try:
            with retry() as session:
                response = session.get(url=url, headers=headers)
        except (ConnectionError, requests.exceptions.RequestException):
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
                    with retry() as session:
                        session.delete(url=url, headers=headers)
                except (ConnectionError, requests.exceptions.RequestException):
                    pass

    request.addfinalizer(remove_datasets)


@pytest.fixture
def dataverse_demo(client, dataverse_demo_cleanup):
    """Configure environment to use Dataverse demo environment."""
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    client.set_value("dataverse", "access_token", access_token)
    client.set_value("dataverse", "server_url", "https://demo.dataverse.org")

    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("renku.ini")


@pytest.fixture
def doi_responses():
    """Responses for doi.org requests."""
    import responses

    from renku.core.commands.providers.dataverse import DATAVERSE_API_PATH, DATAVERSE_VERSION_API
    from renku.core.commands.providers.doi import DOI_BASE_URL

    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:

        def doi_callback(request):
            response_url = "https://dataverse.harvard.edu/citation" "?persistentId=doi:10.11588/data/yyxx1122"
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
                        "DOI": "10.11588/data/yyxx1122",
                        "publisher": "heiDATA",
                        "title": "dataset",
                        "URL": response_url,
                    }
                ),
            )

        response.add_callback(
            method="GET", url=re.compile("{base_url}/.*".format(base_url=DOI_BASE_URL)), callback=doi_callback
        )

        def version_callback(request):
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps({"status": "OK", "data": {"version": "4.1.3", "build": "abcdefg"}}),
            )

        base_url = "https://dataverse.harvard.edu"

        url_parts = list(urllib.parse.urlparse(base_url))
        url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSION_API)
        pattern = "{url}.*".format(url=urllib.parse.urlunparse(url_parts))

        response.add_callback(method="GET", url=re.compile(pattern), callback=version_callback)
        yield response
