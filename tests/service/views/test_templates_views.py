# -*- coding: utf-8 -*-
#
# Copyright 2020-2022 - Swiss Data Science Center (SDSC)
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
"""Renku service templates view tests."""
import base64
import json
from copy import deepcopy
from io import BytesIO
from time import sleep

import pytest

from renku.core.management.project_config import config
from renku.core.template.template import fetch_templates_source
from renku.core.util.os import normalize_to_ascii
from renku.domain_model.template import TEMPLATE_MANIFEST, TemplatesManifest
from renku.infrastructure.repository import Repository
from renku.ui.service.errors import (
    ProgramProjectCreationError,
    UserAnonymousError,
    UserProjectCreationError,
    UserRepoUrlInvalidError,
    UserTemplateInvalidError,
)
from tests.utils import retry_failed


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_read_manifest_from_template(svc_client_with_templates):
    """Check reading manifest template."""
    from PIL import Image

    svc_client, headers, template_params = svc_client_with_templates

    response = svc_client.get("/templates.read_manifest", query_string=template_params, headers=headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["templates"]

    templates = response.json["result"]["templates"]
    assert len(templates) > 0

    default_template = templates[template_params["index"] - 1]
    assert default_template["folder"] == template_params["id"]
    assert "icon" in default_template and default_template["icon"]
    icon = Image.open(BytesIO(base64.b64decode(default_template["icon"])))
    assert icon.size == (256, 256)


@pytest.mark.service
@pytest.mark.integration
def test_compare_manifests(svc_client_with_templates):
    """Check reading manifest template."""
    svc_client, headers, template_params = svc_client_with_templates

    response = svc_client.get("/templates.read_manifest", query_string=template_params, headers=headers)

    assert response
    assert {"result"} == set(response.json.keys())
    assert response.json["result"]["templates"]

    templates_source = fetch_templates_source(source=template_params["url"], reference=template_params["ref"])
    manifest_file = templates_source.path / TEMPLATE_MANIFEST

    manifest = TemplatesManifest.from_path(manifest_file).get_raw_content()

    assert manifest_file and manifest_file.exists()
    assert manifest

    templates_service = response.json["result"]["templates"]
    templates_local = manifest
    default_index = template_params["index"] - 1

    if "icon" in templates_service[default_index]:
        del templates_service[default_index]["icon"]
    if "icon" in templates_local[default_index]:
        del templates_local[default_index]["icon"]

    assert templates_service[default_index] == templates_local[default_index]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize(
    "template_url,error",
    [
        ("definitely_not_a_valid_URL", UserRepoUrlInvalidError),
        ("https://renkulabnonexistingwebsite.io", UserRepoUrlInvalidError),
        ("https://datascience.ch", UserRepoUrlInvalidError),
        ("https://github.com/SwissDataScienceCenter/renku-python", UserTemplateInvalidError),
    ],
)
def test_read_manifest_from_wrong_template(svc_client_with_templates, template_url, error):
    """Check reading manifest template."""
    svc_client, headers, template_params = svc_client_with_templates
    template_params["url"] = template_url

    response = svc_client.get("/templates.read_manifest", query_string=template_params, headers=headers)

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert error.code == response.json["error"]["code"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_project_from_template(svc_client_templates_creation, client_database_injection_manager):
    """Check creating project from a valid template."""
    from renku.core.management.client import LocalClient
    from renku.ui.service.serializers.headers import RenkuHeaders
    from renku.ui.service.utils import CACHE_PROJECTS_PATH

    svc_client, headers, payload, rm_remote = svc_client_templates_creation

    payload["data_directory"] = "my-folder/"

    response = svc_client.post("/templates.create_project", data=json.dumps(payload), headers=headers)

    assert response
    assert {"result"} == set(response.json.keys()), response.json["error"]
    stripped_name = normalize_to_ascii(payload["project_name"])
    assert stripped_name == response.json["result"]["slug"]
    expected_url = f"{payload['project_repository']}/{payload['project_namespace']}/{stripped_name}"
    assert expected_url == response.json["result"]["url"]

    # NOTE: assert correct git user is set on new project
    user_data = RenkuHeaders.decode_user(headers["Renku-User"])
    project_path = (
        CACHE_PROJECTS_PATH
        / user_data["user_id"]
        / response.json["result"]["project_id"]
        / payload["project_namespace"]
        / stripped_name
    )
    reader = Repository(project_path).get_configuration()
    assert reader.get_value("user", "email") == user_data["email"]
    assert reader.get_value("user", "name") == user_data["name"]

    with config.with_path(project_path):
        client = LocalClient()
        with client_database_injection_manager(client):
            project = client.project

    expected_id = f"/projects/{payload['project_namespace']}/{stripped_name}"
    assert expected_id == project.id
    assert client.data_dir == "my-folder/"

    # NOTE: Assert backwards compatibility metadata.yml was created
    old_metadata_path = project_path / ".renku/metadata.yml"
    assert old_metadata_path.exists()
    assert "'http://schema.org/schemaVersion': '9'" in old_metadata_path.read_text()

    # NOTE:  successfully re-use old name after cleanup
    assert rm_remote() is True
    sleep(1)  # NOTE: sleep to make sure remote isn't locked
    response = svc_client.post("/templates.create_project", data=json.dumps(payload), headers=headers)
    assert response
    assert {"result"} == set(response.json.keys())
    assert expected_url == response.json["result"]["url"]


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_create_project_from_template_failures(svc_client_templates_creation):
    """Check failures when creating project from a valid template providing wrong project values."""
    svc_client, headers, payload, rm_remote = svc_client_templates_creation

    # NOTE: fail on anonymous user
    anonymous_headers = deepcopy(headers)
    anonymous_headers["Authorization"] = "None"
    anonymous_headers["Renku-User"] = "None"
    response = svc_client.post("/templates.create_project", data=json.dumps(payload), headers=anonymous_headers)

    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserAnonymousError.code == response.json["error"]["code"]

    # NOTE: fail on missing project name
    payload_missing_project = deepcopy(payload)
    payload_missing_project["project_name"] = ""

    response = svc_client.post("/templates.create_project", data=json.dumps(payload_missing_project), headers=headers)
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserProjectCreationError.code == response.json["error"]["code"]
    assert "project name" in response.json["error"]["devMessage"].lower()

    # NOTE: fail on wrong git url - unexpected when invoked from the UI
    payload_wrong_repo = deepcopy(payload)
    payload_wrong_repo["project_repository"] = "###"

    response = svc_client.post("/templates.create_project", data=json.dumps(payload_wrong_repo), headers=headers)
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserProjectCreationError.code == response.json["error"]["code"]
    assert "git_url" in response.json["error"]["devMessage"]

    # NOTE: missing fields -- unlikely to happen. If that is the case, we should determine if it's a user error or not
    payload_missing_field = deepcopy(payload)
    del payload_missing_field["project_repository"]

    response = svc_client.post("/templates.create_project", data=json.dumps(payload_missing_field), headers=headers)
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert ProgramProjectCreationError.code == response.json["error"]["code"]
    assert "missing data for required field" in response.json["error"]["devMessage"].lower()

    # NOTE: wrong template identifier
    payload_fake_id = deepcopy(payload)
    fake_identifier = "__FAKE_IDENTIFIER__"
    payload_fake_id["identifier"] = fake_identifier

    response = svc_client.post("/templates.create_project", data=json.dumps(payload_fake_id), headers=headers)
    assert 200 == response.status_code
    assert {"error"} == set(response.json.keys())
    assert UserProjectCreationError.code == response.json["error"]["code"]
    assert "does not exist" in response.json["error"]["devMessage"]
    assert fake_identifier in response.json["error"]["devMessage"]

    # NOTE: fail on missing parameters
    if len(payload["parameters"]) > 0:
        payload_without_parameters = deepcopy(payload)
        payload_without_parameters["parameters"] = []
        response = svc_client.post(
            "/templates.create_project", data=json.dumps(payload_without_parameters), headers=headers
        )

        assert 200 == response.status_code
        assert {"error"} == set(response.json.keys())
        assert UserProjectCreationError.code == response.json["error"]["code"]
        assert "does not exist" in response.json["error"]["devMessage"]
        assert fake_identifier in response.json["error"]["devMessage"]
