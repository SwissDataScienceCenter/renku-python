#
# Copyright 2020-2023 -Swiss Data Science Center (SDSC)
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
"""Renku service project clone tests."""
import json
import time
import uuid

import pytest
from marshmallow import EXCLUDE
from werkzeug.utils import secure_filename

from renku.ui.service.controllers.utils.project_clone import user_project_clone
from renku.ui.service.serializers.headers import encode_b64
from renku.ui.service.serializers.templates import ProjectTemplateRequest
from tests.utils import assert_rpc_response, modified_environ, retry_failed


@pytest.mark.integration
@retry_failed
def test_service_user_project_clone(svc_client_cache):
    """Test service user project clone."""
    client, _, cache = svc_client_cache

    user_data = {
        "user_id": uuid.uuid4().hex,
        "email": "contact@renkulab.io",
        "fullname": "renku the frog",
        "token": "None",
    }
    project_data = {
        "project_name": "deadbeef",
        "project_repository": "https://dev.renku.ch",
        "project_namespace": "renku-qa",
        "identifier": "0xdeadbeef",
        "depth": 1,
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "owner": "SwissDataScienceCenter",
    }

    project_data = ProjectTemplateRequest().load({**user_data, **project_data}, unknown=EXCLUDE)
    project_one = user_project_clone(user_data, project_data)
    assert project_one.age >= 0
    assert not project_one.ttl_expired()
    assert project_one.exists()
    old_path = project_one.abs_path

    with modified_environ(RENKU_SVC_CLEANUP_TTL_PROJECTS="1"):
        time.sleep(1)
        assert project_one.ttl_expired()

    with modified_environ(RENKU_SVC_CLEANUP_TTL_PROJECTS="3600"):
        project_two = user_project_clone(user_data, project_data)
        assert project_two.age >= 0
        assert not project_two.ttl_expired()
        assert project_two.exists()

        new_path = project_two.abs_path
        assert old_path == new_path
        user = cache.get_user(user_data["user_id"])
        projects = [project.project_id for project in cache.get_projects(user)]
        assert project_one.project_id in projects
        assert project_two.project_id in projects


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_service_user_non_existing_project_clone(svc_client_cache, it_remote_repo_url):
    """Check reading manifest template."""
    svc_client, headers, cache = svc_client_cache
    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    # NOTE: clone a valid repo and verify there is one project in the cache
    payload = {"git_url": it_remote_repo_url, "depth": -1}
    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response)
    projects = list(cache.get_projects(user))
    assert 1 == len(projects)

    # NOTE: invalidate the project
    cache.invalidate_project(user, projects[0].project_id)
    projects = list(cache.get_projects(user))
    assert 0 == len(projects)

    # NOTE: try to clone a non-existing repo and verify no other projects are added to the cache
    payload["git_url"] = f"{it_remote_repo_url}-non-existing-project-url"
    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=headers)

    assert_rpc_response(response, "error")
    projects = list(cache.get_projects(user))
    assert 0 == len(projects)
