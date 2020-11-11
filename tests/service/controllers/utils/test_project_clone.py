# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
import os
import time
import uuid

import pytest
from flaky import flaky
from marshmallow import EXCLUDE

from renku.service.controllers.utils.project_clone import user_project_clone
from renku.service.serializers.templates import ManifestTemplatesRequest


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_service_user_project_clone(svc_client_cache):
    """Test service user project clone."""
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
    project_one = user_project_clone(user_data, project_data)
    time.sleep(1)

    assert project_one.age == 1
    assert not project_one.ttl_expired()
    assert project_one.exists()
    old_path = project_one.abs_path

    os.environ["RENKU_SVC_CLEANUP_TTL_PROJECTS"] = "1"
    time.sleep(1)
    assert project_one.age == 2
    assert project_one.ttl_expired()

    os.environ["RENKU_SVC_CLEANUP_TTL_PROJECTS"] = "3600"
    project_two = user_project_clone(user_data, project_data)
    time.sleep(1)

    assert project_two.age == 1
    assert not project_two.ttl_expired()
    assert project_two.exists()

    new_path = project_two.abs_path
    assert old_path == new_path
    user = cache.get_user(user_data["user_id"])

    projects = [project.project_id for project in cache.get_projects(user)]
    assert project_one.project_id not in projects
    assert project_two.project_id in projects
