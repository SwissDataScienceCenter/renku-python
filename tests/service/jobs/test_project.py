# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 - Swiss Data Science Center (SDSC)
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
"""Renku service project related job tests."""
import json

import jwt
import pytest
from werkzeug.utils import secure_filename

from renku.service.jobs.project import migrate_job
from renku.service.serializers.headers import JWT_TOKEN_SECRET, encode_b64


@pytest.mark.service
@pytest.mark.integration
def test_migrations_job(svc_client_setup):
    """Check migrations job for successful execution of migrations."""
    svc_client, headers, project_id, _ = svc_client_setup

    decoded = jwt.decode(headers["Renku-User"], JWT_TOKEN_SECRET, algorithms=["HS256"], audience="renku",)
    user_data = {
        "fullname": decoded["name"],
        "email": decoded["email"],
        "user_id": encode_b64(secure_filename(decoded["sub"])),
        "token": headers["Authorization"].split("Bearer ")[-1],
    }

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, is_delayed=True)), headers=headers
    )

    assert 200 == response.status_code
    job_id = response.json["result"]["job_id"]

    assert migrate_job(user_data, project_id, job_id, False, True, True, False, "my migrate commit") is None

    response = svc_client.get("/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)

    assert 200 == response.status_code
    assert not response.json["result"]["migration_required"]
    assert response.json["result"]["project_supported"]
