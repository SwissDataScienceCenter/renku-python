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
"""Renku service jobs view tests."""
import copy
import time
import uuid
from datetime import datetime

import jwt
import pytest
from marshmallow.utils import isoformat
from werkzeug.utils import secure_filename

from renku.service.cache.models.project import Project
from renku.service.serializers.headers import JWT_TOKEN_SECRET, encode_b64


@pytest.mark.service
def test_jobs_view_identity_protected(svc_client):
    """Check serving of user requested jobs."""
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    response = svc_client.get("/jobs", headers=headers)

    assert {"error"} == set(response.json.keys())
    assert "user identification is incorrect or missing" == response.json["error"]["reason"]


@pytest.mark.service
def test_jobs_view_empty_result(svc_client, identity_headers):
    """Check empty result for user requested jobs."""
    response = svc_client.get("/jobs", headers=identity_headers)

    assert {"result"} == set(response.json.keys())
    assert [] == response.json["result"]["jobs"]


@pytest.mark.service
def test_jobs_view_expected_job(svc_client_cache):
    """Check non-empty result for user requested job."""
    svc_client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    job_data = {
        "job_id": uuid.uuid4().hex,
        "state": "CREATED",
        "renku_op": "dataset_import",
        "created_at": isoformat(datetime.now()),
        "updated_at": isoformat(datetime.now()),
        "extras": {"progress": 42},
    }

    project = Project(project_id="123", user_id=user.user_id, owner="renkumeister", name="testproject")
    project.abs_path.mkdir(parents=True, exist_ok=True)
    project.save()

    job = cache.make_job(user, project=project, job_data=job_data)

    response = svc_client.get("/jobs", headers=headers)
    assert 1 == len(response.json["result"]["jobs"])
    assert {"job_id", "state", "created_at", "updated_at", "extras", "client_extras", "renku_op", "project"} == set(
        response.json["result"]["jobs"][0].keys()
    )

    cache.invalidate_job(user, job.job_id)
    response = svc_client.get("/jobs", headers=headers)
    assert 0 == len(response.json["result"]["jobs"])


@pytest.mark.service
def test_jobs_view_check_exclusion(svc_client_cache):
    """Check non-empty result for user requested jobs."""
    svc_client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})
    excluded_user = cache.ensure_user({"user_id": "excluded_user"})

    for _ in range(10):
        job_data = {
            "job_id": uuid.uuid4().hex,
            "state": "CREATED",
            "created_at": isoformat(datetime.utcnow()),
            "updated_at": isoformat(datetime.utcnow()),
            "extras": {"progress": 42},
            "renku_op": "dataset_import",
        }

        project = Project(project_id="123", user_id=user.user_id, owner="renkumeister", name="testproject")
        project.abs_path.mkdir(parents=True, exist_ok=True)
        project.save()

        job1 = cache.make_job(user, project=project, job_data=job_data)
        assert job1

        new_job = copy.deepcopy(job_data)
        new_job["job_id"] = uuid.uuid4().hex

        job2 = cache.make_job(excluded_user, project=project, job_data=new_job)
        assert job2

        assert job1.job_id != job2.job_id

    response = svc_client.get("/jobs", headers=headers)

    assert {"result"} == set(response.json.keys())
    assert 10 == len(response.json["result"]["jobs"])
    for job in response.json["result"]["jobs"]:
        assert {"job_id", "state", "created_at", "updated_at", "extras", "client_extras", "renku_op", "project"} == set(
            job.keys()
        )


@pytest.mark.service
def test_job_details_auth(svc_client):
    """Check authorization for listing a specific job."""
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    response = svc_client.get("/jobs/myjob", headers=headers)

    assert {"error"} == set(response.json.keys())
    assert "user identification is incorrect or missing" == response.json["error"]["reason"]


@pytest.mark.service
def test_job_details_empty(svc_client, identity_headers):
    """Check job details for a user."""
    response = svc_client.get("/jobs/myjob", headers=identity_headers)

    assert {"result"} == set(response.json.keys())
    assert response.json["result"] is None


@pytest.mark.service
def test_job_details_by_user(svc_client_with_user):
    """Check job details for a user."""
    svc_client, headers, cache, user = svc_client_with_user

    jobs = [
        {
            "job_id": uuid.uuid4().hex,
            "state": "CREATED",
            "created_at": isoformat(datetime.now()),
            "updated_at": isoformat(datetime.now()),
            "extras": {"progress": 42},
            "renku_op": "dataset_import",
        }
        for _ in range(10)
    ]

    project = Project(project_id="123", user_id=user.user_id, owner="renkumeister", name="testproject")
    project.abs_path.mkdir(parents=True, exist_ok=True)
    project.save()

    for job_data in jobs:
        cache.make_job(user, job_data=job_data, project=project)

    jwt_data = {
        "jti": "12345",
        "exp": int(time.time()) + 1e6,
        "nbf": 0,
        "iat": 1595317694,
        "iss": "https://stable.dev.renku.ch/auth/realms/Renku",
        "aud": ["renku"],
        "sub": "12345",
        "typ": "ID",
        "azp": "renku",
        "nonce": "12345",
        "auth_time": 1595317694,
        "session_state": "12345",
        "acr": "1",
        "email_verified": False,
        "preferred_username": "user1@platform2.com",
        "given_name": "user",
        "family_name": "user one",
        "name": "User One",
        "email": "user1@platform2.com",
    }

    excluded_user_headers = {
        "Content-Type": "application/json",
        "Renku-User": jwt.encode(jwt_data, JWT_TOKEN_SECRET, algorithm="HS256").decode("utf-8"),
        "Authorization": headers["Authorization"],
    }

    for job in jobs:
        response = svc_client.get("/jobs/{0}".format(job["job_id"]), headers=headers)
        assert response
        assert job["job_id"] == response.json["result"]["job_id"]

        response = svc_client.get("/jobs/{0}".format(job["job_id"]), headers=excluded_user_headers)
        assert response.json["result"] is None
