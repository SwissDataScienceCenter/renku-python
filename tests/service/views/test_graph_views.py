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
"""Renku service graph view tests."""
import json

import pytest
from flaky import flaky

from conftest import IT_REMOTE_REPO_URL
from tests.service.views.test_dataset_views import assert_rpc_response


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_graph_build_view(svc_client_cache, authentication_headers_raw):
    """Create a new graph build job successfully."""
    svc_client, _, cache = svc_client_cache

    # Assure that no jobs are enqueued before invoking the endpoint.
    cache_state = "".join([key.decode("utf-8") for key in cache.cache.keys()])
    assert "rq:queue:graph.jobs" not in cache_state
    assert "rq:job" not in cache_state

    payload = {
        "git_url": IT_REMOTE_REPO_URL,
        "revision": "HEAD",
        "callback_url": "https://webhook.site",
    }

    response = svc_client.post("/graph.build", data=json.dumps(payload), headers=authentication_headers_raw)

    assert response
    assert_rpc_response(response)
    assert {"result": {"status": "ok"}} == response.json

    # Assure that jobs are enqueued after invoking the endpoint.
    cache_state = "".join([key.decode("utf-8") for key in cache.cache.keys()])
    assert "rq:queue:graph.jobs" in cache_state
    assert "rq:job" in cache_state


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=1, min_passes=1)
def test_graph_build_no_callback(svc_client_cache, authentication_headers_raw):
    """Try to create a new graph build job."""
    svc_client, _, cache = svc_client_cache
    payload = {"git_url": IT_REMOTE_REPO_URL, "revision": "HEAD"}

    response = svc_client.post("/graph.build", data=json.dumps(payload), headers=authentication_headers_raw)

    assert response
    assert {
        "error": {"code": -32602, "reason": "Validation error: `callback_url` - Missing data for required field."}
    } == response.json


@pytest.mark.service
@pytest.mark.integration
@flaky(max_runs=1, min_passes=1)
def test_graph_build_no_revision(svc_client_cache, authentication_headers_raw):
    """Create a new graph build job successfully."""
    svc_client, _, cache = svc_client_cache

    payload = {"git_url": IT_REMOTE_REPO_URL, "callback_url": "http://localhost:8080"}

    response = svc_client.post("/graph.build", data=json.dumps(payload), headers=authentication_headers_raw)
    assert response
    assert_rpc_response(response)
