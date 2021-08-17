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

from tests.service.views.test_dataset_views import assert_rpc_response
from tests.utils import retry_failed


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_graph_export_view(svc_client_cache, it_remote_repo_url):
    """Create a new graph export job successfully."""
    svc_client, headers, _ = svc_client_cache

    payload = {
        "git_url": it_remote_repo_url,
        "revision": "HEAD",
        "callback_url": "https://webhook.site",
        "migrate_project": True,
    }

    response = svc_client.get("/graph.export", data=json.dumps(payload), headers=headers)

    assert response
    assert_rpc_response(response)
    assert {"result": {"graph": None}} == response.json


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_graph_export_no_callback(svc_client_cache, it_remote_repo_url):
    """Try to create a new graph export job."""
    svc_client, headers, _ = svc_client_cache
    payload = {"git_url": it_remote_repo_url, "revision": "HEAD", "migrate_project": True}

    response = svc_client.get("/graph.export", data=json.dumps(payload), headers=headers)

    assert response
    assert {"result": {"graph": None}} == response.json


@pytest.mark.service
@pytest.mark.integration
@retry_failed
def test_graph_export_no_revision(svc_client_cache, it_remote_repo_url):
    """Create a new graph export job successfully."""
    svc_client, headers, _ = svc_client_cache

    payload = {"git_url": it_remote_repo_url, "callback_url": "https://webhook.site", "migrate_project": True}

    response = svc_client.get("/graph.export", data=json.dumps(payload), headers=headers)
    assert response
    assert_rpc_response(response)
