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
"""Renku service graph jobs tests."""
import json

import pytest


@pytest.mark.service
@pytest.mark.jobs
@pytest.mark.integration
@pytest.mark.parametrize("revision", [None, "HEAD", "HEAD^^", "HEAD^^..HEAD"])
def test_graph_export_job(svc_client_cache, it_remote_repo_url, revision):
    """Test graph export job."""
    svc_client, headers, _ = svc_client_cache

    payload = {
        "git_url": it_remote_repo_url,
        "revision": revision,
        "callback_url": "https://webhook.site",
        "migrate_project": True,
    }

    response = svc_client.get("/graph.export", data=json.dumps(payload), headers=headers)
    assert response
    assert {"graph"} == set(response.json["result"].keys())
