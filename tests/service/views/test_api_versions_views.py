#
# Copyright 2022 -Swiss Data Science Center (SDSC)
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
"""Renku service version view tests."""

import pytest

from renku.ui.service.views.api_versions import MAXIMUM_VERSION, MINIMUM_VERSION


@pytest.mark.service
@pytest.mark.integration
def test_versions_differences(svc_client, identity_headers, it_remote_repo_url):
    """Test different versions return different responses."""
    query_string = dict(git_url=it_remote_repo_url)
    max_ver = MAXIMUM_VERSION.name
    min_ver = MINIMUM_VERSION.name

    response_old = svc_client.get(
        f"/{min_ver}/cache.migrations_check", query_string=query_string, headers=identity_headers
    )

    assert 200 == response_old.status_code
    assert response_old.json["result"]["core_compatibility_status"]["migration_required"] is True
    assert "ssh_supported" not in response_old.json["result"]["template_status"]

    response_new = svc_client.get(
        f"/{max_ver}/cache.migrations_check", query_string=query_string, headers=identity_headers
    )
    assert 200 == response_new.status_code
    assert response_new.json["result"]["core_compatibility_status"]["migration_required"] is True
    assert "ssh_supported" in response_new.json["result"]["template_status"]

    response_default = svc_client.get("/cache.migrations_check", query_string=query_string, headers=identity_headers)
    assert 200 == response_default.status_code
    assert response_default.json["result"]["core_compatibility_status"]["migration_required"] is True
    assert response_default.json["result"].keys() == response_new.json["result"].keys()
    assert response_default.json["result"].keys() == response_old.json["result"].keys()
    assert (
        response_default.json["result"]["template_status"].keys()
        == response_new.json["result"]["template_status"].keys()
    )
    assert (
        response_default.json["result"]["template_status"].keys()
        != response_old.json["result"]["template_status"].keys()
    )
    assert (
        response_default.json["result"]["core_compatibility_status"]["migration_required"]
        == response_new.json["result"]["core_compatibility_status"]["migration_required"]
    )
