#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Renku service cache view tests."""
import json

import pytest


@pytest.mark.service
@pytest.mark.integration
def test_execute_migrations_1_0(svc_client_setup):
    """Check execution of all migrations."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.post(
        "/1.0/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )

    assert 200 == response.status_code
    assert response.json["result"]["was_migrated"]
    assert any(
        m.startswith("Successfully applied") and m.endswith("migrations.") for m in response.json["result"]["messages"]
    )
    assert "warnings" not in response.json["result"]
    assert "errors" not in response.json["result"]
