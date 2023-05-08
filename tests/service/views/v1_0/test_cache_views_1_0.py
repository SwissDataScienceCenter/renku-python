#
# Copyright 2019-2023 - Swiss Data Science Center (SDSC)
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
from unittest.mock import MagicMock

import pytest

from renku.ui.service.errors import IntermittentProjectTemplateUnavailable
from tests.utils import assert_rpc_response


@pytest.mark.service
@pytest.mark.integration
@pytest.mark.remote_repo("old")
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


@pytest.mark.service
@pytest.mark.integration
def test_check_migrations_local_1_0(svc_client_setup):
    """Check if migrations are required for a local project."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    response = svc_client.get("/1.0/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers)
    assert 200 == response.status_code

    assert not response.json["result"]["core_compatibility_status"]["migration_required"]
    assert not response.json["result"]["template_status"]["newer_template_available"]
    assert not response.json["result"]["dockerfile_renku_status"]["automated_dockerfile_update"]
    assert response.json["result"]["project_supported"]
    assert response.json["result"]["project_renku_version"]
    assert response.json["result"]["core_renku_version"]
    assert "template_source" in response.json["result"]["template_status"]
    assert "template_ref" in response.json["result"]["template_status"]
    assert "template_id" in response.json["result"]["template_status"]
    assert "automated_template_update" in response.json["result"]["template_status"]


@pytest.mark.service
@pytest.mark.integration
def test_migrate_wrong_template_source_1_0(svc_client_setup, monkeypatch):
    """Check if migrations gracefully fail when the project template is not available."""
    svc_client, headers, project_id, _, _ = svc_client_setup

    # NOTE: fake source
    with monkeypatch.context() as monkey:
        import renku.core.template.usecase

        monkey.setattr(
            renku.core.template.usecase.TemplateMetadata, "source", property(MagicMock(return_value="https://FAKE_URL"))
        )

        response = svc_client.get(
            "/1.0/cache.migrations_check", query_string=dict(project_id=project_id), headers=headers
        )

        assert_rpc_response(response, "error")
        assert IntermittentProjectTemplateUnavailable.code == response.json["error"]["code"]
