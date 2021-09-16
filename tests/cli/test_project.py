# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test ``project`` command."""

import json

from renku.cli import cli
from renku.core.metadata.gateway.project_gateway import ProjectGateway
from renku.core.models.provenance.agent import Person
from tests.utils import format_result_exception


def test_project_edit(runner, client, subdirectory, client_database_injection_manager):
    """Check project metadata editing."""
    (client.path / "README.md").write_text("Make repo dirty.")

    creator = "Forename Surname [Affiliation]"

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = client.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(
        cli, ["project", "edit", "-d", " new description ", "-c", creator, "--metadata", str(metadata_path)]
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: creator, description, custom_metadata." in result.output
    assert "Warning: No email or wrong format for: Forename Surname" in result.output

    with client_database_injection_manager(client):
        project_gateway = ProjectGateway()
        project = project_gateway.get_project()

    assert " new description " == project.description
    assert isinstance(project.creator, Person)
    assert "Forename Surname" == project.creator.name
    assert "Affiliation" == project.creator.affiliation
    assert metadata == project.annotations[0].body

    assert client.repo.is_dirty()
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after


def test_project_edit_no_change(runner, client):
    """Check project metadata editing does not commit when there is no change."""
    (client.path / "README.md").write_text("Make repo dirty.")

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["project", "edit"], catch_exceptions=False)

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Nothing to update." in result.output

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before
    assert client.repo.is_dirty()
