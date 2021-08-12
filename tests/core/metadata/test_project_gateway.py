# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Test project database gateways."""

from datetime import datetime

from renku.core.metadata.gateway.project_gateway import ProjectGateway
from renku.core.models.project import Project
from renku.core.models.provenance.agent import Person


def test_project_gateway_update(dummy_database_injection_manager, monkeypatch):
    """Test updating project metadata."""

    project = Project(
        id=Project.generate_id("namespace", "my_project"),
        name="my_project",
        creator=Person(
            id=Person.generate_id("test@test.com", "Test tester"), email="test@test.com", name="Test tester"
        ),
        date_created=datetime.utcnow(),
    )

    with dummy_database_injection_manager(None):
        project_gateway = ProjectGateway()

        project_gateway.update_project(project)

        stored_project = project_gateway.get_project()

        assert stored_project.id == project.id
        assert stored_project.agent_version
        assert stored_project.name == "my_project"

        monkeypatch.setattr("renku.__version__", "999.999.999")

        stored_project.name = "new name"

        project_gateway.update_project(project)

        stored_project = project_gateway.get_project()

        assert stored_project.id == project.id
        assert stored_project.agent_version == "999.999.999"
        assert stored_project.name == "new name"
