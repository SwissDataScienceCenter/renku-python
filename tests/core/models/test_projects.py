# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Test projects API."""
from datetime import timezone

from freezegun import freeze_time

from renku.core.models.projects import Project


def test_project_serialization(client):
    """Test project serialization with JSON-LD context."""
    from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION

    with freeze_time("2017-03-01T08:00:00.000000+00:00") as frozen_time:
        project_time = frozen_time().replace(tzinfo=timezone.utc)
        project = Project(name="demo", client=client)
        assert project.name == "demo"
        assert project.created == project_time

    data = project.as_jsonld()
    assert "http://schema.org/Project" in data["@type"]
    assert "http://www.w3.org/ns/prov#Location" in data["@type"]

    assert "demo" == data["http://schema.org/name"]
    assert project_time.isoformat("T") == data["http://schema.org/dateCreated"]
    assert str(SUPPORTED_PROJECT_VERSION) == data["http://schema.org/schemaVersion"]


def test_project_creator_deserialization(client, project):
    """Check that the correct creator is returned on deserialization."""
    from renku.core.models.provenance.agents import Person

    # modify the project metadata to change the creator
    project = client.project
    project.creator = Person(email="johndoe@example.com", name="Johnny Doe")
    project.to_yaml()
    client.repo.git.commit("-a", "--amend", "-C", "HEAD", "--author", "Johnny Doe <johndoe@example.com>", "--no-verify")

    # the project creator should always be the one in the metadata
    assert "johndoe@example.com" == client.project.creator.email
    assert "Johnny Doe" == client.project.creator.name
    assert client.project.creator.label == client.project.creator.name

    # Remove the creator from metadata
    project = client.project
    project.creator = None
    project.to_yaml()
    client.repo.git.commit("-a", "--amend", "-C", "HEAD", "--author", "Jane Doe <janedoe@example.com>", "--no-verify")

    # now the creator should be the one from the commit
    project = Project.from_yaml(client.renku_metadata_path, client=client)
    assert "janedoe@example.com" == project.creator.email
    assert "Jane Doe" == project.creator.name
    assert project.creator.label == project.creator.name
