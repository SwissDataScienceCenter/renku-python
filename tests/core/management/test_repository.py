# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Repository tests."""

import tempfile
from pathlib import Path

from renku.core.commands.dataset import create_dataset_command
from renku.core.management.client import LocalClient
from renku.core.metadata.repository import Repository


def test_latest_version(project, client_database_injection_manager):
    """Test returning the latest version of `SoftwareAgent`."""
    from renku import __version__

    create_dataset_command().build().execute("ds1", title="", description="", creators=[])

    client = LocalClient(project)
    with client_database_injection_manager(client):
        agent_version = client.latest_agent
    assert __version__ == agent_version


def test_latest_version_user_commits(project, client_database_injection_manager):
    """Test retrieval of `SoftwareAgent` with latest non-renku command."""
    from renku import __version__

    create_dataset_command().build().execute("ds1", title="", description="", creators=[])

    file = Path("my-file")
    file.write_text("123")

    repository = Repository(project)
    repository.add(file)
    repository.commit("added my-file")

    client = LocalClient(project)
    with client_database_injection_manager(client):
        agent_version = client.latest_agent
    assert __version__ == agent_version


def test_init_repository(local_client):
    """Test initializing an empty repository."""
    local_client.init_repository()
    assert (local_client.path / ".git").exists()
    assert (local_client.path / ".git" / "HEAD").exists()
    assert not (local_client.path / ".renku").exists()


def test_import_from_template(local_client):
    """Test importing data from template."""
    output_file = "metadata.yml"
    local_client.init_repository()
    with tempfile.TemporaryDirectory() as tempdir:
        template_path = Path(tempdir)
        fake_template_file = template_path / output_file
        with fake_template_file.open("w") as dest:
            dest.writelines(
                [
                    "name: {{ __name__ }}",
                    "description: {{ description }}",
                    "created: {{ date_created }}",
                    "updated: {{ date_updated }}",
                ]
            )
            metadata = {
                "__name__": "name",
                "description": "description",
                "date_created": "now",
                "date_updated": "now",
                "__template_source__": "renku",
                "__template_ref__": "master",
                "__template_id__": "python-minimal",
                "__namespace__": "",
                "__repository__": "",
                "__project_slug__": "",
            }
        local_client.import_from_template(template_path, metadata)
        compiled_file = local_client.path / output_file
        compiled_content = compiled_file.read_text()
        expected_content = "name: name" "description: description" "created: now" "updated: now"
        assert expected_content == compiled_content
