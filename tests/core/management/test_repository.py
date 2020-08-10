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
"""Repository tests."""

import tempfile
from pathlib import Path

from renku import LocalClient
from renku.core.commands.dataset import create_dataset


def test_latest_version(project):
    """Test returning the latest version of `SoftwareAgent`."""
    from renku import __version__

    create_dataset(
        "ds1", title="", description="", creators=[],
    )

    agent_version = LocalClient(project).latest_agent
    assert __version__ == agent_version


def test_latest_version_user_commits(project):
    """Test retrieval of `SoftwareAgent` with latest non-renku command."""
    from git import Repo

    from renku import __version__

    create_dataset(
        "ds1", title="", description="", creators=[],
    )

    myfile = Path("myfile")
    myfile.write_text("123")

    repo = Repo(project)
    repo.index.add([str(myfile)])
    repo.index.commit("added myfile")

    agent_version = LocalClient(project).latest_agent
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
                    "name: {{ name }}",
                    "description: {{ description }}",
                    "created: {{ date_created }}",
                    "updated: {{ date_updated }}",
                ]
            )
            metadata = {
                "name": "name",
                "description": "description",
                "date_created": "now",
                "date_updated": "now",
            }
        local_client.import_from_template(template_path, metadata)
        compiled_file = local_client.path / output_file
        compiled_content = compiled_file.read_text()
        expected_content = "name: name" "description: description" "created: now" "updated: now"
        assert expected_content == compiled_content
