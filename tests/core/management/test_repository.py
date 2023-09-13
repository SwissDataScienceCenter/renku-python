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
"""Repository tests."""

import tempfile
from pathlib import Path

from renku.command.dataset import create_dataset_command
from renku.core.init import init_repository
from renku.domain_model.project_context import project_context


def test_latest_version(project, with_injection):
    """Test returning the latest version of `SoftwareAgent`."""
    from renku import __version__

    create_dataset_command().build().execute("ds1", name="", description="", creators=[])

    with project_context.with_path(project.path), with_injection():
        assert __version__ == project_context.latest_agent


def test_latest_version_user_commits(project, with_injection):
    """Test retrieval of `SoftwareAgent` with latest non-renku command."""
    from renku import __version__

    create_dataset_command().build().execute("ds1", name="", description="", creators=[])

    file = Path("my-file")
    file.write_text("123")

    project.repository.add(file)
    project.repository.commit("added my-file")

    with project_context.with_path(project.path), with_injection():
        assert __version__ == project_context.latest_agent


def test_init_repository():
    """Test initializing an empty repository."""
    with tempfile.TemporaryDirectory() as tempdir, project_context.with_path(tempdir):
        assert not (project_context.path / ".git").exists()

        init_repository()

        assert (project_context.path / ".git").exists()
        assert (project_context.path / ".git" / "HEAD").exists()
        assert not (project_context.path / ".renku").exists()
