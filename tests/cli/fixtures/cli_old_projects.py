# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Renku CLI fixtures for old project management."""

from pathlib import Path
from typing import Generator, List, Tuple

import pytest

from renku.core.util.contexts import with_project_metadata
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository
from tests.fixtures.repository import RenkuProject
from tests.utils import clone_compressed_repository


@pytest.fixture(
    params=["old-datasets-v0.3.0.git", "old-datasets-v0.5.1.git", "test-renku-v0.3.0.git", "pre-2.0-project.git"]
)
def old_project(request, tmp_path) -> Generator[RenkuProject, None, None]:
    """Prepares a testing repo created by old version of renku."""
    from renku.core.util.contexts import chdir

    name = request.param
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.path):
        yield RenkuProject(path=repository.path, repository=repository)


@pytest.fixture(
    params=[
        {
            "name": "old-workflows-v0.10.3.git",
            "log_path": "cat-output.txt",
            "expected_strings": [
                "catoutput.txt",
                "stdin.txt",
                "stdout.txt",
            ],
        },
        {
            "name": "old-workflows-complicated-v0.10.3.git",
            "log_path": "concat2.txt",
            "expected_strings": [
                "concat2.txt",
                "output_rand",
                "input2.txt",
            ],
        },
    ],
)
def old_workflow_project(request, tmp_path) -> Generator[Tuple[RenkuProject, List[str]], None, None]:
    """Prepares a testing repo created by old version of renku."""
    from renku.core.util.contexts import chdir

    name = request.param["name"]
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.path):
        yield RenkuProject(path=repository.path, repository=repository), request.param["expected_strings"]


@pytest.fixture(params=["old-datasets-v0.9.1.git"])
def old_dataset_project(request, tmp_path) -> Generator[RenkuProject, None, None]:
    """Prepares a testing repo created by old version of renku."""
    from renku.core.util.contexts import chdir

    name = request.param
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.path), project_context.with_path(repository.path):
        yield RenkuProject(path=repository.path, repository=repository)


@pytest.fixture
def old_repository_with_submodules(tmp_path) -> Generator[RenkuProject, None, None]:
    """Prepares a testing repo that has datasets using git submodules."""
    import tarfile

    from renku.core.util.contexts import chdir

    name = "old-datasets-v0.6.0-with-submodules"
    base_path = Path(__file__).parent / ".." / ".." / "data" / f"{name}.tar.gz"
    working_dir = tmp_path / name

    with tarfile.open(str(base_path), "r") as repo:
        repo.extractall(working_dir)

    repo_path = working_dir / name
    repository = Repository(repo_path)

    with chdir(repo_path):
        yield RenkuProject(path=repository.path, repository=repository)


@pytest.fixture
def unsupported_project(project, with_injection) -> Generator[RenkuProject, None, None]:
    """A newer project version."""
    with with_injection():
        with with_project_metadata() as project_metadata:
            impossible_newer_version = 42000
            project_metadata.version = impossible_newer_version

    project.repository.add(".renku")
    project.repository.commit("update renku.ini", no_verify=True)

    yield project
