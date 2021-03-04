# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku CLI fixtures for old project management."""
from pathlib import Path

import pytest
from git import Repo


def clone_compressed_repository(base_path, name):
    """Decompress and clone a repository."""
    import tarfile

    compressed_repo_path = Path(__file__).parent / ".." / ".." / "data" / f"{name}.tar.gz"
    working_dir = base_path / name

    bare_base_path = working_dir / "bare"

    with tarfile.open(compressed_repo_path, "r") as fixture:
        fixture.extractall(str(bare_base_path))

    bare_path = bare_base_path / name
    repository_path = working_dir / "repository"
    repository = Repo(bare_path, search_parent_directories=True).clone(repository_path)

    return repository


@pytest.fixture(
    params=["old-datasets-v0.3.0.git", "old-datasets-v0.5.0.git", "old-datasets-v0.5.1.git", "test-renku-v0.3.0.git"]
)
def old_project(request, tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku.core.utils.contexts import chdir

    name = request.param
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.working_dir):
        yield repository


@pytest.fixture(
    params=[
        {
            "name": "old-workflows-v0.10.3.git",
            "log_path": "catoutput.txt",
            "expected_strings": [
                "catoutput.txt",
                "_cat.yaml",
                "_echo.yaml",
                "9ecc28b2 stdin.txt",
                "bdc801c6 stdout.txt",
            ],
        },
        {
            "name": "old-workflows-complicated-v0.10.3.git",
            "log_path": "concat2.txt",
            "expected_strings": [
                "concat2.txt",
                "5828275ae5344eba8bad475e7d3cf2d5.cwl",
                "_migrated.yaml",
                "88add2ea output_rand",
                "e6fa6bf3 input2.txt",
            ],
        },
    ],
)
def old_workflow_project(request, tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku.core.utils.contexts import chdir

    name = request.param["name"]
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)
    repository_path = repository.working_dir

    with chdir(repository_path):
        yield {
            "repo": repository,
            "path": repository_path,
            "log_path": request.param["log_path"],
            "expected_strings": request.param["expected_strings"],
        }


@pytest.fixture
def old_dataset_project(tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku import LocalClient
    from renku.core.utils.contexts import chdir

    name = "old-datasets-v0.9.1.git"
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.working_dir):
        yield LocalClient(path=repository.working_dir)


@pytest.fixture
def old_repository_with_submodules(request, tmp_path):
    """Prepares a testing repo that has datasets using git submodules."""
    import tarfile

    from renku.core.utils.contexts import chdir

    name = "old-datasets-v0.6.0-with-submodules"
    base_path = Path(__file__).parent / ".." / ".." / "data" / f"{name}.tar.gz"
    working_dir = tmp_path / name

    with tarfile.open(str(base_path), "r") as repo:
        repo.extractall(working_dir)

    repo_path = working_dir / name
    repo = Repo(repo_path)

    with chdir(repo_path):
        yield repo


@pytest.fixture
def unsupported_project(client):
    """A client with a newer project version."""
    with client.with_metadata() as project:
        impossible_newer_version = 42000
        project.version = impossible_newer_version

    client.repo.git.add(".renku")
    client.repo.index.commit("update renku.ini", skip_hooks=True)

    yield client
