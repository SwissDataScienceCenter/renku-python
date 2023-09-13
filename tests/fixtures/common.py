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
"""Renku common fixtures."""

import os
from pathlib import Path
from typing import Generator, List

import pytest

from renku.core.config import set_value
from renku.core.lfs import get_minimum_lfs_file_size


@pytest.fixture
def directory_tree_files() -> List[str]:
    """List of files for ``directory_tree`` fixture."""
    return ["file1", os.path.join("dir1", "file2"), os.path.join("dir1", "file3")]


@pytest.fixture()
def directory_tree(tmp_path, directory_tree_files) -> Path:
    """Create a test directory tree."""
    # initialize
    base = tmp_path / "directory_tree"
    for path in directory_tree_files:
        path = base / path
        path.parent.mkdir(parents=True, exist_ok=True)

        if str(path).endswith("file1"):
            path.write_text("file1 content")
        elif str(path).endswith("file2"):
            path.write_text("file2 content")
        elif str(path).endswith("file3"):
            path.write_text("file3 content")

    return base


@pytest.fixture
def no_lfs_size_limit(project):
    """Configure environment to track all files in LFS independent of size."""
    set_value("renku", "lfs_threshold", "0b")
    project.repository.add(".renku/renku.ini")
    project.repository.commit("update renku.ini")

    yield


@pytest.fixture
def no_datadir_commit_warning(project):
    """Configure pre-commit hook to ignore files added to a datasets data directory."""
    set_value("renku", "check_datadir_files", "false")
    project.repository.add(".renku/renku.ini")
    project.repository.commit("update renku.ini")

    yield


@pytest.fixture
def large_file(tmp_path):
    """A file larger than the minimum LFS file size."""
    path = tmp_path / "large-file"
    with open(path, "w") as file:
        file.seek(get_minimum_lfs_file_size())
        file.write("some data")

    yield path


@pytest.fixture
def transaction_id(project) -> Generator[str, None, None]:
    """Return current transaction ID."""
    from renku.domain_model.project_context import project_context

    yield project_context.transaction_id
