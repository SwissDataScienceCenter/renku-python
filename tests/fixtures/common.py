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
"""Renku common fixtures."""

import os
from pathlib import Path
from typing import List

import pytest


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
def data_repository(directory_tree):
    """Create a test repository."""
    # NOTE: Initialize step.
    from renku.infrastructure.repository import Actor, Repository

    repository = Repository.initialize(directory_tree)

    # NOTE: Add a file step.
    repository.add(directory_tree / "file1")
    repository.commit("test commit", author=Actor("me", "me@example.com"))

    # NOTE: Commit changes to the same file with a different user.
    directory_tree.joinpath("file1").write_text("5678")
    repository.add(directory_tree / "file1")
    repository.commit("test commit", author=Actor("me2", "me2@example.com"))

    # NOTE: Commit a second file.
    repository.add(directory_tree / "dir1" / "file2")
    repository.commit("test commit", author=Actor("me", "me@example.com"))

    # NOTE: Return the repo.
    return repository


@pytest.fixture
def no_lfs_size_limit(client):
    """Configure environment track all files in LFS independent of size."""
    client.set_value("renku", "lfs_threshold", "0b")
    client.repository.add(".renku/renku.ini")
    client.repository.commit("update renku.ini")

    yield client


@pytest.fixture
def large_file(tmp_path, client):
    """A file larger than the minimum LFS file size."""
    path = tmp_path / "large-file"
    with open(path, "w") as file_:
        file_.seek(client.minimum_lfs_file_size)
        file_.write("some data")

    yield path
