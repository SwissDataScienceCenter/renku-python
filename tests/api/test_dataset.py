# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Tests for Dataset API."""

import os

import pytest

from renku.api import Dataset, Project


def test_list_datasets(client_with_datasets):
    """Test listing datasets within a project context."""
    with Project():
        datasets = Dataset.list()

        assert {"dataset-1", "dataset-2"} == {d.name for d in datasets}


def test_list_datasets_outside_a_context(client_with_datasets):
    """Test listing datasets outside a project context."""
    datasets = Dataset.list()

    assert {"dataset-1", "dataset-2"} == {d.name for d in datasets}


def test_list_datasets_outside_a_renku_project(directory_tree):
    """Test listing datasets in a non-renku directory."""
    os.chdir(directory_tree)

    assert [] == Dataset.list()


@pytest.mark.parametrize(
    "dataset, files_paths",
    [
        ("dataset-1", []),
        ("dataset-2", ["data/dataset-2/file1", "data/dataset-2/dir1/file2", "data/dataset-2/dir1/file3"]),
    ],
)
def test_list_dataset_files(client_with_datasets, dataset, files_paths):
    """Test listing datasets files."""
    with Project() as project:
        dataset = next(d for d in Dataset.list() if d.name == dataset)

        assert set(files_paths) == {d.path for d in dataset.files}
        assert set(project.path / p for p in files_paths) == {d.full_path for d in dataset.files}
