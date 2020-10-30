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
"""Tests for Project API."""

import os

import pytest

from renku.api import Project


@pytest.mark.parametrize("sub_path", [".", "src", "src/notebooks"])
def test_get_project(client, sub_path):
    """Test getting Project context within a repository."""
    working_dir = client.path / sub_path
    working_dir.mkdir(exist_ok=True, parents=True)
    os.chdir(working_dir)

    with Project() as project:
        assert client.path == project.path


def test_get_project_multiple(client):
    """Test getting Project context multiple times within a repository."""
    with Project() as project_1:
        pass

    with Project() as project_2:
        pass

    assert project_1.path == project_2.path


def test_get_or_create_project(client):
    """Test getting Project context or creating one yileds similar results."""
    with Project() as project_1:
        pass

    project_2 = Project()

    assert project_1.path == project_2.path


def test_get_project_outside_a_renku_project(directory_tree):
    """Test creating a Project object in a non-renku directory."""
    os.chdir(directory_tree)

    with Project() as project:
        assert project.client is not None
