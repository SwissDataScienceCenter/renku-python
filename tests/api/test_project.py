# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

from renku.domain_model.project_context import project_context
from renku.ui.api import Project
from renku.ui.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


@pytest.mark.parametrize("sub_path", [".", "src", "src/notebooks"])
def test_get_project(project, sub_path):
    """Test getting Project context within a repository."""
    working_dir = project_context.path / sub_path
    working_dir.mkdir(exist_ok=True, parents=True)
    os.chdir(working_dir)

    with Project() as project:
        assert project_context.path == project.path


def test_get_project_multiple(project):
    """Test getting Project context multiple times within a repository."""
    with Project() as project_1:
        pass

    with Project() as project_2:
        pass

    assert project_1.path == project_2.path


def test_get_or_create_project(project):
    """Test getting Project context or creating one yields similar results."""
    with Project() as project_1:
        pass

    project_2 = Project()

    assert project_1.path == project_2.path


def test_get_project_outside_a_renku_project(directory_tree):
    """Test creating a Project object in a non-renku directory."""
    os.chdir(directory_tree)

    with Project() as project:
        assert project.repository is None


def test_status(runner, project):
    """Test status check."""
    source = project_context.path / "source.txt"
    output = project_context.path / "data" / "output.txt"

    repository = project_context.repository

    write_and_commit_file(repository, source, "content")

    result = runner.invoke(cli, ["run", "cp", source, output])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", "cat", "--no-output", source])
    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(repository, source, "new content")

    result = Project().status()

    assert (os.path.relpath(output), {os.path.relpath(source)}) in result.outdated_outputs.items()
    assert "source.txt" in result.modified_inputs
    assert 1 == len(result.outdated_activities)
    assert "/activities/" in list(result.outdated_activities.keys())[0]
    assert 0 == len(result.deleted_inputs)
