# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Project properties tests."""

from pathlib import Path

import pytest

from renku.core import errors
from renku.core.project.project_properties import ProjectProperties


def test_project_properties(tmpdir):
    """Test ProjectProperties object."""
    project_properties = ProjectProperties()

    with pytest.raises(errors.ConfigurationError):
        _ = project_properties.path

    with pytest.raises(IndexError):
        project_properties.pop_path()

    path = Path(tmpdir.mkdir("push"))
    project_properties.push_path(path)

    assert project_properties.path == path

    assert project_properties.pop_path().path == path

    project_properties.replace_path(path)
    assert project_properties.path == path

    assert project_properties.pop_path().path == path

    with pytest.raises(IndexError):
        project_properties.pop_path()

    new_path = Path(tmpdir.mkdir("new"))

    with project_properties.with_path(new_path):
        assert project_properties.path == new_path

    with pytest.raises(errors.ConfigurationError):
        _ = project_properties.path


def test_database(tmpdir):
    """Test getting correct database."""
    test_dir = tmpdir.mkdir("test")
    other_test_dir = tmpdir.mkdir("other_test")

    project_properties = ProjectProperties()

    project_properties.push_path(tmpdir)

    assert project_properties.path == Path(tmpdir).resolve()
    assert project_properties.database._storage.path.parent.parent == Path(tmpdir).resolve()

    project_properties.push_path(str(test_dir))

    assert project_properties.path == Path(test_dir).resolve()
    assert project_properties.database._storage.path.parent.parent == Path(test_dir).resolve()

    project_properties.push_path(Path(other_test_dir), save_changes=True)

    assert project_properties.path == Path(other_test_dir).resolve()
    assert project_properties.database._storage.path.parent.parent == Path(other_test_dir).resolve()

    project_properties.pop_path()

    # NOTE: Make sure the database was committed on pop
    assert (Path(other_test_dir) / ".renku" / "metadata" / "root").exists()

    project_properties.pop_path()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(test_dir) / ".renku" / "metadata" / "root").exists()

    project_properties.pop_path()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(tmpdir) / ".renku" / "metadata" / "root").exists()

    with pytest.raises(errors.ConfigurationError):
        _ = project_properties.database


def test_project_properties_with_path():
    """Test ProjectProperties.with_path leaves the state as before."""
    project_properties = ProjectProperties()

    before_path = Path.cwd() / "before-path"

    project_properties.push_path(before_path)

    with project_properties.with_path("with-path"):
        project_properties.push_path("after-path-1")
        project_properties.push_path("after-path-2")

    assert before_path == project_properties.path


def test_project_properties_with_path_empty():
    """Test ProjectProperties.with_path leaves the state as before when no path was pushed before."""
    project_properties = ProjectProperties()

    with project_properties.with_path("with-path"):
        project_properties.push_path("after-path-1")
        project_properties.push_path("after-path-2")

    with pytest.raises(errors.ConfigurationError):
        _ = project_properties.path
