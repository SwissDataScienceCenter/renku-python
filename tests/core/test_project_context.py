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
"""Project context tests."""

from pathlib import Path

import pytest

from renku.core import errors
from renku.domain_model.project_context import ProjectContext


def test_use_project_context(tmpdir):
    """Test ProjectContext object."""
    project_context = ProjectContext()

    with pytest.raises(errors.ProjectContextError):
        _ = project_context.path

    with pytest.raises(IndexError):
        project_context.pop_context()

    path = Path(tmpdir.mkdir("push"))
    project_context.push_path(path)

    assert project_context.path == path

    assert project_context.pop_context().path == path

    project_context.replace_path(path)
    assert project_context.path == path

    assert project_context.pop_context().path == path

    with pytest.raises(IndexError):
        project_context.pop_context()

    new_path = Path(tmpdir.mkdir("new"))

    with project_context.with_path(new_path):
        assert project_context.path == new_path

    with pytest.raises(errors.ProjectContextError):
        _ = project_context.path


def test_database(tmpdir):
    """Test getting correct database."""
    test_dir = tmpdir.mkdir("test")
    other_test_dir = tmpdir.mkdir("other_test")

    project_context = ProjectContext()

    project_context.push_path(tmpdir)

    assert project_context.path == Path(tmpdir).resolve()
    assert project_context.database._storage.path.parent.parent == Path(tmpdir).resolve()

    project_context.push_path(str(test_dir))

    assert project_context.path == Path(test_dir).resolve()
    assert project_context.database._storage.path.parent.parent == Path(test_dir).resolve()

    project_context.push_path(Path(other_test_dir), save_changes=True)

    assert project_context.path == Path(other_test_dir).resolve()
    assert project_context.database._storage.path.parent.parent == Path(other_test_dir).resolve()

    project_context.pop_context()

    # NOTE: Make sure the database was committed on pop
    assert (Path(other_test_dir) / ".renku" / "metadata" / "root").exists()

    project_context.pop_context()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(test_dir) / ".renku" / "metadata" / "root").exists()

    project_context.pop_context()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(tmpdir) / ".renku" / "metadata" / "root").exists()

    with pytest.raises(errors.ProjectContextError):
        _ = project_context.database


def test_use_project_context_with_path():
    """Test ProjectContext.with_path leaves the state as before."""
    project_context = ProjectContext()

    before_path = Path.cwd() / "before-path"

    project_context.push_path(before_path)

    with project_context.with_path("with-path"):
        project_context.push_path("after-path-1")
        project_context.push_path("after-path-2")

    assert before_path == project_context.path


def test_use_project_context_with_path_empty():
    """Test ProjectContext.with_path leaves the state as before when no path was pushed before."""
    project_context = ProjectContext()

    with project_context.with_path("with-path"):
        project_context.push_path("after-path-1")
        project_context.push_path("after-path-2")

    with pytest.raises(errors.ProjectContextError):
        _ = project_context.path


def test_get_repository_outside_a_project(tmpdir):
    """Test accessing project's repository outside a project raises an error."""
    project_context = ProjectContext()

    with project_context.with_path(tmpdir.mkdir("project")):
        with pytest.raises(errors.ProjectContextError):
            _ = project_context.repository


def test_reuse_context(tmp_path):
    """Test that the same context can be used for multiple commands."""
    from renku.command.command_builder.command import Command
    from renku.domain_model.project_context import project_context

    def _test():
        """Dummy method for testing."""
        pass

    testdir = tmp_path / "test"
    testdir.mkdir(parents=True, exist_ok=True)

    assert not project_context.has_context()

    with project_context.with_path(testdir):
        assert project_context.has_context()

        Command().command(_test).build().execute()
        assert project_context.has_context()

        Command().command(_test).build().execute()
        assert project_context.has_context()

        Command().command(_test).build().execute()
        assert project_context.has_context()

    assert not project_context.has_context()
