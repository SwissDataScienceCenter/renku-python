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
"""Renku CLI fixtures for project management."""

import shutil
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Generator

import pytest

from renku.core.config import set_value
from renku.infrastructure.repository import Repository
from tests.fixtures.repository import RenkuProject


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time

    yield
    time.sleep(0.5)


@pytest.fixture
def project_with_remote(project, tmpdir) -> Generator[RenkuProject, None, None]:
    """Return a project with a (local) remote set."""
    # NOTE: Create a remote repository
    path = tmpdir.mkdir("remote")
    Repository.initialize(path, bare=True)

    project.repository.remotes.add(name="origin", url=path)
    project.repository.push("origin", "master", set_upstream=True)

    try:
        yield project
    finally:
        project.repository.checkout("master")
        project.repository.run_git_command("branch", "--unset-upstream")
        project.repository.remotes.remove("origin")
        shutil.rmtree(path)


@pytest.fixture
def no_lfs_warning(project):
    """Sets show_lfs_message to False.

    For those times in life when mocking just isn't enough.
    """
    set_value("renku", "show_lfs_message", "false")

    project.repository.add(all=True)
    project.repository.commit(message="Unset show_lfs_message")

    yield


@pytest.fixture
def project_with_lfs_warning(project):
    """Return a Renku repository with lfs warnings active."""
    from renku.domain_model.project_context import project_context

    with project_context.with_path(project.path):
        set_value("renku", "lfs_threshold", "0b")
        set_value("renku", "show_lfs_message", "true")

        project.repository.add(".renku/renku.ini")
        project.repository.commit("update renku.ini")

    yield


@pytest.fixture(params=[".", "some/sub/directory"])
def subdirectory(request) -> Generator[Path, None, None]:
    """Runs tests in root directory and a subdirectory."""
    from renku.core.util.contexts import chdir

    if request.param != ".":
        path = Path(request.param) / ".gitkeep"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

        repository = Repository()
        repository.add(path)
        repository.commit("Create subdirectory", no_verify=True)

    with chdir(request.param):
        yield Path(request.param).resolve()


@dataclass
class RenkuWorkflowFileProject(RenkuProject):
    """A Renku project with a workflow file property."""

    workflow_file: str = field(init=False)


@pytest.fixture
def workflow_file_project(project, request) -> Generator[RenkuWorkflowFileProject, None, None]:
    """Return a Renku repository with a workflow file."""
    filename = getattr(request, "param", None) or "workflow-file.yml"
    workflow_file = Path(__file__).parent / ".." / ".." / "data" / filename

    workflow_file_project = RenkuWorkflowFileProject.__new__(RenkuWorkflowFileProject)
    for f in fields(project):
        setattr(workflow_file_project, f.name, getattr(project, f.name))
    workflow_file_project.workflow_file = filename

    shutil.copy(workflow_file, project.path)

    # Create dummy input files used in the workflow file
    (project.path / "data" / "collection").mkdir(parents=True)
    (project.path / "data" / "collection" / "models.csv").write_text("\n".join(f"model-{i}" for i in range(99)))
    (project.path / "data" / "collection" / "colors.csv").write_text("\n".join(f"color-{i}" for i in range(99)))

    yield workflow_file_project
