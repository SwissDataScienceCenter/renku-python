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
from pathlib import Path
from typing import Generator

import pytest

from renku.core.config import set_value
from renku.infrastructure.repository import Repository


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time

    yield
    time.sleep(0.5)


@pytest.fixture
def project_with_remote(repository, tmpdir) -> Generator["Repository", None, None]:
    """Return a client with a (local) remote set."""
    # NOTE: Create a remote repository
    path = tmpdir.mkdir("remote")
    Repository.initialize(path, bare=True)

    repository.remotes.add(name="origin", url=path)
    repository.push("origin", "master", set_upstream=True)

    try:
        yield repository
    finally:
        repository.checkout("master")
        repository.run_git_command("branch", "--unset-upstream")
        repository.remotes.remove("origin")
        shutil.rmtree(path)


@pytest.fixture
def no_lfs_warning(repository):
    """Sets show_lfs_message to False.

    For those times in life when mocking just isn't enough.
    """
    set_value("renku", "show_lfs_message", "False")

    repository.add(all=True)
    repository.commit(message="Unset show_lfs_message")

    yield


@pytest.fixture
def client_with_lfs_warning(repository):
    """Return a Renku repository with lfs warnings active."""
    from renku.core.project.project_properties import project_properties

    with project_properties.with_path(repository.path):
        set_value("renku", "lfs_threshold", "0b")
        set_value("renku", "show_lfs_message", "True")

        repository.add(".renku/renku.ini")
        repository.commit("update renku.ini")

    yield


@pytest.fixture(params=[".", "some/sub/directory"])
def subdirectory(project, request):
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
