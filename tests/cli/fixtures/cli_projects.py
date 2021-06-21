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

import pytest
from git import Repo


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time

    yield
    time.sleep(0.5)


@pytest.fixture
def client_with_remote(client, tmpdir):
    """Return a client with a (local) remote set."""
    # create remote
    path = str(tmpdir.mkdir("remote"))
    Repo().init(path, bare=True)

    origin = client.repo.create_remote("origin", path)
    client.repo.git.push("--set-upstream", "origin", "master")

    yield client

    client.repo.heads["master"].checkout()
    client.repo.git.branch("--unset-upstream")
    client.repo.delete_remote(origin)
    shutil.rmtree(path)


@pytest.fixture
def no_lfs_warning(client):
    """Sets show_lfs_message to False.

    For those times in life when mocking just isn't enough.
    """
    with client.commit():
        client.set_value("renku", "show_lfs_message", "False")

    yield client


@pytest.fixture
def client_with_lfs_warning(project):
    """Return a Renku repository with lfs warnings active."""
    from renku.core.management import LocalClient

    client = LocalClient(path=project)
    client.set_value("renku", "lfs_threshold", "0b")

    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")

    yield client


@pytest.fixture(params=[".", "some/sub/directory"])
def subdirectory(project, request):
    """Runs tests in root directory and a subdirectory."""
    from renku.core.utils.contexts import chdir

    if request.param != ".":
        path = Path(request.param) / ".gitkeep"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        Repo().git.add(str(path))
        Repo().index.commit("Create subdirectory", skip_hooks=True)

    with chdir(request.param):
        yield Path(request.param).resolve()
