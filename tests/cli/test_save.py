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
"""Test ``save`` command."""

import os

from git import Repo

from renku.cli import cli


def test_save_without_remote(runner, project, client, tmpdir_factory):
    """Test saving local changes."""
    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")

    result = runner.invoke(cli, ["save", "-m", "save changes", "tracked"], catch_exceptions=False)
    assert 1 == result.exit_code
    assert "No remote has been set up" in result.output

    path = str(tmpdir_factory.mktemp("remote"))
    Repo().init(path, bare=True)

    result = runner.invoke(cli, ["save", "-d", path, "tracked"], catch_exceptions=False)

    assert 0 == result.exit_code
    assert "tracked" in result.output
    assert "Saved changes to: tracked" in client.repo.head.commit.message

    client.repo.delete_remote("origin")


def test_save_with_remote(runner, project, client_with_remote, tmpdir_factory):
    """Test saving local changes."""
    client = client_with_remote["client"]
    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")

    result = runner.invoke(cli, ["save", "-m", "save changes", "tracked"], catch_exceptions=False)

    assert 0 == result.exit_code
    assert "tracked" in result.output
    assert "save changes" in client.repo.head.commit.message


def test_save_with_staged(runner, project, client_with_remote, tmpdir_factory):
    """Test saving local changes."""
    client = client_with_remote["client"]
    with (client.path / "deleted").open("w") as fp:
        fp.write("deleted file")

    client.repo.index.add("deleted")
    client.repo.index.commit("add file for later deletion")

    os.remove(client.path / "deleted")

    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")

    with (client.path / "modified").open("w") as fp:
        fp.write("modified file")

    client.repo.git.add("tracked")
    client.repo.git.add("deleted")

    result = runner.invoke(cli, ["save", "-m", "save changes", "modified", "deleted"], catch_exceptions=False)

    assert 1 == result.exit_code
    assert "These files are in the git staging area, but " in result.output
    assert "tracked" in result.output
    assert "tracked" in [f.a_path for f in client.repo.index.diff("HEAD")]
    assert "modified" in client.repo.untracked_files

    result = runner.invoke(
        cli, ["save", "-m", "save changes", "tracked", "modified", "deleted"], catch_exceptions=False
    )

    assert 0 == result.exit_code
    assert {"tracked", "modified", "deleted"} == {f.a_path for f in client.repo.head.commit.diff("HEAD~1")}
