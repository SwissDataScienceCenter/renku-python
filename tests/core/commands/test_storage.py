# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Storage command tests."""

import os
import subprocess
from pathlib import Path

from renku.cli import cli


def test_lfs_storage_clean_no_remote(runner, project, client):
    """Test ``renku storage clean`` command with no remote set."""
    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")
    client.repo.git.add("*")
    client.repo.index.commit("tracked file")

    result = runner.invoke(cli, ["storage", "clean", "tracked"], catch_exceptions=False)
    assert 1 == result.exit_code
    assert "No git remote is configured for" in result.output


def test_lfs_storage_clean(runner, project, client_with_remote):
    """Test ``renku storage clean`` command."""
    client = client_with_remote["client"]

    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")
    client.repo.git.add("*")
    client.repo.index.commit("tracked file")

    result = runner.invoke(cli, ["storage", "clean", "tracked"], catch_exceptions=False)
    assert 0 == result.exit_code
    assert "These paths were ignored as they are not tracked" in result.output

    subprocess.call(["git", "lfs", "track", "tracked"])
    client.repo.git.add("*")
    client.repo.index.commit("Tracked in lfs")
    client.repo.git.push("--no-verify", "origin")

    with (client.path / "tracked").open("r") as fp:
        assert "tracked file" in fp.read()

    assert "tracked" in Path(".gitattributes").read_text()

    lfs_objects = []
    for _, _, files in os.walk(str(client.path / ".git" / "lfs" / "objects")):
        lfs_objects.extend(files)

    assert 1 == len(lfs_objects)

    result = runner.invoke(cli, ["storage", "clean", "tracked"], catch_exceptions=False)
    assert 0 == result.exit_code

    assert "version https://git-lfs.github.com/spec/v1" in (client.path / "tracked").read_text()

    lfs_objects = []
    for _, _, files in os.walk(str(client.path / ".git" / "lfs" / "objects")):
        lfs_objects.extend(files)

    assert 0 == len(lfs_objects)

    # already clean file should be ignored on clean
    result = runner.invoke(cli, ["storage", "clean", "tracked"], catch_exceptions=False)
    assert 0 == result.exit_code


def test_lfs_storage_unpushed_clean(runner, project, client_with_remote):
    """Test ``renku storage clean`` command for unpushed files."""
    client = client_with_remote["client"]

    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")
    subprocess.call(["git", "lfs", "track", "tracked"])
    client.repo.git.add("*")
    client.repo.index.commit("tracked file")

    result = runner.invoke(cli, ["storage", "clean", "tracked"], catch_exceptions=False)

    assert 0 == result.exit_code
    assert "These paths were ignored as they are not pushed" in result.output


def test_lfs_migrate(runner, project, client):
    """Test ``renku storage migrate`` command for large files in git."""

    for _file in ["dataset_file", "workflow_file", "regular_file"]:
        (client.path / _file).write_text(_file)

    client.repo.git.add("*")
    client.repo.index.commit("add files")
    dataset_checksum = client.repo.head.commit.tree["dataset_file"].hexsha
    workflow_checksum = client.repo.head.commit.tree["workflow_file"].hexsha

    result = runner.invoke(cli, ["graph", "generate"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "add", "-c", "my_dataset", "dataset_file"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["run", "cp", "workflow_file", "output_file"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "0b"])
    assert 0 == result.exit_code

    previous_head = client.repo.head.commit.hexsha

    result = runner.invoke(cli, ["storage", "migrate", "--all"], input="y")
    assert 0 == result.exit_code
    assert "dataset_file" in result.output
    assert "workflow_file" in result.output
    assert "regular_file" in result.output
    assert "*.ini" not in result.output

    assert previous_head != client.repo.head.commit.hexsha
    changed_files = client.repo.head.commit.stats.files.keys()
    assert ".renku/dataset.json" in changed_files
    assert ".renku/provenance.json" in changed_files

    assert dataset_checksum not in (client.path / ".renku" / "dataset.json").read_text()

    assert workflow_checksum not in (client.path / ".renku" / "provenance.json").read_text()


def test_lfs_migrate_no_changes(runner, project, client):
    """Test ``renku storage migrate`` command without broken files."""

    for _file in ["dataset_file", "workflow_file", "regular_file"]:
        (client.path / _file).write_text(_file)

    client.repo.git.add("*")
    client.repo.index.commit("add files")

    result = runner.invoke(cli, ["graph", "generate"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "add", "-c", "my_dataset", "dataset_file"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["run", "cp", "workflow_file", "output_file"])
    assert 0 == result.exit_code

    previous_head = client.repo.head.commit.hexsha

    result = runner.invoke(cli, ["storage", "migrate", "--all"], input="y")
    assert 0 == result.exit_code
    assert "All files are already in LFS" in result.output

    assert previous_head == client.repo.head.commit.hexsha


def test_lfs_migrate_explicit_path(runner, project, client):
    """Test ``renku storage migrate`` command explicit path."""

    for _file in ["dataset_file", "workflow_file", "regular_file"]:
        (client.path / _file).write_text(_file)

    client.repo.git.add("*")
    client.repo.index.commit("add files")
    dataset_checksum = client.repo.head.commit.tree["dataset_file"].hexsha
    workflow_checksum = client.repo.head.commit.tree["workflow_file"].hexsha

    result = runner.invoke(cli, ["graph", "generate"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "add", "-c", "my_dataset", "dataset_file"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["run", "cp", "workflow_file", "output_file"])
    assert 0 == result.exit_code

    previous_head = client.repo.head.commit.hexsha

    result = runner.invoke(cli, ["storage", "migrate", "regular_file"])
    assert 0 == result.exit_code

    assert previous_head != client.repo.head.commit.hexsha

    assert dataset_checksum in (client.path / ".renku" / "dataset.json").read_text()

    assert workflow_checksum in (client.path / ".renku" / "provenance.json").read_text()

    assert "oid sha256:" in (client.path / "regular_file").read_text()
