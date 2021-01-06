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
"""Test ``graph`` command."""

import os

import pytest

from renku.cli import cli
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR


@pytest.mark.parametrize("format", ["json-ld", "jsonld"])
def test_graph_export_validation(runner, client, directory_tree, run, format):
    """Test graph validation when exporting."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code
    file1 = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = client.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export", "--format", format, "--strict"])

    assert 0 == result.exit_code, result.output


def test_graph_export(runner, client, directory_tree, run):
    """Test graph export."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code
    file1 = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = client.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export"])

    assert "file1" in result.output
    assert "out1" in result.output
    assert "file2" in result.output
    assert "out2" in result.output


def test_graph_export_with_file_paths(runner, client, directory_tree, run):
    """Test graph export with path."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code
    file1 = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = client.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export", "out2"])

    assert "file1" not in result.output
    assert "out1" not in result.output
    assert "file2" in result.output
    assert "out2" in result.output


def test_graph_export_with_directory_paths(runner, client):
    """Test graph export with directory path."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code
    a_script = ("sh", "-c", f"mkdir -p {DATA_DIR}/$0/dir1; touch {DATA_DIR}/$0/dir1/file1")
    assert 0 == runner.invoke(cli, ["run", *a_script, "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["run", *a_script, "my-new-data"]).exit_code

    # File inside a directory
    file1 = os.path.join(DATA_DIR, "my-data", "dir1", "file1")
    result = runner.invoke(cli, ["graph", "export", file1])

    assert "my-data" in result.output
    assert "my-new-data" not in result.output

    # A directory that includes generated files
    result = runner.invoke(cli, ["graph", "export", "data"])

    assert "my-data" not in result.output
    assert "my-new-data" in result.output


def test_graph_export_with_revision(runner, client, directory_tree, run):
    """Test graph export."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code
    file1 = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = client.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    commit_sha = client.repo.head.object.hexsha
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export", "--workflows-only", "--revision", commit_sha])

    assert "file1" in result.output
    assert "out1" in result.output
    assert "file2" not in result.output
    assert "out2" not in result.output
