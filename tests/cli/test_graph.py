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
"""Test ``graph`` command."""

import os

import pytest

from renku.cli import cli
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from tests.utils import format_result_exception, modified_environ


@pytest.mark.parametrize("revision", [None, "HEAD", "HEAD^^", "HEAD^^..HEAD"])
@pytest.mark.parametrize("format", ["json-ld", "rdf", "nt"])
def test_graph_export_validation(runner, client, directory_tree, run, revision, format):
    """Test graph validation when exporting."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code

    file1 = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    file2 = client.path / DATA_DIR / "my-data" / directory_tree.name / "dir1" / "file2"
    assert 0 == run(["run", "head", str(file1)], stdout="out1")
    assert 0 == run(["run", "tail", str(file2)], stdout="out2")

    result = runner.invoke(cli, ["graph", "export", "--format", format, "--strict", "--revision", revision])

    assert 0 == result.exit_code, format_result_exception(result)

    assert "https://localhost" in result.output
    assert "https://renkulab.io" not in result.output

    with modified_environ(RENKU_DOMAIN="https://renkulab.io"):
        result = runner.invoke(cli, ["graph", "export", "--format", format, "--strict", "--revision", revision])

        assert 0 == result.exit_code, format_result_exception(result)

        assert "https://localhost" not in result.output
        assert "https://renkulab.io" in result.output


@pytest.mark.serial
@pytest.mark.shelled
@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_graph_export_strict_run(runner, project, run_shell, format):
    """Test graph export output of run command."""
    # Run a shell command with pipe.
    result = run_shell('renku run echo "my input string" > my_output_file')

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--strict", "--format={}".format(format)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my_output_file" in result.output
    assert "my input string" in result.output


@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_graph_export_strict_dataset(tmpdir, runner, project, client, format, subdirectory):
    """Test output of graph export for dataset add."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    paths = []
    test_paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))
        test_paths.append(os.path.relpath(str(new_file), str(project)))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--strict", f"--format={format}"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert all(p in result.output for p in test_paths), result.output
