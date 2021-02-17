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
"""Test ``log`` command."""
import os

import git
import pytest

from renku.cli import cli


@pytest.mark.serial
@pytest.mark.shelled
@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_run_log_strict(runner, project, run_shell, format):
    """Test log output of run command."""
    # Run a shell command with pipe.
    result = run_shell('renku run echo "a" > output')

    # Assert created output file.
    result = runner.invoke(cli, ["log", "--strict", "--format={}".format(format)])
    assert 0 == result.exit_code, result.output
    assert ".renku/workflow/" in result.output


@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_dataset_log_strict(tmpdir, runner, project, client, format, subdirectory):
    """Test output of log for dataset add."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    paths = []
    test_paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))
        test_paths.append(os.path.relpath(str(new_file), str(project)))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths,)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["log", "--strict", "--format={}".format(format)])
    assert 0 == result.exit_code, result.output
    assert all(p in result.output for p in test_paths)


@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_dataset_log_invalidation_strict(tmpdir, runner, project, client, format, subdirectory):
    """Test output of log for dataset add."""
    repo = git.Repo(project)
    input_ = client.path / "input.txt"
    input_.write_text("first")

    repo.git.add("--all")
    repo.index.commit("Created input.txt")

    os.remove(input_)
    repo.git.add("--all")
    repo.index.commit("Removed input.txt")

    result = runner.invoke(cli, ["log", "--strict", "--format={}".format(format)])

    assert 0 == result.exit_code, result.output
    assert "wasInvalidatedBy" in result.output
