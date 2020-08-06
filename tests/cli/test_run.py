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
"""Test ``run`` command."""
from __future__ import absolute_import, print_function

import os

import pytest

from renku.cli import cli


def test_run_simple(runner, project):
    """Test tracking of run command."""
    cmd = ["echo", "test"]

    result = runner.invoke(cli, ["run", "--no-output"] + cmd)
    assert 0 == result.exit_code

    # There are no output files.
    result = runner.invoke(cli, ["log"])
    assert 0 == result.exit_code
    assert 1 == len(result.output.strip().split("\n"))

    # Display tools with no outputs.
    result = runner.invoke(cli, ["log", "--no-output"])
    assert 0 == result.exit_code
    assert ".renku/workflow/" in result.output


def test_run_many_args(client, run):
    """Test a renku run command which implicitly relies on many inputs."""
    os.mkdir("files")
    output = "output.txt"
    for i in range(103):
        os.system("touch files/{}.txt".format(i))
    client.repo.index.add(["files/"])
    client.repo.index.commit("add many files")

    exit_code = run(args=("run", "ls", "files/"), stdout=output)
    assert 0 == exit_code


@pytest.mark.shelled
def test_run_clean(runner, project, run_shell):
    """Test tracking of run command in clean repo."""
    # Run a shell command with pipe.
    output = run_shell('renku run echo "a" > output')

    # Assert expected empty stdout.
    assert b"" == output[0]
    # Assert not allocated stderr.
    assert output[1] is None

    # Assert created output file.
    result = runner.invoke(cli, ["log"])
    assert "output" in result.output
    assert ".yaml" in result.output
    assert ".renku/workflow/" in result.output
