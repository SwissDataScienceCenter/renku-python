# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Test ``show`` command."""

from renku.cli import cli


def test_show_outputs_with_directory(runner, client, run):
    """Output files in directory are not shown as separate outputs."""
    base_sh = ["bash", "-c", 'DIR="$0"; mkdir -p "$DIR"; ' 'for x in "$@"; do touch "$DIR/$x"; done']

    assert 0 == run(args=["run"] + base_sh + ["output", "foo", "bar"])
    assert (client.path / "output" / "foo").exists()
    assert (client.path / "output" / "bar").exists()

    cmd = ["show", "outputs"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output"])
    assert 0 == result.exit_code
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output/foo"])
    assert 0 == result.exit_code
    assert {"output"} == set(result.output.strip().split("\n"))

    result = runner.invoke(cli, cmd + ["output/foo", "output/bar"])
    assert 0 == result.exit_code
    assert {"output"} == set(result.output.strip().split("\n"))


def test_show_verbose(runner, client, run):
    """Show with verbose option."""
    base_sh = ["bash", "-c", 'DIR="$0"; mkdir -p "$DIR"; ' 'for x in "$@"; do touch "$DIR/$x"; done']
    assert 0 == run(args=["run"] + base_sh + ["intermediate", "foo", "bar"])
    input_commit = client.repo.head.commit.parents[0].hexsha
    assert 0 == run(args=["run", "ls", "intermediate"], stdout="baz")
    output_commit = client.repo.head.commit.parents[0].hexsha

    workflow_partial_name = "_ls.yaml"

    result = runner.invoke(cli, ["show", "inputs", "-v"])
    assert 0 == result.exit_code
    assert input_commit in result.output
    assert workflow_partial_name in result.output
    for header in ("PATH", "COMMIT", "USAGE TIME", "WORKFLOW"):
        assert header in result.output.split("\n")[0]

    result = runner.invoke(cli, ["show", "outputs", "-v", "baz"])
    assert 0 == result.exit_code
    assert output_commit in result.output
    assert workflow_partial_name in result.output
    for header in ("PATH", "COMMIT", "GENERATION TIME", "WORKFLOW"):
        assert header in result.output.split("\n")[0]
