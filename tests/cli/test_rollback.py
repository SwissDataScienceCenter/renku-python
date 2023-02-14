# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Test ``rollback`` command."""

from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_rollback(runner, project):
    """Test renku rollback."""
    result = runner.invoke(cli, ["run", "--name", "run1", "touch", "foo"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", "--name", "run2", "cp", "foo", "bar"])
    assert 0 == result.exit_code, format_result_exception(result)

    metadata_path = project.path / "input"
    metadata_path.write_text("input")

    project.repository.add("input")
    project.repository.commit("add input")

    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "foo"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "bar"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "input"])
    assert 0 == result.exit_code, format_result_exception(result)

    metadata_path.write_text("changed input")

    project.repository.add("input")
    project.repository.commit("change input")

    result = runner.invoke(cli, ["run", "--name", "run3", "cp", "input", "output"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["rollback"], input="q")
    assert 0 == result.exit_code, format_result_exception(result)
    assert "[0]" in result.output
    assert "[7]" in result.output
    assert "[8]" not in result.output

    result = runner.invoke(cli, ["rollback"], input="1\nn")
    assert 1 == result.exit_code, format_result_exception(result)
    assert "\tPlan: run3\n" in result.output
    assert "\tPlan: run2\n" not in result.output
    assert "\n\toutput\n" in result.output
    assert "\n\tbar" not in result.output
    assert "\n\tinput" in result.output
    assert "\n\toutput" in result.output

    result = runner.invoke(cli, ["rollback"], input="7\nn")
    assert 1 == result.exit_code, format_result_exception(result)
    assert "\tPlan: run1\n" in result.output
    assert "\tPlan: run2\n" in result.output
    assert "\tPlan: run3\n" in result.output
    assert "\n\tbar" in result.output
    assert "\n\tfoo" in result.output
    assert "\n\tinput" in result.output
    assert "\n\toutput" in result.output

    result = runner.invoke(cli, ["rollback"], input="7\ny")
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "ls"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 2 == len(result.output.splitlines())
    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 2 == len(result.output.splitlines())
    result = runner.invoke(cli, ["log"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert 1 == len(result.output.splitlines())
