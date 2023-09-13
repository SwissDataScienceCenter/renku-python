#
# Copyright 2018-2023 - Swiss Data Science Center (SDSC)
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

from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_activity_log(runner, project):
    """Test renku log for activities."""
    result = runner.invoke(cli, ["run", "--name", "run1", "touch", "foo"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["log"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Activity /activities/" in result.output
    assert "Command: touch foo" in result.output
    assert "output-1: foo" in result.output
    assert "Start Time:" in result.output
    assert "Renku Version:" in result.output

    result = runner.invoke(cli, ["run", "--name", "run2", "cp", "foo", "bar"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["log"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Activity /activities/" in result.output
    assert "Plan:" in result.output
    assert "Id: /plans/" in result.output
    assert "Name: run1" in result.output
    assert "Command: touch foo" in result.output
    assert "output-1: foo" in result.output
    assert "Start Time:" in result.output
    assert "Renku Version:" in result.output
    assert "Command: cp foo bar" in result.output
    assert "input-1: foo" in result.output
    assert "output-2: bar" in result.output


def test_dataset_log(runner, project):
    """Test renku log for dataset."""
    result = runner.invoke(cli, ["dataset", "create", "test-set"])
    assert 0 == result.exit_code, format_result_exception(result)

    with (project.path / "my_file").open("w") as fp:
        fp.write("dataset file")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "test-set", "my_file"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(
        cli, ["dataset", "edit", "test-set", "-n", "new name", "-d", "new description", "-k", "a", "-k", "b"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["dataset", "unlink", "test-set", "--include", "my_file"], input="y")
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["dataset", "rm", "test-set"], input="y")
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["log"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Dataset test-set" in result.output
    assert "Changes: created" in result.output
    assert "Changes: modified" in result.output
    assert "Changes: deleted" in result.output
    assert "Files modified" in result.output
    assert "- data/test-set/my_file" in result.output
    assert "+ data/test-set/my_file" in result.output
    assert "Name set to: new name" in result.output
    assert "Description set to: new description" in result.output
    assert "Keywords modified" in result.output
    assert "Creators modified" in result.output
