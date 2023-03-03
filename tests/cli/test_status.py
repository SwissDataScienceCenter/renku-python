#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Test ``status`` command."""

import os

from renku.ui.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


def test_status_exit_codes(runner, project, subdirectory):
    """Test status check returns 0 when up-to-date and 1 otherwise."""
    source = os.path.join(project.path, "source.txt")
    output = os.path.join(project.path, "data", "output.txt")

    write_and_commit_file(project.repository, source, "content")

    result = runner.invoke(cli, ["run", "cp", source, output])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", "cat", "--no-output", source])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])

    assert 0 == result.exit_code, format_result_exception(result)

    write_and_commit_file(project.repository, source, "new content")

    result = runner.invoke(cli, ["status"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "Outdated outputs(1):" in result.output
    assert f"{os.path.relpath(output)}: {os.path.relpath(source)}" in result.output
    assert "Modified inputs(1):" in result.output
    assert "Outdated activities that have no outputs(1)" in result.output
    assert "/activities/" in result.output
