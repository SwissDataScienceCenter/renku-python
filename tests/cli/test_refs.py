# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
"""Test references created using CLI."""

from renku import cli


def test_workflow_naming(runner, client):
    """Test naming of CWL tools and workflows."""
    result = runner.invoke(cli.cli, ['run', 'touch', 'data.txt'])
    assert result.exit_code == 0

    cmd = ['workflow', 'set-name', '.invalid']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code != 0

    cmd = ['workflow', 'set-name', 'first']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    tools = list(client.workflow_path.glob('*.cwl'))
    assert 1 == len(tools)

    cmd = ['workflow', 'set-name', 'group/second', str(tools[0])]
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    #: Show all CWL files with aliases.
    result = runner.invoke(cli.cli, ['workflow'])
    assert result.exit_code == 0
    assert 'first' in result.output
    assert 'group/second' in result.output

    #: Rename an alias and verify in output.
    result = runner.invoke(cli.cli, ['workflow', 'rename', 'first', 'third'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['workflow'])
    assert 'first' not in result.output
    assert 'third' in result.output

    #: Create/Override alias with the same name.
    result = runner.invoke(cli.cli, ['run', 'touch', 'output.txt'])
    assert result.exit_code == 0

    cmd = ['workflow', 'set-name', 'group/second']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code != 0

    cmd = ['workflow', 'set-name', 'group/second', '--force']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code == 0

    result = runner.invoke(
        cli.cli, ['workflow', 'rename', 'group/second', 'third']
    )
    assert result.exit_code != 0

    result = runner.invoke(
        cli.cli, ['workflow', 'rename', 'group/second', 'third', '--force']
    )
    assert result.exit_code == 0

    #: Remove an alias and verify in output.
    result = runner.invoke(cli.cli, ['workflow', 'remove', 'third'])
    assert result.exit_code == 0

    result = runner.invoke(cli.cli, ['workflow'])
    assert 'group/second' not in result.output
    assert 'third' not in result.output

    #: Last commit was not workflow run, rerun or update
    cmd = ['workflow', 'set-name', 'unknown_tool']
    result = runner.invoke(cli.cli, cmd)
    assert result.exit_code != 0
