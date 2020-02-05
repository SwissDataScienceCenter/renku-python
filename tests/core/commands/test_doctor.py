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
"""Renku doctor tests."""
from pathlib import Path

from renku.cli import cli


def test_git_hooks(runner, project):
    """Test detection of not-installed git hooks."""
    # Initially, every thing is OK
    result = runner.invoke(cli, ['doctor'])
    assert 0 == result.exit_code
    assert 'Everything seems to be ok.' in result.output

    result = runner.invoke(cli, ['githooks', 'uninstall'])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['doctor'])
    assert 1 == result.exit_code
    assert 'Git hooks are not installed.' in result.output


def test_git_hooks_modified(runner, project):
    """Test detection of modified git hooks."""
    result = runner.invoke(cli, ['githooks', 'install', '--force'])
    assert 0 == result.exit_code

    hook_path = Path(project) / '.git' / 'hooks' / 'pre-commit'
    lines = hook_path.read_text().split('/n')

    # Append some more commands
    appended = lines + ['# Some more commands', 'ls']
    hook_path.write_text('\n'.join(appended))

    # Check passes as long as Renku hook is not modified
    result = runner.invoke(cli, ['doctor'])
    assert 0 == result.exit_code
    assert 'Everything seems to be ok.' in result.output

    # Modify Renku hook
    hook_path.write_text('\n'.join(lines[:-5]))

    result = runner.invoke(cli, ['doctor'])
    assert 1 == result.exit_code
    assert 'Git hooks are outdated or not installed.' in result.output
