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
"""Renku doctor tests."""
from pathlib import Path

from renku.cli import cli


def test_new_project_is_ok(runner, project):
    """Test renku doctor initially is OK on a new project."""
    # Initially, every thing is OK
    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code
    assert "Everything seems to be ok." in result.output


def test_git_hooks_not_available(runner, project):
    """Test detection of not-installed git hooks."""
    result = runner.invoke(cli, ["githooks", "uninstall"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "Git hooks are not installed." in result.output


def test_git_hooks_modified(runner, project):
    """Test detection of modified git hooks."""
    result = runner.invoke(cli, ["githooks", "install", "--force"])
    assert 0 == result.exit_code

    hook_path = Path(project) / ".git" / "hooks" / "pre-commit"
    lines = hook_path.read_text().split("\n")

    # Append some more commands
    appended = lines + ["# Some more commands", "ls"]
    hook_path.write_text("\n".join(appended))

    # Check passes as long as Renku hook is not modified
    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code
    assert "Everything seems to be ok." in result.output

    # Modify Renku hook
    modified = [line for line in lines if "# END RENKU HOOK." not in line]
    hook_path.write_text("\n".join(modified))

    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "Git hooks are outdated or not installed." in result.output


def test_lfs_broken_history(runner, client, tmp_path):
    """Test lfs migrate info check on a broken history."""
    big_file = tmp_path / "big-file.bin"
    with open(big_file, "w") as file_:
        file_.seek(client.minimum_lfs_file_size)
        file_.write("some-data")

    # Add a file without adding it to LFS
    result = runner.invoke(
        cli,
        ["--no-external-storage", "dataset", "add", "--create", "new-dataset", str(big_file)],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "Git history contains large files" in result.output
    assert "*.bin" in result.output

    # Exclude *.ipynb files from LFS in .renkulfsignore
    (client.path / client.RENKU_LFS_IGNORE_PATH).write_text("\n".join(["*swp", "*.bin", ".DS_Store"]))

    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code
    assert "Git history contains large files" not in result.output
