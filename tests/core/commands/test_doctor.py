# -*- coding: utf-8 -*-
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
"""Renku doctor tests."""
from pathlib import Path

from renku.domain_model.dataset import Url
from renku.ui.cli import cli
from tests.utils import format_result_exception, with_dataset


def test_new_project_is_ok(runner, project):
    """Test renku doctor initially is OK on a new project."""
    # Initially, every thing is OK
    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Everything seems to be ok." in result.output


def test_git_hooks_not_available(runner, project):
    """Test detection of not-installed git hooks."""
    result = runner.invoke(cli, ["githooks", "uninstall"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "Git hooks are not installed." in result.output


def test_git_hooks_modified(runner, project):
    """Test detection of modified git hooks."""
    result = runner.invoke(cli, ["githooks", "install", "--force"])
    assert 0 == result.exit_code, format_result_exception(result)

    hook_path = Path(project) / ".git" / "hooks" / "pre-commit"
    lines = hook_path.read_text().split("\n")

    # Append some more commands
    appended = lines + ["# Some more commands", "ls"]
    hook_path.write_text("\n".join(appended))

    # Check passes as long as Renku hook is not modified
    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)
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
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "Git history contains large files" in result.output
    assert "*.bin" in result.output

    # Exclude *.ipynb files from LFS in .renkulfsignore
    (client.path / client.RENKU_LFS_IGNORE_PATH).write_text("\n".join(["*swp", "*.bin", ".DS_Store"]))

    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Git history contains large files" not in result.output


def test_check_invalid_imported_dataset(runner, client_with_datasets, client_database_injection_manager):
    """Test checking imported datasets that have both derived_from and same_as set."""
    with client_database_injection_manager(client_with_datasets):
        with with_dataset(client_with_datasets, name="dataset-1", commit_database=True) as dataset:
            # NOTE: Set both same_as and derived_from for a dataset
            dataset.same_as = Url(url_str="http://example.com")
            dataset.derived_from = Url(url_id="datasets/non-existing-id")

    result = runner.invoke(cli, ["doctor"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "There are invalid dataset metadata in the project" in result.output
    assert "dataset-1" in result.output

    result = runner.invoke(cli, ["graph", "export", "--full"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["log", "--datasets"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_fix_invalid_imported_dataset(runner, client_with_datasets, client_database_injection_manager):
    """Test fixing imported datasets that have both derived_from and same_as set."""
    with client_database_injection_manager(client_with_datasets):
        with with_dataset(client_with_datasets, name="dataset-1", commit_database=True) as dataset:
            # NOTE: Set both same_as and derived_from for a dataset
            dataset.same_as = Url(url_str="http://example.com")
            dataset.derived_from = Url(url_id="datasets/non-existing-id")

    client_with_datasets.repository.add(all=True)
    client_with_datasets.repository.commit("modified dataset")

    before_commit_sha = client_with_datasets.repository.head.commit.hexsha

    result = runner.invoke(cli, ["doctor", "--fix"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Fixing dataset 'dataset-1'" in result.output

    assert before_commit_sha != client_with_datasets.repository.head.commit.hexsha
    assert not client_with_datasets.repository.is_dirty(untracked_files=True)

    with client_database_injection_manager(client_with_datasets):
        with with_dataset(client_with_datasets, name="dataset-1") as dataset:
            # NOTE: Set both same_as and derived_from for a dataset
            assert dataset.same_as.value == "http://example.com"
            assert dataset.derived_from is None
