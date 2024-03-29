# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
"""Test ``move`` command."""

import os
import shutil
from pathlib import Path

import pytest

from renku.ui.cli import cli
from tests.utils import format_result_exception, get_dataset_with_injection


def test_move(runner, project):
    """Test move of files."""
    src1 = Path("src1") / "sub" / "src1.txt"
    src1.parent.mkdir(parents=True, exist_ok=True)
    src1.touch()
    src2 = Path("src2") / "sub" / "src2.txt"
    src2.parent.mkdir(parents=True, exist_ok=True)
    src2.touch()
    project.repository.add(all=True)
    project.repository.commit("Add some files")

    result = runner.invoke(cli, ["mv", "-v", "src1", "src2", "dst/sub"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not src1.exists()
    assert not src2.exists()
    dst1 = Path("dst") / "sub" / "src1" / "sub" / "src1.txt"
    assert dst1.exists()
    dst2 = Path("dst") / "sub" / "src2" / "sub" / "src2.txt"
    assert dst2.exists()
    assert f"{src1} -> {dst1}" in result.output
    assert f"{src2} -> {dst2}" in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_move_outside_paths(runner, project, directory_tree):
    """Test move from/to outside paths is not possible."""
    result = runner.invoke(cli, ["mv", str(directory_tree), "data"])

    assert 2 == result.exit_code
    assert f"Error: Invalid parameter value - Path '{directory_tree}' is outside the project" in result.output

    result = runner.invoke(cli, ["mv", "data", str(directory_tree)])

    assert 2 == result.exit_code
    assert f"Error: Invalid parameter value - Path '{directory_tree}' is outside the project" in result.output


def test_move_non_existing_sources(runner, project):
    """Test move from non-existing sources is not possible."""
    result = runner.invoke(cli, ["mv", "non-existing", "data"])

    assert 2 == result.exit_code
    assert "Path 'non-existing' does not exist" in result.output


@pytest.mark.parametrize("path", [".renku", ".renku/metadata/root", ".gitignore", "Dockerfile"])
def test_move_protected_paths(runner, project, path):
    """Test move from/to protected paths is not possible."""
    result = runner.invoke(cli, ["mv", path, "README.md"])

    assert 2 == result.exit_code, result.output
    assert f"Invalid parameter value - Path '{path}' is protected." in result.output

    result = runner.invoke(cli, ["mv", "README.md", path])

    assert 2 == result.exit_code
    assert f"Invalid parameter value - Path '{path}' is protected." in result.output


def test_move_existing_destination(runner, project):
    """Test move to existing destination."""
    (project.path / "source").write_text("123")
    project.repository.add(all=True)
    project.repository.commit("source file")

    result = runner.invoke(cli, ["mv", "source", "README.md"])

    assert 2 == result.exit_code, result.output
    assert "The following move target exist" in result.output
    assert "README.md" in result.output

    # Use ``--force``
    result = runner.invoke(cli, ["mv", "--force", "-v", "source", "README.md"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "source -> README.md" in result.output
    assert not Path("source").exists()
    assert "123" == Path("README.md").read_text()


def test_move_to_ignored_file(runner, project):
    """Test move to an ignored pattern."""
    result = runner.invoke(cli, ["mv", "README.md", "ignored.so"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "The following moved path match .gitignore" in result.output
    assert "ignored.so" in result.output


def test_move_empty_source(runner, project):
    """Test move from empty directory."""
    (project.path / "empty").mkdir()

    result = runner.invoke(cli, ["mv", "empty", "data"])

    assert 2 == result.exit_code, result.output
    assert "Invalid parameter value - There are no files to move" in result.output


def test_move_dataset_file(runner, project_with_datasets, directory_tree_files):
    """Test move of a file that belongs to a dataset."""
    for path in directory_tree_files:
        src = Path("data") / "dataset-2" / path
        assert src.exists()

    dataset_before = get_dataset_with_injection("dataset-2")

    result = runner.invoke(cli, ["mv", "data", "files"], input="y", catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Warning: You are trying to move dataset files out of a datasets data directory" in result.output

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # Check immutability
    dataset_after = get_dataset_with_injection("dataset-2")
    assert dataset_before.id != dataset_after.id
    assert dataset_before.identifier != dataset_after.identifier

    for path in directory_tree_files:
        src = Path("data") / "dataset-2" / path
        assert not src.exists()
        dst = Path("files") / "dataset-2" / path
        assert dst.exists()

        file = dataset_after.find_file(dst)
        assert not file


@pytest.mark.parametrize("args", [[], ["--to-dataset", "dataset-2"]])
def test_move_in_the_same_dataset(runner, project_with_datasets, args):
    """Test move and overwrite a file in the same dataset."""
    src = os.path.join("data", "dataset-2", "file1")
    dst = os.path.join("data", "dataset-2", "dir1", "file2")
    dataset = get_dataset_with_injection("dataset-2")
    file_before = dataset.find_file(dst)

    result = runner.invoke(cli, ["mv", "-f", src, dst] + args)
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("dataset-2")
    assert {dst, dst.replace("file2", "file3")} == {f.entity.path for f in dataset.files}
    assert not (project_with_datasets.path / src).exists()
    file_after = dataset.find_file(dst)
    assert file_after.entity.checksum != file_before.entity.checksum
    assert dst == file_after.entity.path
    assert "file1 content" == Path(dst).read_text()

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert not project_with_datasets.repository.is_dirty()


def test_move_to_existing_destination_in_a_dataset(runner, project_with_datasets):
    """Test move to a file in dataset will update file's metadata."""
    (project_with_datasets.path / "source").write_text("new-content")
    project_with_datasets.repository.add(all=True)
    project_with_datasets.repository.commit("source file")

    result = runner.invoke(cli, ["mv", "-f", "--to-dataset", "dataset-2", "source", "new_file"])
    assert 2 == result.exit_code, format_result_exception(result)
    assert "Destination new_file must be in data/dataset-2 when moving to a dataset." in result.output

    dst = os.path.join("data", "dataset-2", "file1")

    dataset_before = get_dataset_with_injection("dataset-2")
    file_before = dataset_before.find_file(dst)

    result = runner.invoke(cli, ["mv", "-f", "source", dst])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset_after = get_dataset_with_injection("dataset-2")
    file_after = dataset_after.find_file(dst)

    # Check dataset immutability
    assert dataset_after.identifier != dataset_before.identifier

    assert file_after.entity.checksum != file_before.entity.checksum
    assert dst == file_after.entity.path
    assert {"data/dataset-2/file1", "data/dataset-2/dir1/file2", "data/dataset-2/dir1/file3"} == {
        f.entity.path for f in dataset_after.files
    }

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert not project_with_datasets.repository.is_dirty()


def test_move_between_datasets(runner, project, directory_tree, large_file, directory_tree_files):
    """Test move files between datasets."""
    shutil.copy(large_file, directory_tree / "file1")
    shutil.copy(large_file, directory_tree / "dir1" / "file2")
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "dataset-1", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    file1 = Path("data") / "dataset-1" / directory_tree.name / "file1"
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "-c", "dataset-2", str(file1)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "create", "dataset-3"]).exit_code

    source = Path("data") / "dataset-1"
    destination = Path("data") / "dataset-3"

    result = runner.invoke(cli, ["mv", "-f", str(source), str(destination), "--to-dataset", "dataset-3"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert not source.exists()
    assert 0 == len(get_dataset_with_injection("dataset-1").files)
    assert 1 == len(get_dataset_with_injection("dataset-2").files)

    dataset = get_dataset_with_injection("dataset-3")
    assert 3 == len(dataset.files)

    for path in directory_tree_files:
        path = destination / directory_tree.name / path
        assert path.exists()
        assert dataset.find_file(path)

    # Some more moves and dataset operations
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "dataset-3", str(large_file)]).exit_code
    src1 = os.path.join("data", "dataset-3", directory_tree.name, "dir1")
    dst1 = os.path.join("data", "dataset-1")
    shutil.rmtree(dst1, ignore_errors=True)  # NOTE: Remove directory to force a rename
    assert 0 == runner.invoke(cli, ["mv", "-f", src1, dst1, "--to-dataset", "dataset-1"]).exit_code
    src2 = os.path.join("data", "dataset-3", directory_tree.name, "file1")
    dst2 = os.path.join("data", "dataset-2")
    (project.path / dst2).mkdir(parents=True, exist_ok=True)
    result = runner.invoke(cli, ["mv", src2, dst2, "--force", "--to-dataset", "dataset-2"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert {"data/dataset-1/file2", "data/dataset-1/file3"} == {
        f.entity.path for f in get_dataset_with_injection("dataset-1").files
    }
    assert {"data/dataset-2/file1"} == {f.entity.path for f in get_dataset_with_injection("dataset-2").files}
    assert {"data/dataset-3/large-file"} == {f.entity.path for f in get_dataset_with_injection("dataset-3").files}

    assert 0 == runner.invoke(cli, ["doctor"], catch_exceptions=False).exit_code
    assert not project.repository.is_dirty()
