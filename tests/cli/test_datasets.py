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
"""Test ``dataset`` command."""

import json
import os
import shutil
import textwrap
import time
from pathlib import Path
from unittest.mock import call

import pytest

from renku.command.format.dataset_files import DATASET_FILES_COLUMNS, DATASET_FILES_FORMATS
from renku.command.format.datasets import DATASETS_COLUMNS, DATASETS_FORMATS
from renku.core import errors
from renku.core.config import set_value
from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.constant import REFS, RENKU_HOME
from renku.core.dataset.providers.dataverse import DataverseProvider
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.zenodo import ZenodoProvider
from renku.core.interface.storage import FileHash
from renku.core.lfs import track_paths_in_storage
from renku.core.util.git import get_dirty_paths
from renku.core.util.urls import get_slug
from renku.domain_model.dataset import Dataset
from renku.ui.cli import cli
from tests.utils import (
    assert_dataset_is_mutated,
    format_result_exception,
    get_dataset_with_injection,
    get_datasets_provenance_with_injection,
    write_and_commit_file,
)


def test_datasets_create_clean(runner, project):
    """Test creating a dataset in clean repository."""
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = get_dataset_with_injection("dataset")
    assert isinstance(dataset, Dataset)
    assert Path("data/dataset/") == dataset.get_datadir()

    assert not project.repository.is_dirty()


def test_datasets_create_clean_with_datadir(runner, project):
    """Test creating a dataset in clean repository."""

    datadir = Path("my/data/dir")

    result = runner.invoke(cli, ["dataset", "create", "--datadir", datadir, "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = get_dataset_with_injection("dataset")
    assert isinstance(dataset, Dataset)
    assert datadir == dataset.get_datadir()

    assert not project.repository.is_dirty()


def test_datasets_create_with_datadir_with_files(runner, project):
    """Test creating a dataset in clean repository."""

    datadir = Path("my/data/dir")
    datadir.mkdir(parents=True, exist_ok=True)

    file = datadir / "my_file"
    file.write_text("content")

    result = runner.invoke(cli, ["dataset", "create", "--datadir", datadir, "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = get_dataset_with_injection("dataset")
    assert isinstance(dataset, Dataset)
    assert datadir == dataset.get_datadir()
    assert dataset.find_file(file)

    assert not project.repository.is_dirty()


def test_datasets_create_dirty(runner, project):
    """Test creating a dataset in a dirty repository."""
    (project.path / "untracked").write_text("untracked")
    (project.path / "staged").write_text("staged")
    project.repository.add("staged")

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("dataset")
    assert dataset

    # All staged files will be committed
    assert 0 == len(project.repository.staged_changes)

    # Untracked files won't be committed
    assert {"untracked"} == set(project.repository.untracked_files)


@pytest.mark.parametrize(
    "datadir_option,datadir", [([], f"{DATA_DIR}/my-dataset"), (["--datadir", "my-dir"], "my-dir")]
)
def test_dataset_show(runner, project, subdirectory, datadir_option, datadir):
    """Test creating and showing a dataset with metadata."""
    result = runner.invoke(cli, ["dataset", "show", "my-dataset"])
    assert 1 == result.exit_code, format_result_exception(result)
    assert "Dataset 'my-dataset' is not found." in result.output

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    result = runner.invoke(
        cli,
        [
            "dataset",
            "create",
            "my-dataset",
            "--title",
            "Long Title",
            "--description",
            "# t1\n## t2\nsome description here",
            "-c",
            "John Doe <john.doe@mail.ch>",
            "-c",
            "John Smiths<john.smiths@mail.ch>",
            "-k",
            "keyword-1",
            "-k",
            "keyword-2",
            "--metadata",
            str(metadata_path),
        ]
        + datadir_option,
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "show", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "some description here" in result.output
    assert "Long Title" in result.output
    assert "keyword-1" in result.output
    assert "keyword-2" in result.output
    assert "Created: " in result.output
    assert "Name: my-dataset" in result.output
    assert "John Doe <john.doe@mail.ch>" in result.output
    assert "some_unique_value" in result.output
    assert "https://schema.org/specialProperty" in result.output
    assert "https://example.com/annotation1" in result.output
    assert "https://schema.org/specialType" in result.output
    assert "##" not in result.output
    assert datadir in result.output
    assert "Data Directory:"


def test_dataset_show_tag(runner, project, subdirectory):
    """Test creating and showing a dataset with metadata."""
    result = runner.invoke(cli, ["dataset", "show", "my-dataset"])
    assert 1 == result.exit_code, format_result_exception(result)
    assert "Dataset 'my-dataset' is not found." in result.output

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    result = runner.invoke(
        cli,
        [
            "dataset",
            "create",
            "my-dataset",
            "--title",
            "Long Title",
            "--description",
            "description1",
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "tag1"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "edit", "-d", "description2", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: description" in result.output

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "tag2"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "edit", "-d", "description3", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: description" in result.output

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "tag3"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "show", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "description3" in result.output
    assert "description2" not in result.output
    assert "description1" not in result.output

    result = runner.invoke(cli, ["dataset", "show", "--tag", "tag3", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "description3" in result.output
    assert "description2" not in result.output
    assert "description1" not in result.output

    result = runner.invoke(cli, ["dataset", "show", "--tag", "tag2", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "description2" in result.output
    assert "description3" not in result.output
    assert "description1" not in result.output

    result = runner.invoke(cli, ["dataset", "show", "--tag", "tag1", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "description1" in result.output
    assert "description2" not in result.output
    assert "description3" not in result.output


def test_datasets_create_different_names(runner, project):
    """Test creating datasets with same title but different name."""
    result = runner.invoke(cli, ["dataset", "create", "dataset-1", "--title", "title"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "dataset-2", "--title", "title"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output


def test_datasets_create_with_same_name(runner, project):
    """Test creating datasets with same name."""
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert "Dataset exists: 'dataset'" in result.output


@pytest.mark.parametrize(
    "name",
    [
        "any name /@#$!",
        "name longer than 24 characters",
        "semi valid-name",
        "dataset/new",
        "/dataset",
        "dataset/",
        "name ends in.lock",
    ],
)
def test_datasets_invalid_name(runner, project, name):
    """Test creating datasets with invalid name."""
    result = runner.invoke(cli, ["dataset", "create", name])

    assert 2 == result.exit_code
    assert f"Dataset name '{name}' is not valid" in result.output
    assert f"Hint: '{get_slug(name)}' is valid" in result.output


def test_datasets_create_dirty_exception_untracked(runner, project):
    """Test exception raise for untracked file in renku directory."""
    # 1. Create a problem.
    datasets_dir = project.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 2. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_datasets_create_dirty_exception_staged(runner, project):
    """Test exception raise for staged file in renku directory."""
    # 1. Create a problem within .renku directory
    datasets_dir = project.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 2. Stage a problem without committing it.
    project.repository.add(datasets_dir / "a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_dataset_create_dirty_exception_all_untracked(runner, project):
    """Test exception raise for all untracked files."""
    # 1. Create unclean root to enforce ensure checks.
    with (project.path / "a").open("w") as fp:
        fp.write("a")

    # 2. Create a problem.
    datasets_dir = project.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_datasets_create_dirty_exception_all_staged(runner, project):
    """Test exception raise for all staged files."""
    # 1. Create unclean root to enforce ensure checks.
    with (project.path / "a").open("w") as fp:
        fp.write("a")

    project.repository.add("a")

    # 2. Create a problem.
    datasets_dir = project.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    project.repository.add(datasets_dir / "a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_dataset_create_exception_refs(runner, project):
    """Test untracked/unstaged exception raise in dirty renku home dir."""
    with (project.path / "a").open("w") as fp:
        fp.write("a")

    datasets_dir = project.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    refs_dir = project.path / RENKU_HOME / REFS
    if not refs_dir.exists():
        refs_dir.mkdir()

    with (refs_dir / "b").open("w") as fp:
        fp.write("b")

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert "a" in result.output


@pytest.mark.parametrize(
    "creator,field",
    [
        ("John Doe", "Email"),
        ("John Doe<>", "Email"),
        ("<john.doe@mail.ch>", "Name"),
        ("John Doe<john.doe@mail>", "Email"),
    ],
)
def test_dataset_creator_is_invalid(runner, project, creator, field):
    """Test create dataset with invalid creator format."""
    result = runner.invoke(cli, ["dataset", "create", "ds", "-c", creator])
    assert 2 == result.exit_code
    assert field + " is invalid" in result.output


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
def test_datasets_list_empty(output_format, runner, project):
    """Test listing without datasets."""
    format_option = f"--format={output_format}"
    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
@pytest.mark.parametrize(
    "datadir_option,datadir", [([], f"{DATA_DIR}/my-dataset"), (["--datadir", "my-dir"], "my-dir")]
)
def test_datasets_list_non_empty(output_format, runner, project, datadir_option, datadir):
    """Test listing with datasets."""
    format_option = f"--format={output_format}"
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"] + datadir_option)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code, format_result_exception(result)

    if output_format != "json-ld":
        assert "my-dataset" in result.output
        assert datadir in result.output


@pytest.mark.parametrize(
    "columns,headers,values",
    [
        ("title,short_name", ["TITLE", "NAME"], ["my-dataset", "Long Title"]),
        ("title,name", ["TITLE", "NAME"], ["my-dataset", "Long Title"]),
        ("creators", ["CREATORS"], ["John Doe"]),
    ],
)
def test_datasets_list_with_columns(runner, project, columns, headers, values):
    """Test listing datasets with custom column name."""
    result = runner.invoke(
        cli, ["dataset", "create", "my-dataset", "--title", "Long Title", "-c", "John Doe <john.doe@mail.ch>"]
    )
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "ls", "--columns", columns])
    assert 0 == result.exit_code, format_result_exception(result)
    assert headers == result.output.split("\n").pop(0).split()
    for value in values:
        assert value in result.output


@pytest.mark.parametrize("column", DATASETS_COLUMNS.keys())
def test_datasets_list_columns_correctly(runner, project, column):
    """Test dataset listing only shows requested columns."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "test"]).exit_code

    result = runner.invoke(cli, ["dataset", "ls", "--columns", column])
    assert 0 == result.exit_code, format_result_exception(result)
    header = result.output.split("\n").pop(0)
    name, display_name = DATASETS_COLUMNS[column]
    display_name = display_name or name
    assert display_name.upper() == header


@pytest.mark.parametrize("columns", ["invalid", "id,invalid"])
def test_datasets_list_invalid_column(runner, project, columns):
    """Test dataset listing invalid column name."""
    result = runner.invoke(cli, ["dataset", "ls", "--columns", columns])
    assert 2 == result.exit_code
    assert 'Invalid column name: "invalid".' in result.output


def test_datasets_list_description(runner, project):
    """Test dataset description listing."""
    description = "Very long description. " * 100
    assert 0 == runner.invoke(cli, ["dataset", "create", "test", "-d", description]).exit_code

    short_description = textwrap.wrap(description, width=64, max_lines=2)[0]

    result = runner.invoke(cli, ["dataset", "ls", "--columns=name,description"])

    assert 0 == result.exit_code, format_result_exception(result)
    line = next(line for line in result.output.split("\n") if "test" in line)
    assert short_description in line
    assert description[: len(short_description) + 1] not in line


@pytest.mark.parametrize(
    "datadir_option,datadir", [([], f"{DATA_DIR}/new-dataset"), (["--datadir", "my-dir"], "my-dir")]
)
def test_add_and_create_dataset(directory_tree, runner, project, subdirectory, datadir_option, datadir):
    """Test add data to a non-existing dataset."""
    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 1 == result.exit_code
    assert "Dataset 'new-dataset' does not exist." in result.output

    existing_file = project.path / datadir / "my-folder" / "my-file"
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.write_text("content")

    existing_folder = project.path / datadir / "my_other_folder"
    existing_folder.mkdir(parents=True, exist_ok=True)

    # Add succeeds with --create
    result = runner.invoke(
        cli,
        ["dataset", "add", "--create", "--copy", "new-dataset", str(directory_tree)] + datadir_option,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result)

    path1 = os.path.join(project.path, datadir, directory_tree.name, "file1")
    path2 = os.path.join(project.path, datadir, directory_tree.name, "dir1", "file2")
    path3 = os.path.join(project.path, datadir, directory_tree.name, "dir1", "file3")

    assert os.stat(path1)
    assert os.stat(path2)
    assert os.stat(path3)
    dataset = get_dataset_with_injection("new-dataset")
    assert {os.path.relpath(p, project.path) for p in [path1, path2, path3, existing_file]} == {
        f.entity.path for f in dataset.files
    }

    # Further, add with --create fails
    result = runner.invoke(cli, ["dataset", "add", "--copy", "--create", "new-dataset", str(directory_tree)])
    assert 1 == result.exit_code


def test_add_and_create_dataset_with_lfs_warning(directory_tree, runner, project_with_lfs_warning):
    """Test add data with lfs warning."""

    # Add succeeds with --create
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "--copy", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Adding these files to Git LFS" in result.output
    assert "dir1/file2" in result.output
    assert "file" in result.output


def test_add_to_dirty_repo(directory_tree, runner, project):
    """Test adding to a dataset in a dirty repo commits only added files."""
    with (project.path / "tracked").open("w") as fp:
        fp.write("tracked file")
    project.repository.add(all=True)
    project.repository.commit("tracked file")

    with (project.path / "tracked").open("w") as fp:
        fp.write("modified tracked file")
    with (project.path / "untracked").open("w") as fp:
        fp.write("untracked file")

    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    assert project.repository.is_dirty()
    assert ["untracked"] == project.repository.untracked_files

    # Add without making a change
    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 1 == result.exit_code

    assert project.repository.is_dirty()
    assert ["untracked"] == project.repository.untracked_files


def test_add_unicode_file(tmpdir, runner, project):
    """Test adding files with unicode special characters in their names."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    filename = "fi1é-àèû爱ಠ_ಠ.txt"
    new_file = tmpdir.join(filename)
    new_file.write("test")

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert filename in result.output.encode("latin1").decode("unicode-escape")


def test_multiple_file_to_dataset(tmpdir, runner, project):
    """Test importing multiple data into a dataset at once."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = get_dataset_with_injection("dataset")
    assert dataset.title == "dataset"

    paths = []
    for i in range(3):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("datadir_option,datadir", [([], f"{DATA_DIR}/local"), (["--datadir", "my-dir"], "my-dir")])
def test_add_with_relative_path(runner, project, directory_tree, subdirectory, datadir_option, datadir):
    """Test adding data with relative path."""
    relative_path = os.path.relpath(directory_tree / "file1", os.getcwd())

    result = runner.invoke(cli, ["dataset", "add", "--copy", "--create", "local", relative_path] + datadir_option)
    assert 0 == result.exit_code, format_result_exception(result)

    path = project.path / datadir / "file1"
    assert path.exists()
    assert "file1 content" == path.read_text()


@pytest.mark.parametrize(
    "action,existing_paths,missing_paths,existing_links",
    [
        ("--copy", ["my-file", "data/local/my-file"], [], []),
        ("--move", ["data/local/my-file"], ["my-file"], []),
        ("--link", ["my-file"], [], ["data/local/my-file"]),
    ],
)
def test_add_local_actions(runner, project, action, existing_paths, missing_paths, existing_links):
    """Test adding local data with different actions."""
    with (project.path / "my-file").open("w") as fp:
        fp.write("my file")

    result = runner.invoke(cli, ["dataset", "add", action, "--create", "local", "my-file"])
    assert 0 == result.exit_code, format_result_exception(result)

    for existing_path in existing_paths:
        path = Path()
        assert Path(existing_path).exists()
        assert not path.is_symlink()

    for missing_path in missing_paths:
        assert not Path(missing_path).exists()

    for existing_link in existing_links:
        path = Path(existing_link)
        assert path.exists()
        assert path.is_symlink()


@pytest.mark.parametrize("action, source_exists_after", [("--copy", True), ("--move", False)])
def test_add_non_local_actions(runner, project, directory_tree, action, source_exists_after):
    """Test adding data outside the project with different actions."""
    path = directory_tree / "file1"

    result = runner.invoke(cli, ["dataset", "add", action, "--create", "local", path])

    assert 0 == result.exit_code, format_result_exception(result)
    assert source_exists_after == path.exists()
    assert (project.path / "data" / "local" / "file1").exists()


def test_add_non_local_link_action(runner, project, directory_tree):
    """Test cannot add and link data outside the project."""
    path = directory_tree / "file1"

    result = runner.invoke(cli, ["dataset", "add", "--link", "--create", "local", path])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "Cannot use '--link' for files outside of project:" in result.output


@pytest.mark.parametrize("action, source_exists_after", [("copy", True), ("move", False)])
@pytest.mark.serial
def test_add_default_configured_actions(runner, project, directory_tree, action, source_exists_after):
    """Test adding data with different actions set in Renku configuration file."""
    path = directory_tree / "file1"
    set_value("renku", "default_dataset_add_action", action, global_only=True)

    result = runner.invoke(cli, ["dataset", "add", "--create", "local", path])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "The following files will be copied to" not in result.output
    assert path.exists() is source_exists_after
    assert (project.path / "data" / "local" / "file1").exists()


@pytest.mark.serial
def test_add_default_configured_link(runner, project, directory_tree):
    """Test adding data with default ``link`` action should prompt the user."""
    path = directory_tree / "file1"
    set_value("renku", "default_dataset_add_action", "link", global_only=True)

    result = runner.invoke(cli, ["dataset", "add", "--create", "local", path], input="y\n")

    assert 0 == result.exit_code, format_result_exception(result)
    assert "The following files will be copied to" in result.output
    assert path.exists()
    assert (project.path / "data" / "local" / "file1").exists()
    assert not (project.path / "data" / "local" / "file1").is_symlink()


@pytest.mark.serial
def test_add_default_configured_invalid_action(runner, project, directory_tree):
    """Test adding data with an invalid actions set in Renku configuration file."""
    path = directory_tree / "file1"
    set_value("renku", "default_dataset_add_action", "invalid", global_only=True)

    result = runner.invoke(cli, ["dataset", "add", "--create", "local", path])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "Invalid default action for adding to datasets in Renku config: 'invalid'." in result.output
    assert "Valid values are 'copy', 'link', and 'move'." in result.output


def test_add_an_empty_directory(runner, project, directory_tree):
    """Test adding an empty directory to a dataset."""
    path = directory_tree / "empty-directory"
    path.mkdir()

    result = runner.invoke(cli, ["dataset", "add", "--copy", "--create", "local", str(path)])
    assert 2 == result.exit_code, format_result_exception(result)
    assert "Error: There are no files to create a dataset" in result.output


def test_repository_file_to_dataset(runner, project, subdirectory):
    """Test adding a file from the repository into a dataset."""
    # create a dataset
    assert 0 == runner.invoke(cli, ["dataset", "create", "dataset"]).exit_code

    a_path = project.path / "a"
    a_path.write_text("a content")

    project.repository.add(a_path)
    project.repository.commit(message="Added file a", no_verify=True)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "dataset", str(a_path)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("dataset")
    assert dataset.title == "dataset"
    assert dataset.find_file("data/dataset/a") is not None


def test_relative_import_to_dataset(tmpdir, runner, project, subdirectory):
    """Test importing data from a directory structure."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = get_dataset_with_injection("dataset")
    assert dataset.title == "dataset"

    zero_data = tmpdir.join("zero.txt")
    zero_data.write("zero")

    first_level = tmpdir.mkdir("first")
    second_level = first_level.mkdir("second")

    first_data = first_level.join("first.txt")
    first_data.write("first")

    second_data = second_level.join("second.txt")
    second_data.write("second")

    paths = [str(zero_data), str(first_level), str(second_level)]

    result = runner.invoke(cli, ["dataset", "add", "--copy", "dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    assert os.stat(project.path / DATA_DIR / "dataset" / "zero.txt")
    assert os.stat(project.path / DATA_DIR / "dataset" / "first" / "first.txt")
    assert os.stat(project.path / DATA_DIR / "dataset" / "first" / "second" / "second.txt")


@pytest.mark.parametrize(
    "params,message",
    [
        (["-s", "file", "https://renkulab.io/"], "Cannot use '-s/--src/--source' with URLs or local files."),
        (["-s", "file", "/some/local/path"], "Cannot use '-s/--src/--source' with URLs or local files."),
    ],
)
def test_usage_error_in_add_from_url(runner, project, params, message):
    """Test user's errors when adding URL/local file to a dataset."""
    result = runner.invoke(cli, ["dataset", "add", "remote", "--create"] + params, catch_exceptions=False)
    assert 2 == result.exit_code
    assert message in result.output


def test_add_untracked_file(runner, project):
    """Test adding an untracked file to a dataset."""
    untracked = project.path / "untracked"
    untracked.write_text("untracked")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "--create", str(untracked)])

    assert 0 == result.exit_code, format_result_exception(result)

    assert project.repository.is_dirty()
    assert project.repository.contains(project.path / "data" / "my-dataset" / "untracked")
    assert get_dataset_with_injection("my-dataset").find_file("data/my-dataset/untracked")


def test_add_data_directory(runner, project, directory_tree):
    """Test adding a dataset's data directory to it prints an error."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "--create", "new-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "new-dataset", "data/new-dataset"], catch_exceptions=False)
    assert 2 == result.exit_code
    assert "Cannot recursively add path containing dataset's data directory" in result.output


def test_dataset_add_with_copy(tmpdir, runner, project):
    """Test adding data to dataset with copy."""
    import os
    import stat

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    paths = []
    original_inodes = []
    for i in range(3):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        original_inodes.append(os.lstat(str(new_file))[stat.ST_INO])
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    received_inodes = []
    dataset = get_dataset_with_injection("my-dataset")
    assert dataset.title == "my-dataset"

    for file in dataset.files:
        path = (project.path / file.entity.path).resolve()
        received_inodes.append(os.lstat(path)[stat.ST_INO])

    # check that original inodes are within created ones
    for inode in received_inodes:
        assert inode not in original_inodes


@pytest.mark.serial
def test_dataset_add_many(tmpdir, runner, project):
    """Test adding many files to dataset."""

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    paths = []
    for i in range(1000):
        new_file = tmpdir.join(f"file_{i}")
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    assert len(project.repository.head.commit.message.splitlines()[0]) <= 100


def test_dataset_file_path_from_subdirectory(runner, project, subdirectory):
    """Test adding a file into a dataset and check path independent of the CWD."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    a_path = project.path / "a"
    a_path.write_text("a text")

    project.repository.add(a_path)
    project.repository.commit(message="Added file a")

    # add data
    result = runner.invoke(cli, ["dataset", "add", "--copy", "dataset", str(a_path)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("dataset")
    file = dataset.find_file("data/dataset/a")
    assert file is not None
    assert "data/dataset/a" == file.entity.path


def test_datasets_ls_files_tabular_empty(runner, project):
    """Test listing of data within empty dataset."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # list all files in dataset
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", "added,creators,dataset,path", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check output
    output = result.output.split("\n")
    assert output.pop(0).split() == ["ADDED", "CREATORS", "DATASET", "PATH"]
    assert set(output.pop(0)) == {" ", "-"}
    assert output.pop(0) == ""
    assert not output


@pytest.mark.parametrize("output_format", DATASET_FILES_FORMATS.keys())
def test_datasets_ls_files_check_exit_code(output_format, runner, project):
    """Test file listing exit codes for different formats."""
    format_option = f"--format={output_format}"
    result = runner.invoke(cli, ["dataset", "ls-files", format_option])
    assert 0 == result.exit_code, format_result_exception(result)


def test_datasets_ls_files_lfs(runner, project, tmpdir, large_file):
    """Test file listing lfs status."""
    # NOTE: create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # NOTE: create some data
    paths = []

    new_file = tmpdir.join("file_1")
    new_file.write(str(1))
    paths.append(str(new_file))

    paths.append(str(large_file))

    # NOTE: add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # NOTE: check files
    result = runner.invoke(cli, ["dataset", "ls-files"])
    assert 0 == result.exit_code, format_result_exception(result)

    lines = result.output.split("\n")
    file1_entry = next(line for line in lines if "file_1" in line)
    file2_entry = next(line for line in lines if large_file.name in line)

    assert file1_entry
    assert file2_entry
    assert not file1_entry.endswith("*")
    assert file2_entry.endswith("*")


def test_datasets_ls_files_json(runner, project, tmpdir, large_file):
    """Test file listing lfs status."""
    # NOTE: create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # NOTE: create some data
    paths = []

    new_file = tmpdir.join("file_1")
    new_file.write(str(1))
    paths.append(str(new_file))

    paths.append(str(large_file))

    # NOTE: add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # NOTE: check files
    result = runner.invoke(cli, ["dataset", "ls-files", "--format", "json"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = json.loads(result.output)

    assert len(result) == 2
    file1 = next(f for f in result if f["path"].endswith("file_1"))
    file2 = next(f for f in result if f["path"].endswith(large_file.name))

    assert not file1["is_lfs"]
    assert file2["is_lfs"]

    assert file1["creators"]
    assert file1["size"]
    assert file1["dataset_name"]
    assert file1["dataset_id"]

    assert file2["creators"]
    assert file2["size"]
    assert file2["dataset_name"]
    assert file2["dataset_id"]


@pytest.mark.parametrize("column", DATASET_FILES_COLUMNS.keys())
def test_datasets_ls_files_columns_correctly(runner, project, column, directory_tree):
    """Test file listing only shows requested columns."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree)]).exit_code

    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", column], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    header = result.output.split("\n").pop(0).strip()
    name, display_name = DATASET_FILES_COLUMNS[column]
    display_name = display_name or name
    assert display_name.upper() == header


@pytest.mark.parametrize("columns", ["invalid", "path,invalid"])
def test_datasets_ls_files_invalid_column(runner, project, columns):
    """Test file listing with invalid column name."""
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", columns])
    assert 2 == result.exit_code
    assert 'Invalid column name: "invalid".' in result.output


def test_datasets_ls_files_tabular_dataset_filter(runner, project, directory_tree):
    """Test listing of data within dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree)]).exit_code

    # list all files in non empty dataset
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", "added,path", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check output from ls-files command
    output = result.output.split("\n")
    assert output.pop(0).split() == ["ADDED", "PATH"]
    assert set(output.pop(0)) == {" ", "-"}

    created_files = list(f.name for f in directory_tree.rglob("*file*"))
    # check listing
    added_at = []
    for _ in range(3):
        row = output.pop(0).split(" ")
        assert Path(row.pop()).name in created_files
        added_at.append(row.pop(0))

    # check if sorted by added_at
    assert added_at == sorted(added_at)


def test_datasets_ls_files_tabular_patterns(runner, project, directory_tree):
    """Test listing of data within dataset with include/exclude filters."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree)]).exit_code

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--include=**/file*", "--exclude=**/file2"])

    assert 0 == result.exit_code, format_result_exception(result)
    # check output
    assert "file1" in result.output
    assert "file2" not in result.output
    assert "file3" in result.output

    # check directory pattern
    result = runner.invoke(cli, ["dataset", "ls-files", "--include=**/dir1/*"])

    assert 0 == result.exit_code, format_result_exception(result)
    # check output
    assert "file1" not in result.output
    assert "file2" in result.output
    assert "file3" in result.output


def test_datasets_ls_files_tabular_creators(runner, project, directory_tree):
    """Test listing of data within dataset with creators filters."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree)]).exit_code
    creator = get_dataset_with_injection("my-dataset").creators[0].name

    assert creator is not None

    # check creators filters
    result = runner.invoke(cli, ["dataset", "ls-files", f"--creators={creator}"])
    assert 0 == result.exit_code, format_result_exception(result)

    # check output
    for file_ in directory_tree.rglob("*file*"):
        assert file_.name in result.output


def test_datasets_ls_files_correct_paths(runner, project, directory_tree):
    """Test listing of data within dataset and check that paths are correct."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree)]).exit_code

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)

    output = json.loads(result.output)
    for record in output:
        for entity in record:
            path = entity.get("http://www.w3.org/ns/prov#atLocation")
            if path:
                path = path[0]["@value"]
                assert (project.path / path).exists()


def test_datasets_ls_files_with_name(directory_tree, runner, project):
    """Test listing of data within dataset with include/exclude filters."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset", "--title", "Long Title"])
    assert 0 == result.exit_code, format_result_exception(result)

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # list files with name
    result = runner.invoke(cli, ["dataset", "ls-files", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "dir1/file2" in result.output


def test_datasets_ls_files_correct_size(runner, project, directory_tree, large_file):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert (
        0
        == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code
    )

    path = project.path / DATA_DIR / "my-dataset" / "file1"
    shutil.copy(large_file, path)

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns=size, path"])
    assert 0 == result.exit_code, format_result_exception(result)

    line = next(line for line in result.output.split("\n") if "file1" in line)
    size = int(line.split()[0])

    assert 13 == size


@pytest.mark.skip(reason="FIXME: We don't have commit SHAs for files. What should be listed here?")
def test_datasets_ls_files_correct_commit(runner, project, directory_tree):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert (
        0
        == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code
    )

    commit = project.repository.get_previous_commit(path=project.path / DATA_DIR / "my-dataset" / "file1")

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns=commit,path"])
    assert 0 == result.exit_code, format_result_exception(result)

    line = next(line for line in result.output.split("\n") if "file1" in line)
    commit_sha = line.split()[0]

    assert commit.hexsha == commit_sha


def test_dataset_unlink_file_not_found(runner, project):
    """Test unlinking of file from dataset with no files found."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", "not-there.csv"])

    assert 2 == result.exit_code, format_result_exception(result)


def test_dataset_unlink_file_abort_unlinking(tmpdir, runner, project):
    """Test unlinking of file from dataset and aborting."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)

    # unlink file from dataset
    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename], input="n")
    assert 1 == result.exit_code

    # check output
    assert "Aborted!" in result.output


def test_dataset_unlink_file(tmpdir, runner, project, subdirectory):
    """Test unlinking of file and check removal from dataset."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not project.repository.is_dirty()

    dataset = get_dataset_with_injection("my-dataset")
    created_dataset_files = [Path(f.entity.path) for f in dataset.files]
    assert new_file.basename in {f.name for f in created_dataset_files}

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename, "-y"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not project.repository.is_dirty()

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after

    dataset = get_dataset_with_injection("my-dataset")

    assert new_file.basename not in [Path(f.entity.path).name for f in dataset.files if not f.is_removed()]
    assert all([not f.exists() for f in created_dataset_files])


def test_dataset_rm(runner, project, directory_tree, subdirectory):
    """Test removal of a dataset."""
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "--copy", "--create", "my-dataset", str(directory_tree)]).exit_code
    )

    assert get_dataset_with_injection("my-dataset")

    result = runner.invoke(cli, ["dataset", "rm", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output
    assert not get_dataset_with_injection("my-dataset")

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_dataset_rm_failure(runner, project):
    """Test errors in removal of a dataset."""
    assert 2 == runner.invoke(cli, ["dataset", "rm"]).exit_code
    assert 1 == runner.invoke(cli, ["dataset", "rm", "does-not-exist"]).exit_code


def test_dataset_overwrite_no_confirm(runner, project):
    """Check dataset overwrite behaviour without confirmation."""
    result = runner.invoke(cli, ["dataset", "create", "rockstar"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "rockstar"])
    assert 1 == result.exit_code
    assert "OK" not in result.output


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit(runner, project, dirty, subdirectory):
    """Check dataset metadata editing."""
    if dirty:
        (project.path / "README.md").write_text("Make repo dirty.")

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    result = runner.invoke(
        cli,
        ["dataset", "create", "dataset", "-t", "original title", "-k", "keyword-1", "--metadata", str(metadata_path)],
    )
    assert 0 == result.exit_code, format_result_exception(result)

    creator1 = "Forename1 Surname1 <name.1@mail.com> [Affiliation 1]"
    creator2 = "Forename2 Surname2"

    result = runner.invoke(
        cli,
        ["dataset", "edit", "dataset", "-d", " new description ", "-c", creator1, "-c", creator2],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: creators, description." in result.output
    warning_msg = "Warning: No email or wrong format for: Forename2 Surname2"
    assert warning_msg in result.output

    dataset = get_dataset_with_injection("dataset")
    assert " new description " == dataset.description
    assert "original title" == dataset.title
    assert {creator1, creator2}.issubset({c.full_identity for c in dataset.creators})

    result = runner.invoke(cli, ["dataset", "edit", "dataset", "-t", " new title "], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: title." in result.output

    result = runner.invoke(
        cli, ["dataset", "edit", "dataset", "-k", "keyword-2", "-k", "keyword-3"], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: keywords." in result.output

    new_metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_other_unique_value",
    }
    metadata_path.write_text(json.dumps(new_metadata))

    result = runner.invoke(
        cli, ["dataset", "edit", "dataset", "--metadata", str(metadata_path)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: custom_metadata." in result.output

    dataset = get_dataset_with_injection("dataset")
    assert " new description " == dataset.description
    assert "new title" == dataset.title
    assert {creator1, creator2}.issubset({c.full_identity for c in dataset.creators})
    assert {"keyword-2", "keyword-3"} == set(dataset.keywords)
    assert 1 == len(dataset.annotations)
    assert new_metadata == dataset.annotations[0].body

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize(
    "metadata",
    [
        [
            {
                "@id": "https://example.com/annotation1",
                "@type": "https://schema.org/specialType",
                "https://schema.org/specialProperty": "some_unique_value",
            },
            {
                "@id": "https://example.com/annotation2",
                "@type": "https://schema.org/specialType2",
                "https://schema.org/specialProperty2": "some_unique_value2",
            },
        ],
        {
            "@id": "https://example.com/annotation1",
            "@type": "https://schema.org/specialType",
            "https://schema.org/specialProperty": "some_unique_value",
        },
    ],
)
@pytest.mark.parametrize("source", [None, "test1"])
def test_dataset_edit_metadata(runner, project, source, metadata):
    """Check dataset metadata editing."""
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))
    create_args = [
        "dataset",
        "create",
        "dataset",
        "-t",
        "original title",
        "-k",
        "keyword-1",
    ]
    edit_args = [
        "dataset",
        "edit",
        "dataset",
        "--metadata",
        str(metadata_path),
    ]
    if source is None:
        expected_source = "renku"
    else:
        expected_source = source
        edit_args.append("--metadata-source")
        edit_args.append(source)
    result = runner.invoke(cli, create_args)
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, edit_args)
    assert 0 == result.exit_code, format_result_exception(result)
    dataset = get_dataset_with_injection("dataset")
    annotation_bodies = [annotation.body for annotation in dataset.annotations]
    annotation_sources = [annotation.source for annotation in dataset.annotations]
    if isinstance(metadata, dict):
        metadata = [metadata]
    assert all([imetadata in annotation_bodies for imetadata in metadata])
    assert all([imetadata in metadata for imetadata in annotation_bodies])
    assert len(annotation_bodies) == len(metadata)
    assert all([isource == expected_source for isource in annotation_sources])


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit_unset(runner, project, dirty, subdirectory):
    """Check dataset metadata editing unsetting values."""
    if dirty:
        (project.path / "README.md").write_text("Make repo dirty.")

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    result = runner.invoke(
        cli,
        [
            "dataset",
            "create",
            "dataset",
            "-t",
            "original title",
            "-c",
            "John Doe <john@does.example.com>",
            "-k",
            "keyword-1",
            "--metadata",
            str(metadata_path),
        ],
    )
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli,
        ["dataset", "edit", "dataset", "-u", "keywords", "-u", "metadata"],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: keywords, custom_metadata." in result.output

    dataset = get_dataset_with_injection("dataset")
    assert 0 == len(dataset.keywords)
    assert 0 == len(dataset.annotations)


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit_no_change(runner, project, dirty):
    """Check metadata editing does not commit when there is no change."""
    result = runner.invoke(cli, ["dataset", "create", "dataset", "-t", "original title"])
    assert 0 == result.exit_code, format_result_exception(result)

    if dirty:
        (project.path / "README.md").write_text("Make repo dirty.")

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "edit", "dataset"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Nothing to update." in result.output

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_after == commit_sha_before
    assert dirty is project.repository.is_dirty()


@pytest.mark.parametrize(
    "uri", ["10.5281/zenodo.3363060", "doi:10.5281/zenodo.3363060", "https://zenodo.org/record/3363060"]
)
def test_dataset_provider_resolution_zenodo(doi_responses, uri):
    """Check that zenodo uris resolve to ZenodoProvider."""
    provider = ProviderFactory.get_import_provider(uri)
    assert type(provider) is ZenodoProvider


@pytest.mark.parametrize(
    "uri",
    [
        "10.7910/DVN/TJCLKP",
        "doi:10.7910/DVN/TJCLKP",
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/TJCLKP",
    ],
)
@pytest.mark.integration
@pytest.mark.shaky
def test_dataset_provider_resolution_dataverse(doi_responses, uri):
    """Check that dataverse URIs resolve to ``DataverseProvider``."""
    provider = ProviderFactory.get_import_provider(uri)
    assert type(provider) is DataverseProvider


def test_dataset_tag(tmpdir, runner, project, subdirectory):
    """Test that dataset tags can be created."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write("test")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli, ["dataset", "tag", "my-dataset", "A", "-d", "short description"], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-dataset")
        all_tags = datasets_provenance.get_all_tags(dataset)
        assert {dataset.id} == {t.dataset_id.value for t in all_tags}

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_dataset_overwrite_tag(runner, project_with_datasets):
    """Test that dataset tags can be overwritten."""
    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "dataset-1", "1.0"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # retag
    result = runner.invoke(cli, ["dataset", "tag", "dataset-1", "1.0"], catch_exceptions=False)
    assert 2 == result.exit_code, format_result_exception(result)
    assert "Tag '1.0' already exists" in result.output

    # force overwrite
    result = runner.invoke(cli, ["dataset", "tag", "--force", "dataset-1", "1.0"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 1 == result.output.count('"@id": "https://localhost/dataset-tags/1.0%40')


@pytest.mark.parametrize("form", ["tabular", "json-ld"])
def test_dataset_ls_tags(tmpdir, runner, project, form):
    """Test listing of dataset tags."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write("test")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id1 = get_dataset_with_injection("my-dataset").id

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0", "-d", "first tag!"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id2 = get_dataset_with_injection("my-dataset").id

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "ls-tags", "my-dataset", f"--format={form}"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "1.0" in result.output
    assert "aBc9.34-11_55.t" in result.output
    assert "first tag!" in result.output
    assert id1 in result.output
    assert id2 in result.output


def test_dataset_rm_tag(tmpdir, runner, project, subdirectory):
    """Test removing of dataset tags."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write("test")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id1 = get_dataset_with_injection("my-dataset").id

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0", "-d", "first tag!"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "ls-tags", "my-dataset"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "1.0" in result.output
    assert "first tag!" in result.output
    assert id1 in result.output

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "2.0"], catch_exceptions=False)
    assert 2 == result.exit_code
    assert "not found" in result.output

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1.0"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1.0"], catch_exceptions=False)
    assert 2 == result.exit_code
    assert "not found" in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_dataset_rm_tags_multiple(tmpdir, runner, project):
    """Test removing multiple dataset tags at once."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write("test")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    for i in range(1, 4):
        # tag dataset
        result = runner.invoke(cli, ["dataset", "tag", "my-dataset", str(i)], catch_exceptions=False)
        assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1", "2", "3"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "1" not in result.output
    assert "2" not in result.output
    assert "3" not in result.output


def test_dataset_rm_tags_failure(tmpdir, runner, project):
    """Test removing non-existent dataset tag."""
    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False)

    assert 1 == result.exit_code
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write("test")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False)
    assert 2 == result.exit_code


def test_dataset_clean_up_when_add_fails(runner, project, subdirectory):
    """Test project is cleaned when dataset add fails for a new dataset."""
    # add a non-existing path to a new dataset
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", "non-existing-file"], catch_exceptions=True
    )

    assert 2 == result.exit_code
    ref = project.metadata_path / "refs" / "datasets" / "new-dataset"
    assert not ref.is_symlink() and not ref.exists()


def test_avoid_empty_commits(runner, project, directory_tree):
    """Test no empty commit is created when adding existing data."""
    runner.invoke(cli, ["dataset", "create", "my-dataset"])

    commit_sha_before = project.repository.head.commit.hexsha
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after

    commit_sha_before = commit_sha_after
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree)])
    assert 1 == result.exit_code
    assert "Error: There is nothing to commit." in result.output

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before == commit_sha_after


def test_multiple_dataset_commits(runner, project, directory_tree):
    """Check adding existing data to multiple datasets."""
    commit_sha_before = project.repository.head.commit.hexsha
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset1", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after

    commit_sha_before = commit_sha_after
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset2", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after


@pytest.mark.parametrize("filename", [".renku", ".renku/", "Dockerfile"])
def test_add_protected_file(runner, project, filename, subdirectory):
    """Check adding a protected file."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset1", str(project.path / filename)])

    assert 1 == result.exit_code
    assert "Error: The following paths are protected" in result.output


@pytest.mark.parametrize("filename", [".renku-not-actually-renku", "this-is-not.renku"])
def test_add_non_protected_file(runner, project, tmpdir, filename, subdirectory):
    """Check adding an 'almost' protected file."""
    new_file = tmpdir.join(filename)
    new_file.write("test")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset1", str(new_file)])

    assert 0 == result.exit_code, format_result_exception(result)


def test_add_removes_local_path_information(runner, project, directory_tree):
    """Test added local paths are stored as relative path."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("my-dataset")
    relative_path = os.path.relpath(directory_tree, project.path)
    for file in dataset.files:
        assert file.source.startswith(relative_path)
        assert file.source.endswith(Path(file.entity.path).name)


def test_pull_data_from_lfs(runner, project, tmpdir, subdirectory, no_lfs_size_limit):
    """Test pulling data from LFS using relative paths."""
    data = tmpdir.join("data.txt")
    data.write("DATA")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-data", str(data)])
    assert 0 == result.exit_code, format_result_exception(result)
    attributes = (project.path / ".gitattributes").read_text().split()
    assert "data/my-data/data.txt" in attributes

    path = project.path / DATA_DIR / "my-data" / "data.txt"
    relative_path = os.path.relpath(path, os.getcwd())

    result = runner.invoke(cli, ["storage", "pull", relative_path])
    assert 0 == result.exit_code, format_result_exception(result)


def test_lfs_hook(project_with_injection, subdirectory, large_file):
    """Test committing large files to Git."""
    filenames = {"large-file", "large file with whitespace", "large*file?with wildcards"}

    for filename in filenames:
        shutil.copy(large_file, project_with_injection.path / filename)
    project_with_injection.repository.add(all=True)

    # Commit fails when file is not tracked in LFS
    with pytest.raises(errors.GitCommandError) as e:
        project_with_injection.repository.commit("large files not in LFS")

    assert "You are trying to commit large files to Git" in e.value.stderr
    for filename in filenames:
        assert filename in e.value.stderr

    # Can be committed after being tracked in LFS
    track_paths_in_storage(*filenames)
    project_with_injection.repository.add(all=True)
    commit = project_with_injection.repository.commit("large files tracked")
    assert "large files tracked\n" == commit.message

    tracked_lfs_files = set(
        project_with_injection.repository.run_git_command("lfs", "ls-files", "--name-only").split("\n")
    )
    assert filenames == tracked_lfs_files


@pytest.mark.parametrize("use_env_var", [False, True])
def test_lfs_hook_autocommit(runner, project, subdirectory, large_file, use_env_var):
    """Test committing large files to Git gets automatically added to lfs."""
    if use_env_var:
        os.environ["AUTOCOMMIT_LFS"] = "true"
    else:
        assert 0 == runner.invoke(cli, ["config", "set", "autocommit_lfs", "true"]).exit_code

    filenames = {"large-file", "large file with whitespace", "large*file?with wildcards"}

    for filename in filenames:
        shutil.copy(large_file, project.path / filename)
    project.repository.add(all=True)

    result = project.repository.run_git_command(
        "commit",
        message="large files not in LFS",
        with_extended_output=True,
        env={"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"},
    )
    for filename in filenames:
        assert filename in result[1]

    assert "You are trying to commit large files to Git instead of Git-LFS" in result[2]
    assert "Adding files to LFS" in result[2]
    for filename in filenames:
        assert f'Tracking "{filename}"' in result[2]
    assert len(get_dirty_paths(project.repository)) == 0  # NOTE: make sure repo is clean

    tracked_lfs_files = set(project.repository.run_git_command("lfs", "ls-files", "--name-only").split("\n"))
    assert filenames == tracked_lfs_files


def test_lfs_hook_can_be_avoided(runner, project, subdirectory, large_file):
    """Test committing large files to Git."""
    result = runner.invoke(
        cli, ["--no-external-storage", "dataset", "add", "--copy", "-c", "my-dataset", str(large_file)]
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output


def test_datadir_hook(runner, project, subdirectory):
    """Test pre-commit hook fir checking datadir files."""
    set_value(section="renku", key="check_datadir_files", value="true", global_only=True)

    datadir = project.path / "test"
    datadir.mkdir()

    result = runner.invoke(cli, ["--no-external-storage", "dataset", "create", "--datadir", str(datadir), "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    file = datadir / "new_file"
    file.write_text("some updates")
    file2 = datadir / "another_file"
    file2.write_text("some updates")

    project.repository.add(all=True)

    # Commit fails when a file in datadir is not added to a dataset
    with pytest.raises(errors.GitCommandError) as e:
        project.repository.commit("datadir files not in dataset")

    assert "Files in datasets data directory that aren't up to date" in e.value.stderr

    assert file.name in e.value.stderr
    assert file2.name in e.value.stderr

    result = runner.invoke(cli, ["--no-external-storage", "dataset", "update", "-c", "--all", "--no-remote"])
    assert 0 == result.exit_code, format_result_exception(result)

    file3 = datadir / "yet_another_new_file"
    file3.write_text("some updates")
    project.repository.add(all=True)

    # Commit fails when a file in datadir is not added to a dataset
    with pytest.raises(errors.GitCommandError) as e:
        project.repository.commit("datadir files not in dataset")

    assert "Files in datasets data directory that aren't up to date" in e.value.stderr

    result = runner.invoke(cli, ["config", "set", "check_datadir_files", "false"])
    assert 0 == result.exit_code, format_result_exception(result)

    project.repository.add(all=True)
    # Commit would fail if a file in datadir is not added to a dataset
    project.repository.commit("datadir files in dataset")


def test_add_existing_files(runner, project, directory_tree, no_lfs_size_limit):
    """Check adding/overwriting existing files."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)

    path = Path(DATA_DIR) / "my-dataset" / directory_tree.name / "file1"

    dataset = get_dataset_with_injection("my-dataset")
    assert dataset.find_file(path) is not None

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree)])
    assert 1 == result.exit_code
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "Warning: No new file was added to project" in result.output
    assert "Error: There is nothing to commit." in result.output

    result = runner.invoke(cli, ["dataset", "add", "--copy", "--overwrite", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" not in result.output
    assert str(path) not in result.output
    assert "Warning: No new file was added to project" in result.output
    assert "Error: There is nothing to commit." not in result.output  # dataset metadata is always updated


def test_add_existing_and_new_files(runner, project, directory_tree):
    """Check adding/overwriting existing files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", str(directory_tree)]).exit_code

    path = Path(DATA_DIR) / "my-dataset" / directory_tree.name / "file1"

    # Add existing files and files within same project
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree), "README.md"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output

    # Add existing and non-existing files
    directory_tree.joinpath("new-file").write_text("new-file")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "OK" in result.output


def test_add_existing_files_updates_metadata(runner, project, large_file):
    """Check overwriting existing files updates their metadata."""
    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "--create", str(large_file)])
    assert result.exit_code == 0, result.output

    path = Path(DATA_DIR) / "my-dataset" / large_file.name

    before = get_dataset_with_injection("my-dataset").find_file(path)

    time.sleep(2)
    large_file.write_text("New modified content.")

    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", "--overwrite", str(large_file)]).exit_code

    after = get_dataset_with_injection("my-dataset").find_file(path)
    assert before.id != after.id
    assert before.date_added != after.date_added
    assert before.entity.checksum != after.entity.checksum
    assert before.entity.path == after.entity.path
    assert before.source == after.source


def test_add_ignored_files(runner, project, directory_tree):
    """Check adding/force-adding ignored files."""
    source_path = directory_tree / ".DS_Store"
    source_path.write_text("ignored-file")
    path = project.path / DATA_DIR / "my-dataset" / directory_tree.name / ".DS_Store"
    relative_path = str(path.relative_to(project.path))

    result = runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Theses paths are ignored" in result.output
    assert str(source_path) in result.output
    assert "OK" in result.output

    dataset = get_dataset_with_injection("my-dataset")

    assert dataset.find_file(relative_path) is None

    result = runner.invoke(cli, ["dataset", "add", "--copy", "--force", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Theses paths are ignored" not in result.output
    assert str(source_path) not in result.output
    assert "OK" in result.output

    dataset = get_dataset_with_injection("my-dataset")

    assert dataset.find_file(relative_path) is not None


@pytest.mark.serial
def test_workflow_with_linked_file(runner, project, run, no_lfs_size_limit):
    """Check using linked files in workflows."""
    write_and_commit_file(project.repository, "file1", "file1 content")
    path = project.path / "file1"
    (project.path / "data" / "my-data" / "directory_tree").mkdir(parents=True, exist_ok=True)

    result = runner.invoke(cli, ["dataset", "add", "-c", "-d", "directory_tree", "--link", "my-data", path])
    assert 0 == result.exit_code, format_result_exception(result)

    source = project.path / DATA_DIR / "my-data" / "directory_tree" / "file1"
    output = project.path / DATA_DIR / "output.txt"

    assert 0 == run(args=("run", "wc", "-c"), stdin=source, stdout=output)

    previous_commit = project.repository.get_previous_commit(output)

    # Update linked file
    path.write_text("some updates")
    project.repository.add(all=True)
    project.repository.commit("changes", no_verify=True)

    # Renku status/update follows symlinks when calculating hashes and doesn't respect linked files, so, no dataset
    # update is needed
    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["update", "--all"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    current_commit = project.repository.get_previous_commit(source)
    assert current_commit != previous_commit

    attributes = (project.path / ".gitattributes").read_text().split()
    assert "data/output.txt" in attributes


def test_immutability_for_files(directory_tree, runner, project):
    """Test dataset's ID changes after a change to dataset files."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = get_dataset_with_injection("my-data")

    time.sleep(1)
    # Add some files
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", str(directory_tree)]).exit_code

    dataset = get_dataset_with_injection("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    time.sleep(1)
    # Add the same files again; it should mutate because files addition dates change
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "--overwrite", str(directory_tree)]).exit_code
    )

    dataset = get_dataset_with_injection("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    time.sleep(1)
    # Remove some files
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "-I", "file1", "--yes"]).exit_code

    dataset = get_dataset_with_injection("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_for_adding_files_twice(directory_tree, runner, project):
    """Test dataset's ID does not change changes if the same files are added again."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "--create", str(directory_tree)]).exit_code
    old_dataset = get_dataset_with_injection("my-data")

    assert 1 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", str(directory_tree)]).exit_code
    dataset = get_dataset_with_injection("my-data")

    assert old_dataset.id == dataset.id


def test_datasets_provenance_after_create(runner, project):
    """Test datasets provenance is updated after creating a dataset."""
    args = [
        "dataset",
        "create",
        "my-data",
        "--title",
        "Long Title",
        "--description",
        "some description here",
        "-c",
        "John Doe <john.doe@mail.ch>",
        "-c",
        "John Smiths<john.smiths@mail.ch>",
        "-k",
        "keyword-1",
        "-k",
        "keyword-2",
    ]
    assert 0 == runner.invoke(cli, args, catch_exceptions=False).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-data")

    assert "Long Title" == dataset.title
    assert "my-data" == dataset.name
    assert "some description here" == dataset.description
    assert "John Doe" in [c.name for c in dataset.creators]
    assert "john.doe@mail.ch" in [c.email for c in dataset.creators]
    assert "John Smiths" in [c.name for c in dataset.creators]
    assert "john.smiths@mail.ch" in [c.email for c in dataset.creators]
    assert {"keyword-1", "keyword-2"} == set(dataset.keywords)
    assert dataset.initial_identifier == dataset.identifier
    assert dataset.derived_from is None
    assert dataset.same_as is None
    assert [] == dataset.dataset_files

    assert not project.repository.is_dirty()


def test_datasets_provenance_after_create_when_adding(runner, project):
    """Test datasets provenance is updated after creating a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "--create", "my-data", "README.md"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-data")

    assert dataset.initial_identifier == dataset.identifier
    assert dataset.derived_from is None
    assert dataset.same_as is None
    assert {"README.md"} == {Path(f.entity.path).name for f in dataset.dataset_files}

    assert not project.repository.is_dirty()


def test_datasets_provenance_after_edit(runner, project):
    """Test datasets provenance is updated after editing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"], catch_exceptions=False).exit_code

    dataset = get_dataset_with_injection("my-data")

    with get_datasets_provenance_with_injection() as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_previous_version(current_version)

    assert_dataset_is_mutated(old=old_version, new=dataset)
    assert dataset.identifier == current_version.identifier
    assert old_version.initial_identifier == old_version.identifier
    assert set() == set(old_version.keywords)
    assert {"new-data"} == set(current_version.keywords)


def test_datasets_provenance_after_add(runner, project, directory_tree):
    """Test datasets provenance is updated after adding data to a dataset."""
    assert (
        0
        == runner.invoke(
            cli, ["dataset", "add", "--copy", "my-data", "--create", str(directory_tree / "file1")]
        ).exit_code
    )

    with get_datasets_provenance_with_injection() as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-data")

    path = os.path.join(DATA_DIR, "my-data", "file1")
    file = dataset.find_file(path)
    object_hash = project.repository.get_object_hash(path=path)

    assert object_hash in file.entity.id
    assert path in file.entity.id
    assert object_hash == file.entity.checksum
    assert path == file.entity.path


def test_datasets_provenance_after_multiple_adds(runner, project, directory_tree):
    """Test datasets provenance is re-using DatasetFile objects after multiple adds."""
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "-c", str(directory_tree / "dir1")]).exit_code
    )

    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", str(directory_tree / "file1")]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        provenance = datasets_provenance.get_provenance_tails()

        assert 1 == len(provenance)

        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_by_id(current_version.derived_from.url_id)

    old_dataset_file_ids = {f.id for f in old_version.files}

    path = os.path.join(DATA_DIR, "my-data", "dir1", "file2")
    file2 = current_version.find_file(path)

    assert file2.id in old_dataset_file_ids


def test_datasets_provenance_after_add_with_overwrite(runner, project, directory_tree):
    """Test datasets provenance is updated if adding and overwriting same files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "--create", str(directory_tree)]).exit_code
    time.sleep(1)
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "--overwrite", str(directory_tree)]).exit_code
    )

    with get_datasets_provenance_with_injection() as datasets_provenance:
        provenance = datasets_provenance.get_provenance_tails()

        assert 1 == len(provenance)

        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_by_id(current_version.derived_from.url_id)
    old_dataset_file_ids = {f.id for f in old_version.files}

    for dataset_file in current_version.files:
        assert not dataset_file.is_removed()
        # NOTE: DatasetFile should be recreated when adding the same file with the `--overwrite` option
        assert dataset_file.id not in old_dataset_file_ids


def test_datasets_provenance_after_file_unlink(runner, project, directory_tree):
    """Test datasets provenance is updated after removing data."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "-c", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code

    dataset = get_dataset_with_injection("my-data")
    with get_datasets_provenance_with_injection() as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_by_id(Dataset.generate_id(dataset.initial_identifier))
    path = os.path.join(DATA_DIR, "my-data", directory_tree.name, "file1")

    assert 3 == len(old_version.dataset_files)
    assert 3 == len(old_version.files)
    # NOTE: Files are not removed from the list but they are marked as deleted
    assert 3 == len(current_version.dataset_files)
    assert 1 == len(current_version.files)
    assert {path} == {f.entity.path for f in current_version.files}
    assert current_version.identifier != current_version.initial_identifier


def test_datasets_provenance_after_remove(runner, project, directory_tree):
    """Test datasets provenance is updated after removing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "-c", str(directory_tree)]).exit_code

    dataset = get_dataset_with_injection("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "rm", "my-data"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")
        provenance = datasets_provenance.get_provenance_tails()

    assert current_version is None
    # NOTE: We only keep the tail of provenance chain for each dataset in the provenance
    assert 1 == len(provenance)

    last_version = next(d for d in provenance)

    assert last_version.is_removed() is True
    assert_dataset_is_mutated(old=dataset, new=last_version)


def test_datasets_provenance_after_adding_tag(runner, project):
    """Test datasets provenance is updated after tagging a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = get_dataset_with_injection("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        provenance = datasets_provenance.get_provenance_tails()
        current_version = datasets_provenance.get_by_name("my-data")

    assert 1 == len(provenance)
    assert current_version.identifier == current_version.initial_identifier
    assert current_version.derived_from is None
    assert current_version.identifier == old_dataset.identifier
    assert not project.repository.is_dirty()


def test_datasets_provenance_after_removing_tag(runner, project):
    """Test datasets provenance is updated after removing a dataset's tag."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    old_dataset = get_dataset_with_injection("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "rm-tags", "my-data", "42.0"]).exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        provenance = datasets_provenance.get_provenance_tails()
        current_version = datasets_provenance.get_by_name("my-data")

    assert 1 == len(provenance)
    assert current_version.identifier == current_version.initial_identifier
    assert current_version.derived_from is None
    assert current_version.identifier == old_dataset.identifier
    assert not project.repository.is_dirty()


def test_datasets_provenance_multiple(runner, project, directory_tree):
    """Test datasets provenance is updated after multiple dataset operations."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    v1 = get_dataset_with_injection("my-data")
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code

    with get_datasets_provenance_with_injection() as datasets_provenance:
        tail_dataset = datasets_provenance.get_by_name("my-data", immutable=True)
        provenance = datasets_provenance.get_provenance_tails()

        # NOTE: We only keep the tail of provenance chain for each dataset in the provenance
        assert 1 == len(provenance)
        assert tail_dataset is provenance[0]

        assert v1.identifier == tail_dataset.initial_identifier
        tail_dataset = datasets_provenance.get_previous_version(tail_dataset)
        assert v1.identifier == tail_dataset.initial_identifier
        tail_dataset = datasets_provenance.get_previous_version(tail_dataset)
        assert v1.identifier == tail_dataset.initial_identifier
        tail_dataset = datasets_provenance.get_previous_version(tail_dataset)
        assert v1.identifier == tail_dataset.initial_identifier


def test_datasets_provenance_add_file(runner, project, directory_tree):
    """Test add to dataset using graph command."""
    file1 = str(directory_tree.joinpath("file1"))
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "--create", "my-data", file1]).exit_code
    dir1 = str(directory_tree.joinpath("dir1"))
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", dir1]).exit_code

    dataset = get_dataset_with_injection("my-data")

    assert {"file1", "file2", "file3"} == {Path(f.entity.path).name for f in dataset.files}


def test_immutability_of_dataset_files(runner, project, directory_tree):
    """Test DatasetFiles are generated when their Entity changes."""
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", "-c", str(directory_tree / "file1")]).exit_code
    )

    file1 = os.path.join(DATA_DIR, "my-data", "file1")

    v1 = get_dataset_with_injection("my-data").find_file(file1)

    # DatasetFile changes when Entity is changed
    write_and_commit_file(project.repository, file1, "changed content", commit=False)
    assert 0 == runner.invoke(cli, ["dataset", "update", "--all"]).exit_code
    v2 = get_dataset_with_injection("my-data").find_file(file1)

    assert v1.id != v2.id

    # DatasetFile doesn't change when Entity is unchanged
    assert (
        0
        == runner.invoke(cli, ["dataset", "add", "--copy", "my-data", str(directory_tree / "dir1" / "file2")]).exit_code
    )
    v3 = get_dataset_with_injection("my-data").find_file(file1)

    assert v2.id == v3.id

    # DatasetFile changes when Entity is unchanged but is overwritten
    assert (
        0
        == runner.invoke(
            cli, ["dataset", "add", "--copy", "my-data", "--overwrite", str(directory_tree / "file1")]
        ).exit_code
    )
    v4 = get_dataset_with_injection("my-data").find_file(file1)

    assert v3.id != v4.id

    # DatasetFile changes if the file is removed
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "file1"], input="y").exit_code
    dataset = get_dataset_with_injection("my-data")
    v5 = next(f for f in dataset.dataset_files if f.is_removed())

    assert "file1" in v5.entity.path
    assert v4.id != v5.id


@pytest.mark.serial
def test_unauthorized_import(mock_kg, runner, project):
    """Test importing without a valid token."""
    set_value("http", "renku.ch", "not-renku-token", global_only=True)

    result = runner.invoke(
        cli, ["dataset", "import", "https://renku.ch/projects/user/project-name/datasets/123"], catch_exceptions=False
    )

    assert 1 == result.exit_code
    assert "Unauthorized access to knowledge graph" in result.output
    assert "renku login renku.ch" in result.output


@pytest.mark.serial
def test_authorized_import(mock_kg, runner, project):
    """Test importing with a valid token.

    NOTE: Returning 404 from KG means that the request was authorized. We don't implement a full import due to mocking
    complexity.
    """
    set_value("http", "renku.ch", "renku-token", global_only=True)

    result = runner.invoke(cli, ["dataset", "import", "https://renku.ch/projects/user/project-name/datasets/123"])

    assert 1 == result.exit_code
    assert "Unauthorized access to knowledge graph" not in result.output
    assert "Cannot find project in the knowledge graph" in result.output


@pytest.mark.parametrize("datadir_option,datadir", [([], f"{DATA_DIR}/my-data"), (["--datadir", "my-dir"], "my-dir")])
def test_update_local_file(runner, project, directory_tree, datadir_option, datadir):
    """Check updating local files."""
    assert (
        0
        == runner.invoke(
            cli, ["dataset", "add", "--copy", "-c", "my-data", str(directory_tree)] + datadir_option
        ).exit_code
    )

    file1 = Path(datadir) / directory_tree.name / "file1"
    file1.write_text("some updates")
    new_checksum_file1 = project.repository.get_object_hash(file1)

    file2 = Path(datadir) / directory_tree.name / "dir1" / "file2"
    file2.write_text("some updates")
    new_checksum_file2 = project.repository.get_object_hash(file2)

    commit_sha_before_update = project.repository.head.commit.hexsha

    old_dataset = get_dataset_with_injection("my-data")

    assert new_checksum_file1 != old_dataset.find_file(file1).entity.checksum
    assert new_checksum_file2 != old_dataset.find_file(file2).entity.checksum

    # NOTE: Update dry run
    result = runner.invoke(cli, ["dataset", "update", "my-data", "--dry-run"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "The following files will be updated" in result.output
    assert "The following files will be deleted" not in result.output
    assert str(file1) in result.output
    assert str(file2) in result.output
    assert commit_sha_before_update == project.repository.head.commit.hexsha
    assert project.repository.is_dirty()

    result = runner.invoke(cli, ["dataset", "update", "my-data", "--no-local"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert commit_sha_before_update == project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "update", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not project.repository.is_dirty()
    dataset = get_dataset_with_injection("my-data")
    assert new_checksum_file1 == dataset.find_file(file1).entity.checksum
    assert new_checksum_file2 == dataset.find_file(file2).entity.checksum
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


@pytest.mark.parametrize("datadir_option,datadir", [([], f"{DATA_DIR}/my-data"), (["--datadir", "my-dir"], "my-dir")])
def test_update_local_file_in_datadir(runner, project, directory_tree, datadir_option, datadir):
    """Check updating local files dropped in the datadir."""
    assert (
        0
        == runner.invoke(
            cli, ["dataset", "add", "--copy", "-c", "my-data", str(directory_tree)] + datadir_option
        ).exit_code
    )

    file1 = Path(datadir) / "some_new_file"
    file1.write_text("some updates")
    folder = Path(datadir) / "folder"
    folder.mkdir()
    file2 = folder / "another_new_file"
    file2.write_text("some updates")

    old_dataset = get_dataset_with_injection("my-data")

    # NOTE: Update dry run
    result = runner.invoke(cli, ["dataset", "update", "my-data", "--dry-run", "--check-data-directory", "--no-remote"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "The following files will be updated" in result.output
    assert "The following files will be deleted" not in result.output
    assert str(file1) in result.output
    assert str(file2) in result.output

    assert project.repository.is_dirty()

    result = runner.invoke(cli, ["dataset", "update", "my-data", "--check-data-directory", "--no-remote"])

    assert 0 == result.exit_code, format_result_exception(result)

    assert not project.repository.is_dirty()
    dataset = get_dataset_with_injection("my-data")
    assert dataset.find_file(file1)
    assert dataset.find_file(file2)
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_update_local_deleted_file(runner, project, directory_tree):
    """Check updating local deleted files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--copy", "-c", "my-data", str(directory_tree)]).exit_code

    file1 = Path(DATA_DIR) / "my-data" / directory_tree.name / "file1"
    file1.unlink()
    project.repository.add(all=True)
    project.repository.commit("deleted file1")
    commit_sha_after_file1_delete = project.repository.head.commit.hexsha

    # NOTE: Update dry run
    result = runner.invoke(cli, ["dataset", "update", "--all", "--dry-run"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "The following files will be updated" not in result.output
    assert "The following files will be deleted" in result.output
    assert str(file1) in result.output
    assert commit_sha_after_file1_delete == project.repository.head.commit.hexsha
    assert not project.repository.is_dirty()

    # NOTE: Update without `--delete`
    result = runner.invoke(cli, ["dataset", "update", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Some files are deleted:" in result.output
    assert "Updated 0 files" in result.output
    assert commit_sha_after_file1_delete == project.repository.head.commit.hexsha
    old_dataset = get_dataset_with_injection("my-data")
    assert old_dataset.find_file(file1)

    # NOTE: Update with `--delete`
    result = runner.invoke(cli, ["dataset", "update", "--delete", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Updated 0 files and deleted 1 files" in result.output
    assert commit_sha_after_file1_delete != project.repository.head.commit.hexsha
    dataset = get_dataset_with_injection("my-data")
    assert dataset.find_file(file1) is None
    assert_dataset_is_mutated(old=old_dataset, new=dataset)

    result = runner.invoke(cli, ["dataset", "update", "--delete", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Updated 0 files and deleted 0 files" in result.output


def test_update_with_no_dataset(runner, project):
    """Check updating a project with no dataset should not raise an error."""
    result = runner.invoke(cli, ["dataset", "update", "--all"])

    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize(
    "uri", ["s3://s3.endpoint/bucket/path", "azure://renkupythontest1/test-private-1", "/local/file/storage"]
)
def test_add_local_data_to_cloud_datasets(runner, project, mocker, directory_tree, uri):
    """Test adding local data to a dataset with cloud storage backend."""
    storage_factory = mocker.patch("renku.infrastructure.storage.factory.StorageFactory.get_storage", autospec=True)
    cloud_storage = storage_factory.return_value

    cloud_storage.upload.return_value = []

    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", uri])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "cloud-data", "--copy", directory_tree], input="\n\n\n")

    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("cloud-data")
    files_uri = [f"{uri}/directory_tree/file1", f"{uri}/directory_tree/dir1/file2", f"{uri}/directory_tree/dir1/file3"]

    assert 3 == len(dataset.files)
    assert set(files_uri) == {f.based_on.url for f in dataset.files}

    assert {
        "9d98eede4ccb193e379d6dbd7cc1eb86",
        "7bec9352114f8139c2640b2554563508",
        "dacb3fd4bbd9ab5a17e2f7686b90c1d2",
    } == {f.based_on.checksum for f in dataset.files}

    calls = [
        call(source=directory_tree / "file1", uri=files_uri[0]),
        call(source=directory_tree / "dir1" / "file2", uri=files_uri[1]),
        call(source=directory_tree / "dir1" / "file3", uri=files_uri[2]),
    ]

    cloud_storage.upload.assert_has_calls(calls=calls, any_order=True)


@pytest.mark.parametrize("uri", ["s3://s3.endpoint/bucket/", "azure://renkupythontest1/test-private-1"])
def test_dataset_update_remote_file(runner, project, mocker, uri):
    """Test updating a file added from remote/cloud storage."""
    storage_factory = mocker.patch("renku.infrastructure.storage.factory.StorageFactory.get_storage", autospec=True)
    cloud_storage = storage_factory.return_value

    uri = f"{uri}/path/myfile"

    def _fake_download(uri, destination):
        with open(destination, "w") as f:
            f.write("a")

    cloud_storage.get_hashes.return_value = [FileHash(uri=uri, path="path/myfile", size=5, hash="deadbeef")]
    cloud_storage.download.side_effect = _fake_download

    result = runner.invoke(cli, ["dataset", "create", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "local-data", uri])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].based_on.url == uri
    assert dataset.files[0].based_on.checksum == "deadbeef"

    # Updating without changes does nothing
    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].based_on.url == uri
    assert dataset.files[0].based_on.checksum == "deadbeef"

    # Updating with changes works
    def _fake_download2(uri, destination):
        with open(destination, "w") as f:
            f.write("b")

    cloud_storage.get_hashes.return_value = [FileHash(uri=uri, path="path/myfile", size=7, hash="8badf00d")]
    cloud_storage.download.side_effect = _fake_download2

    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].based_on.url == uri
    assert dataset.files[0].based_on.checksum == "8badf00d"

    cloud_storage.get_hashes.return_value = []

    # check deletion doesn't happen without --delete
    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)

    # check deletion
    result = runner.invoke(cli, ["dataset", "update", "local-data", "--delete"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 0 == len(dataset.files)


def test_dataset_update_web_file(runner, project, mocker):
    """Test updating a file added from remote/cloud storage."""

    uri = "http://www.example.com/myfile.txt"

    cache = project.path / ".renku" / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    new_file = cache / "myfile.txt"
    new_file.write_text("output")

    mocker.patch("renku.core.util.requests.get_redirect_url", lambda _: uri)
    mocker.patch(
        "renku.core.util.requests.download_file",
        lambda base_directory, url, filename, extract: (cache, [Path(new_file)]),
    )

    result = runner.invoke(cli, ["dataset", "create", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "local-data", uri])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].source == uri
    assert dataset.files[0].entity.checksum == "6caf68aff423350af0ef7b148fec2ed4243658e5"

    # Updating without changes does nothing
    new_file.write_text("output")

    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].source == uri
    assert dataset.files[0].entity.checksum == "6caf68aff423350af0ef7b148fec2ed4243658e5"

    # Updating with changes works
    new_file.write_text("output2")

    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)
    assert dataset.files[0].source == uri
    assert dataset.files[0].entity.checksum == "1bc6411450b62581e5cea1174c15269c249dd4ea"

    # check deletion doesn't happen without --delete
    def _fake_raise(base_directory, url, filename, extract):
        raise errors.RequestError

    mocker.patch("renku.core.util.requests.download_file", _fake_raise)

    result = runner.invoke(cli, ["dataset", "update", "local-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 1 == len(dataset.files)

    # check deletion
    result = runner.invoke(cli, ["dataset", "update", "local-data", "--delete"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = get_dataset_with_injection("local-data")

    assert 0 == len(dataset.files)


@pytest.mark.parametrize(
    "storage", ["s3://s3.endpoint/bucket/path", "azure://renkupythontest1/test-private-1", "/local/file/storage"]
)
def test_unmounting_dataset(runner, project, mocker, storage):
    """Test unmounting a not-mounted dataset doesn't raise errors."""
    storage_factory = mocker.patch("renku.infrastructure.storage.factory.StorageFactory.get_storage", autospec=True)
    cloud_storage = storage_factory.return_value

    cloud_storage.upload.return_value = []

    result = runner.invoke(cli, ["dataset", "create", "cloud-data", "--storage", storage])

    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "unmount", "cloud-data"])

    assert 0 == result.exit_code, format_result_exception(result)
