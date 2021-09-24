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
"""Test ``dataset`` command."""

import json
import os
import shutil
import textwrap
from pathlib import Path

import git
import pytest

from renku.cli import cli
from renku.core.commands.format.dataset_files import DATASET_FILES_COLUMNS, DATASET_FILES_FORMATS
from renku.core.commands.format.datasets import DATASETS_COLUMNS, DATASETS_FORMATS
from renku.core.commands.providers import DataverseProvider, ProviderFactory, ZenodoProvider
from renku.core.management.config import RENKU_HOME
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.dataset import Dataset
from renku.core.models.refs import LinkReference
from renku.core.utils.git import get_object_hash
from renku.core.utils.urls import get_slug
from tests.utils import assert_dataset_is_mutated, format_result_exception, write_and_commit_file


def test_datasets_create_clean(runner, project, client, load_dataset_with_injection):
    """Test creating a dataset in clean repository."""
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = load_dataset_with_injection("dataset", client)
    assert isinstance(dataset, Dataset)

    staged = client.repo.index.diff("HEAD")
    for file_path in staged:
        assert ".renku" not in file_path

    untracked = client.repo.untracked_files
    for file_path in untracked:
        assert ".renku" not in file_path


def test_dataset_show(runner, client, subdirectory):
    """Test creating a dataset with metadata."""
    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = client.path / "metadata.json"
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
        ],
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


def test_datasets_create_different_names(runner, client):
    """Test creating datasets with same title but different name."""
    result = runner.invoke(cli, ["dataset", "create", "dataset-1", "--title", "title"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "dataset-2", "--title", "title"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output


def test_datasets_create_with_same_name(runner, client):
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
def test_datasets_invalid_name(runner, client, name):
    """Test creating datasets with invalid name."""
    result = runner.invoke(cli, ["dataset", "create", name])

    assert 2 == result.exit_code
    assert f'Dataset name "{name}" is not valid' in result.output
    assert f'Hint: "{get_slug(name)}" is valid' in result.output


def test_datasets_create_dirty(runner, project, client, load_dataset_with_injection):
    """Test creating a dataset in dirty repository."""
    # Create a file in root of the repository.
    with (client.path / "a").open("w") as fp:
        fp.write("a")

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = load_dataset_with_injection("dataset", client)
    assert dataset

    staged = client.repo.index.diff("HEAD")
    for file_path in staged:
        assert ".renku" not in file_path

    untracked = client.repo.untracked_files
    for file_path in untracked:
        assert ".renku" not in file_path


def test_datasets_create_dirty_exception_untracked(runner, project, client):
    """Test exception raise for untracked file in renku directory."""
    # 1. Create a problem.
    datasets_dir = client.path / RENKU_HOME / client.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 2. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_datasets_create_dirty_exception_staged(runner, project, client):
    """Test exception raise for staged file in renku directory."""
    # 1. Create a problem within .renku directory
    datasets_dir = client.path / RENKU_HOME / client.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 2. Stage a problem without committing it.
    client.repo.git.add(datasets_dir / "a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_dataset_create_dirty_exception_all_untracked(runner, project, client):
    """Test exception raise for all untracked files."""
    # 1. Create unclean root to enforce ensure checks.
    with (client.path / "a").open("w") as fp:
        fp.write("a")

    # 2. Create a problem.
    datasets_dir = client.path / RENKU_HOME / client.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_datasets_create_dirty_exception_all_staged(runner, project, client):
    """Test exception raise for all staged files."""
    # 1. Create unclean root to enforce ensure checks.
    with (client.path / "a").open("w") as fp:
        fp.write("a")

    client.repo.git.add("a")

    # 2. Create a problem.
    datasets_dir = client.path / RENKU_HOME / client.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    client.repo.git.add(datasets_dir / "a")

    # 3. Ensure correct error has been raised.
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert ".renku contains uncommitted changes." in result.output


def test_dataset_create_exception_refs(runner, project, client):
    """Test untracked/unstaged exception raise in dirty renku home dir."""
    with (client.path / "a").open("w") as fp:
        fp.write("a")

    datasets_dir = client.path / RENKU_HOME / client.database_path
    if not datasets_dir.exists():
        datasets_dir.mkdir()

    with (datasets_dir / "a").open("w") as fp:
        fp.write("a")

    refs_dir = client.path / RENKU_HOME / LinkReference.REFS
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
def test_dataset_creator_is_invalid(client, runner, creator, field):
    """Test create dataset with invalid creator format."""
    result = runner.invoke(cli, ["dataset", "create", "ds", "-c", creator])
    assert 2 == result.exit_code
    assert field + " is invalid" in result.output


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
def test_datasets_list_empty(output_format, runner, project):
    """Test listing without datasets."""
    format_option = "--format={0}".format(output_format)
    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
def test_datasets_list_non_empty(output_format, runner, project):
    """Test listing with datasets."""
    format_option = "--format={0}".format(output_format)
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my-dataset" in result.output


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


def test_add_and_create_dataset(directory_tree, runner, project, client, subdirectory, load_dataset_with_injection):
    """Test add data to a non-existing dataset."""
    result = runner.invoke(cli, ["dataset", "add", "new-dataset", str(directory_tree)], catch_exceptions=False)
    assert 1 == result.exit_code
    assert 'Dataset "new-dataset" does not exist.' in result.output

    # Add succeeds with --create
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    path1 = os.path.join(client.path, DATA_DIR, "new-dataset", directory_tree.name, "file1")
    path2 = os.path.join(client.path, DATA_DIR, "new-dataset", directory_tree.name, "dir1", "file2")
    path3 = os.path.join(client.path, DATA_DIR, "new-dataset", directory_tree.name, "dir1", "file3")

    assert os.stat(path1)
    assert os.stat(path2)
    assert os.stat(path3)
    dataset = load_dataset_with_injection("new-dataset", client)
    assert {os.path.relpath(p, client.path) for p in [path1, path2, path3]} == {f.entity.path for f in dataset.files}

    # Further, add with --create fails
    result = runner.invoke(cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)])
    assert 1 == result.exit_code


def test_add_and_create_dataset_with_lfs_warning(directory_tree, runner, project, client_with_lfs_warning):
    """Test add data with lfs warning."""

    # Add succeeds with --create
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Adding these files to Git LFS" in result.output
    assert "dir1/file2" in result.output
    assert "file" in result.output


def test_add_to_dirty_repo(directory_tree, runner, project, client):
    """Test adding to a dataset in a dirty repo commits only added files."""
    with (client.path / "tracked").open("w") as fp:
        fp.write("tracked file")
    client.repo.git.add("*")
    client.repo.index.commit("tracked file")

    with (client.path / "tracked").open("w") as fp:
        fp.write("modified tracked file")
    with (client.path / "untracked").open("w") as fp:
        fp.write("untracked file")

    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    assert client.repo.is_dirty()
    assert ["untracked"] == client.repo.untracked_files

    # Add without making a change
    result = runner.invoke(cli, ["dataset", "add", "new-dataset", str(directory_tree)], catch_exceptions=False)
    assert 1 == result.exit_code

    assert client.repo.is_dirty()
    assert ["untracked"] == client.repo.untracked_files


def test_add_unicode_file(tmpdir, runner, project, client):
    """Test adding files with unicode special characters in their names."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    filename = "filéàèû爱ಠ_ಠ.txt"
    new_file = tmpdir.join(filename)
    new_file.write(str("test"))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert filename in result.output.encode("latin1").decode("unicode-escape")


def test_multiple_file_to_dataset(tmpdir, runner, project, client, load_dataset_with_injection):
    """Test importing multiple data into a dataset at once."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = load_dataset_with_injection("dataset", client)
    assert dataset.title == "dataset"

    paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)


def test_repository_file_to_dataset(runner, client, subdirectory, load_dataset_with_injection):
    """Test adding a file from the repository into a dataset."""
    # create a dataset
    assert 0 == runner.invoke(cli, ["dataset", "create", "dataset"]).exit_code

    a_path = client.path / "a"
    a_path.write_text("a content")

    client.repo.git.add(str(a_path))
    client.repo.git.commit(message="Added file a", no_verify=True)

    result = runner.invoke(cli, ["dataset", "add", "dataset", str(a_path)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = load_dataset_with_injection("dataset", client)
    assert dataset.title == "dataset"
    assert dataset.find_file("a") is not None


def test_relative_import_to_dataset(tmpdir, runner, client, subdirectory, load_dataset_with_injection):
    """Test importing data from a directory structure."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    dataset = load_dataset_with_injection("dataset", client)
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

    result = runner.invoke(cli, ["dataset", "add", "dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    assert os.stat(client.path / DATA_DIR / "dataset" / "zero.txt")
    assert os.stat(client.path / DATA_DIR / "dataset" / "first" / "first.txt")
    assert os.stat(client.path / DATA_DIR / "dataset" / "first" / "second" / "second.txt")


@pytest.mark.parametrize(
    "params,message",
    [
        (["-s", "file", "https://renkulab.io/"], 'Cannot use "--source" with URLs or local files.'),
        (["-s", "file", "/some/local/path"], 'Cannot use "--source" with URLs or local files.'),
    ],
)
def test_usage_error_in_add_from_url(runner, client, params, message):
    """Test user's errors when adding URL/local file to a dataset."""
    result = runner.invoke(cli, ["dataset", "add", "remote", "--create"] + params, catch_exceptions=False)
    assert 2 == result.exit_code
    assert message in result.output


def test_add_from_local_repo_warning(runner, client, data_repository, directory_tree):
    """Test a warning is printed when adding from a local git repo."""
    result = runner.invoke(cli, ["dataset", "add", "dataset", "--create", str(directory_tree)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Use remote's Git URL instead to enable lineage " in result.output


def test_add_data_directory(runner, client, directory_tree):
    """Test adding a dataset's data directory to it prints an error."""
    result = runner.invoke(cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "add", "new-dataset", "data/new-dataset"], catch_exceptions=False)
    assert 2 == result.exit_code
    assert "Cannot add dataset's data directory recursively" in result.output


def test_dataset_add_with_copy(tmpdir, runner, project, client, load_dataset_with_injection):
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
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        original_inodes.append(os.lstat(str(new_file))[stat.ST_INO])
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    received_inodes = []
    dataset = load_dataset_with_injection("my-dataset", client)
    assert dataset.title == "my-dataset"

    for file in dataset.files:
        path = (client.path / file.entity.path).resolve()
        received_inodes.append(os.lstat(path)[stat.ST_INO])

    # check that original inodes are within created ones
    for inode in received_inodes:
        assert inode not in original_inodes


@pytest.mark.serial
def test_dataset_add_many(tmpdir, runner, project, client):
    """Test adding many files to dataset."""

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    paths = []
    for i in range(1000):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    assert len(client.repo.head.commit.message) <= 100


def test_dataset_file_path_from_subdirectory(runner, client, subdirectory, load_dataset_with_injection):
    """Test adding a file into a dataset and check path independent
    of the CWD"""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    a_path = client.path / "a"
    a_path.write_text("a text")

    client.repo.git.add(str(a_path))
    client.repo.git.commit(message="Added file a")

    # add data
    result = runner.invoke(cli, ["dataset", "add", "dataset", str(a_path)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = load_dataset_with_injection("dataset", client)
    file = dataset.find_file("a")
    assert file is not None
    assert "a" == file.entity.path


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
    format_option = "--format={0}".format(output_format)
    result = runner.invoke(cli, ["dataset", "ls-files", format_option])
    assert 0 == result.exit_code, format_result_exception(result)


def test_datasets_ls_files_lfs(tmpdir, large_file, runner, project):
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
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths, catch_exceptions=False)
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


def test_datasets_ls_files_json(tmpdir, large_file, runner, project):
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
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # NOTE: check files
    result = runner.invoke(cli, ["dataset", "ls-files", "--format", "json"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = json.loads(result.output)

    assert len(result) == 2
    file1 = next((f for f in result if f["path"].endswith("file_1")))
    file2 = next((f for f in result if f["path"].endswith(large_file.name)))

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
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

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
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

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
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

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


def test_datasets_ls_files_tabular_creators(runner, client, directory_tree, load_dataset_with_injection):
    """Test listing of data within dataset with creators filters."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code
    creator = load_dataset_with_injection("my-dataset", client).creators[0].name

    assert creator is not None

    # check creators filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--creators={0}".format(creator)])
    assert 0 == result.exit_code, format_result_exception(result)

    # check output
    for file_ in directory_tree.rglob("*file*"):
        assert file_.name in result.output


def test_datasets_ls_files_correct_paths(runner, client, directory_tree):
    """Test listing of data within dataset and check that paths are correct."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--format=json-ld"])
    assert 0 == result.exit_code, format_result_exception(result)

    output = json.loads(result.output)
    for record in output:
        for entity in record:
            path = entity.get("http://www.w3.org/ns/prov#atLocation")
            if path:
                path = path[0]["@value"]
                assert (client.path / path).exists()


def test_datasets_ls_files_with_name(directory_tree, runner, project):
    """Test listing of data within dataset with include/exclude filters."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset", "--title", "Long Title"])
    assert 0 == result.exit_code, format_result_exception(result)

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # list files with name
    result = runner.invoke(cli, ["dataset", "ls-files", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "dir1/file2" in result.output


def test_datasets_ls_files_correct_size(runner, client, directory_tree, large_file):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code

    path = client.path / DATA_DIR / "my-dataset" / "file1"
    shutil.copy(large_file, path)

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns=size, path"])
    assert 0 == result.exit_code, format_result_exception(result)

    line = next(line for line in result.output.split("\n") if "file1" in line)
    size = int(line.split()[0])

    assert 3 == size


@pytest.mark.skip(reason="FIXME: We don't have commit shas for files. What should be listed here?")
def test_datasets_ls_files_correct_commit(runner, client, directory_tree):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code

    commit = client.find_previous_commit(paths=client.path / DATA_DIR / "my-dataset" / "file1")

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

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", "notthere.csv"])

    assert 2 == result.exit_code


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
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)

    # unlink file from dataset
    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename], input="n")
    assert 1 == result.exit_code

    # check output
    assert "Aborted!" in result.output


def test_dataset_unlink_file(tmpdir, runner, client, subdirectory, load_dataset_with_injection):
    """Test unlinking of file and check removal from dataset"""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not client.repo.is_dirty()

    dataset = load_dataset_with_injection("my-dataset", client)
    assert new_file.basename in {Path(f.entity.path).name for f in dataset.files}

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename, "-y"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not client.repo.is_dirty()

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after

    dataset = load_dataset_with_injection("my-dataset", client)

    assert new_file.basename not in [Path(f.entity.path).name for f in dataset.files if not f.is_removed()]


def test_dataset_rm(runner, client, directory_tree, subdirectory, load_dataset_with_injection):
    """Test removal of a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--create", "my-dataset", str(directory_tree)]).exit_code

    assert load_dataset_with_injection("my-dataset", client)

    result = runner.invoke(cli, ["dataset", "rm", "my-dataset"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output
    assert not load_dataset_with_injection("my-dataset", client)

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)


def test_dataset_rm_failure(runner, client):
    """Test errors in removal of a dataset."""
    assert 2 == runner.invoke(cli, ["dataset", "rm"]).exit_code
    assert 1 == runner.invoke(cli, ["dataset", "rm", "does-not-exist"]).exit_code


def test_dataset_overwrite_no_confirm(runner, project):
    """Check dataset overwrite behaviour without confirmation."""
    result = runner.invoke(cli, ["dataset", "create", "rokstar"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "rokstar"])
    assert 1 == result.exit_code
    assert "OK" not in result.output


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit(runner, client, project, dirty, subdirectory, load_dataset_with_injection):
    """Check dataset metadata editing."""
    if dirty:
        (client.path / "README.md").write_text("Make repo dirty.")

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = client.path / "metadata.json"
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

    dataset = load_dataset_with_injection("dataset", client)
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

    dataset = load_dataset_with_injection("dataset", client)
    assert " new description " == dataset.description
    assert "new title" == dataset.title
    assert {creator1, creator2}.issubset({c.full_identity for c in dataset.creators})
    assert {"keyword-2", "keyword-3"} == set(dataset.keywords)
    assert 1 == len(dataset.annotations)
    assert new_metadata == dataset.annotations[0].body


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit_no_change(runner, client, project, dirty):
    """Check metadata editing does not commit when there is no change."""
    result = runner.invoke(cli, ["dataset", "create", "dataset", "-t", "original title"])
    assert 0 == result.exit_code, format_result_exception(result)

    if dirty:
        (client.path / "README.md").write_text("Make repo dirty.")

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["dataset", "edit", "dataset"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Nothing to update." in result.output

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before
    assert dirty is client.repo.is_dirty()


@pytest.mark.parametrize(
    "uri", ["10.5281/zenodo.3363060", "doi:10.5281/zenodo.3363060", "https://zenodo.org/record/3363060"]
)
def test_dataset_provider_resolution_zenodo(doi_responses, uri):
    """Check that zenodo uris resolve to ZenodoProvider."""
    provider, _ = ProviderFactory.from_uri(uri)
    assert type(provider) is ZenodoProvider


@pytest.mark.parametrize(
    "uri",
    [
        "10.7910/DVN/TJCLKP",
        "doi:10.7910/DVN/TJCLKP",
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/TJCLKP",
    ],
)
def test_dataset_provider_resolution_dataverse(doi_responses, uri):
    """Check that dataverse URIs resolve to ``DataverseProvider``."""
    provider, _ = ProviderFactory.from_uri(uri)
    assert type(provider) is DataverseProvider


def test_dataset_tag(tmpdir, runner, client, subdirectory, get_datasets_provenance_with_injection):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli, ["dataset", "tag", "my-dataset", "A", "-d", "short descriptiön"], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-dataset")
        all_tags = datasets_provenance.get_all_tags(dataset)
        assert {dataset.id} == {t.dataset_id.value for t in all_tags}


@pytest.mark.parametrize("form", ["tabular", "json-ld"])
def test_dataset_ls_tags(tmpdir, runner, project, client, form, load_dataset_with_injection):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id1 = load_dataset_with_injection("my-dataset", client).id

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0", "-d", "first tag!"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id2 = load_dataset_with_injection("my-dataset", client).id

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(
        cli, ["dataset", "ls-tags", "my-dataset", "--format={}".format(form)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "1.0" in result.output
    assert "aBc9.34-11_55.t" in result.output
    assert "first tag!" in result.output
    assert id1 in result.output
    assert id2 in result.output


def test_dataset_rm_tag(tmpdir, runner, client, subdirectory, load_dataset_with_injection):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    id1 = load_dataset_with_injection("my-dataset", client).id

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


def test_dataset_rm_tags_multiple(tmpdir, runner, project, client):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False)
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


def test_dataset_rm_tags_failure(tmpdir, runner, project, client):
    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False)

    assert 1 == result.exit_code
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False)
    assert 2 == result.exit_code


def test_dataset_clean_up_when_add_fails(runner, client, subdirectory):
    """Test project is cleaned when dataset add fails for a new dataset."""
    # add a non-existing path to a new dataset
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", "non-existing-file"], catch_exceptions=True
    )

    assert 2 == result.exit_code
    ref = client.renku_path / "refs" / "datasets" / "new-dataset"
    assert not ref.is_symlink() and not ref.exists()


def test_avoid_empty_commits(runner, client, directory_tree):
    """Test no empty commit is created when adding existing data."""
    runner.invoke(cli, ["dataset", "create", "my-dataset"])

    commit_sha_before = client.repo.head.object.hexsha
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after

    commit_sha_before = commit_sha_after
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)])
    assert 1 == result.exit_code
    assert "Error: There is nothing to commit." in result.output

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before == commit_sha_after


def test_multiple_dataset_commits(runner, client, directory_tree):
    """Check adding existing data to multiple datasets."""
    commit_sha_before = client.repo.head.object.hexsha
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset1", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after

    commit_sha_before = commit_sha_after
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset2", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after


@pytest.mark.parametrize("filename", [".renku", ".renku/", "Dockerfile"])
def test_add_protected_file(runner, client, filename, subdirectory):
    """Check adding a protected file."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset1", str(client.path / filename)])

    assert 1 == result.exit_code
    assert "Error: The following paths are protected" in result.output


@pytest.mark.parametrize("filename", [".renkunotactuallyrenku", "thisisnot.renku"])
def test_add_nonprotected_file(runner, client, tmpdir, filename, subdirectory):
    """Check adding an 'almost' protected file."""
    new_file = tmpdir.join(filename)
    new_file.write(str("test"))

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset1", str(new_file)])

    assert 0 == result.exit_code, format_result_exception(result)


def test_add_removes_local_path_information(runner, client, directory_tree, load_dataset_with_injection):
    """Test added local paths are stored as relative path."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = load_dataset_with_injection("my-dataset", client)
    relative_path = os.path.relpath(directory_tree, client.path)
    for file in dataset.files:
        assert file.source.startswith(relative_path)
        assert file.source.endswith(Path(file.entity.path).name)


def test_pull_data_from_lfs(runner, client, tmpdir, subdirectory, no_lfs_size_limit):
    """Test pulling data from LFS using relative paths."""
    data = tmpdir.join("data.txt")
    data.write("DATA")

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(data)])
    assert 0 == result.exit_code, format_result_exception(result)
    attributes = (client.path / ".gitattributes").read_text().split()
    assert "data/my-data/data.txt" in attributes

    path = client.path / DATA_DIR / "my-data" / "data.txt"
    relative_path = os.path.relpath(path, os.getcwd())

    result = runner.invoke(cli, ["storage", "pull", relative_path])
    assert 0 == result.exit_code, format_result_exception(result)


def test_lfs_hook(client, subdirectory, large_file):
    """Test committing large files to Git."""
    filenames = {"large-file", "large file with whitespace", "large*file?with wildcards"}

    for filename in filenames:
        shutil.copy(large_file, client.path / filename)
    client.repo.git.add("--all")

    # Commit fails when file is not tracked in LFS
    with pytest.raises(git.exc.HookExecutionError) as e:
        client.repo.index.commit("large files not in LFS")

    assert "You are trying to commit large files to Git" in e.value.stdout
    for filename in filenames:
        assert filename in e.value.stdout

    # Can be committed after being tracked in LFS
    client.track_paths_in_storage(*filenames)
    client.repo.git.add("--all")
    commit = client.repo.index.commit("large files tracked")
    assert "large files tracked" == commit.message

    tracked_lfs_files = set(client.repo.git.lfs("ls-files", "--name-only").split("\n"))
    assert filenames == tracked_lfs_files


@pytest.mark.parametrize("use_env_var", [False, True])
def test_lfs_hook_autocommit(runner, client, subdirectory, large_file, use_env_var):
    """Test committing large files to Git gets automatically added to lfs."""
    if use_env_var:
        os.environ["AUTOCOMMIT_LFS"] = "true"
    else:
        assert 0 == runner.invoke(cli, ["config", "set", "autocommit_lfs", "true"]).exit_code

    filenames = {"large-file", "large file with whitespace", "large*file?with wildcards"}

    for filename in filenames:
        shutil.copy(large_file, client.path / filename)
    client.repo.git.add("--all")

    result = client.repo.git.commit(
        message="large files not in LFS",
        with_extended_output=True,
        env={"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"},
    )
    for filename in filenames:
        assert filename in result[1]
    assert ".gitattributes" in result[1]
    assert "You are trying to commit large files to Git instead of Git-LFS" in result[2]
    assert "Adding files to LFS" in result[2]
    for filename in filenames:
        assert f'Tracking "{filename}"' in result[2]
    assert len(client.dirty_paths) == 0  # NOTE: make sure repo is clean

    tracked_lfs_files = set(client.repo.git.lfs("ls-files", "--name-only").split("\n"))
    assert filenames == tracked_lfs_files


def test_lfs_hook_can_be_avoided(runner, project, subdirectory, large_file):
    """Test committing large files to Git."""
    result = runner.invoke(cli, ["--no-external-storage", "dataset", "add", "-c", "my-dataset", str(large_file)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output


@pytest.mark.parametrize("external", [False, True])
def test_add_existing_files(runner, client, directory_tree, external, no_lfs_size_limit, load_dataset_with_injection):
    """Check adding/overwriting existing files."""
    param = ["-e"] if external else []

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)] + param)

    assert 0 == result.exit_code, format_result_exception(result)

    path = Path(DATA_DIR) / "my-dataset" / directory_tree.name / "file1"

    dataset = load_dataset_with_injection("my-dataset", client)
    assert dataset.find_file(path) is not None

    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)] + param)
    assert 1 == result.exit_code
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "Warning: No new file was added to project" in result.output
    assert "Error: There is nothing to commit." in result.output

    result = runner.invoke(cli, ["dataset", "add", "--overwrite", "my-dataset", str(directory_tree)] + param)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" not in result.output
    assert str(path) not in result.output
    assert external or "Warning: No new file was added to project" in result.output
    assert "Error: There is nothing to commit." not in result.output  # dataset metadata is always updated


@pytest.mark.parametrize("external", [False, True])
def test_add_existing_and_new_files(runner, client, directory_tree, external):
    """Check adding/overwriting existing files."""
    param = ["-e"] if external else []

    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)] + param).exit_code

    path = Path(DATA_DIR) / "my-dataset" / directory_tree.name / "file1"

    # Add existing files and files within same project
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree), "README.md"] + param)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output

    # Add existing and non-existing files
    directory_tree.joinpath("new-file").write_text("new-file")

    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)] + param)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "OK" in result.output


def test_add_existing_files_updates_metadata(runner, client, large_file, load_dataset_with_injection):
    """Check overwriting existing files updates their metadata."""
    # assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "--create", large_file]).exit_code
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", "--create", str(large_file)])
    assert result.exit_code == 0, result.output

    path = Path(DATA_DIR) / "my-dataset" / large_file.name

    before = load_dataset_with_injection("my-dataset", client).find_file(path)

    large_file.write_text("New modified content.")

    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "--overwrite", str(large_file)]).exit_code

    after = load_dataset_with_injection("my-dataset", client).find_file(path)
    assert before.id != after.id
    assert before.date_added != after.date_added
    assert before.entity.checksum != after.entity.checksum
    assert before.entity.path == after.entity.path
    assert before.source == after.source


def test_add_ignored_files(runner, client, directory_tree, load_dataset_with_injection):
    """Check adding/force-adding ignored files."""
    source_path = directory_tree / ".DS_Store"
    source_path.write_text("ignored-file")
    path = client.path / DATA_DIR / "my-dataset" / directory_tree.name / ".DS_Store"
    relative_path = str(path.relative_to(client.path))

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Theses paths are ignored" in result.output
    assert str(source_path) in result.output
    assert "OK" in result.output

    dataset = load_dataset_with_injection("my-dataset", client)

    assert dataset.find_file(relative_path) is None

    result = runner.invoke(cli, ["dataset", "add", "--force", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Theses paths are ignored" not in result.output
    assert str(source_path) not in result.output
    assert "OK" in result.output

    dataset = load_dataset_with_injection("my-dataset", client)

    assert dataset.find_file(relative_path) is not None


def test_add_external_files(runner, client, directory_tree, no_lfs_size_limit, load_dataset_with_injection):
    """Check adding external files."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    assert path.exists()
    assert path.is_symlink()
    external_path = directory_tree / "file1"
    assert path.resolve() == external_path

    dataset = load_dataset_with_injection("my-data", client)
    assert dataset.find_file(path.relative_to(client.path)) is not None

    # Symbolic links should not be tracked
    attr_path = client.path / ".gitattributes"
    assert not attr_path.exists() or "file1" not in attr_path.read_text()


def test_overwrite_external_file(runner, client, directory_tree, subdirectory):
    """Check overwriting external and normal files."""
    # Add external file
    result = runner.invoke(cli, ["dataset", "add", "--create", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    # Cannot add the same file
    result = runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)])
    assert 1 == result.exit_code
    assert "Warning: No new file was added to project" in result.output

    # Can add the same file with --overwrite
    result = runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    pointer_files_deleted = list(client.renku_pointers_path.rglob("*")) == []
    assert pointer_files_deleted

    # Can add the same external file
    result = runner.invoke(cli, ["dataset", "add", "--external", "my-data", "--overwrite", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)
    pointer_files_exist = len(list(client.renku_pointers_path.rglob("*"))) > 0
    assert pointer_files_exist


def test_remove_external_file(runner, client, directory_tree, subdirectory):
    """Test removal of external files."""
    result = runner.invoke(cli, ["dataset", "add", "--create", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    targets_before = {str(p.resolve()) for p in client.renku_pointers_path.rglob("*")}
    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"

    result = runner.invoke(cli, ["rm", str(path)])
    assert 0 == result.exit_code, format_result_exception(result)

    targets_after = {str(p.resolve()) for p in client.renku_pointers_path.rglob("*")}

    removed = targets_before - targets_after
    assert 1 == len(removed)
    assert removed.pop().endswith("/file1")


def test_unavailable_external_files(runner, client, directory_tree, subdirectory):
    """Check for external files that are not available."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    path = Path(DATA_DIR) / "my-data" / directory_tree.name / "file1"
    target = (client.path / path).resolve()

    directory_tree.joinpath("file1").unlink()
    assert not path.exists()

    # Update won't work
    result = runner.invoke(cli, ["dataset", "update", "--external"])
    assert 2 == result.exit_code
    assert "External file not found" in result.output

    # Renku doctor shows inaccessible files
    result = runner.invoke(cli, ["doctor"])
    assert 1 == result.exit_code
    assert "There are missing external files." in result.output
    assert str(path) in result.output
    assert str(target) in result.output


@pytest.mark.serial
def test_external_file_update(runner, client, directory_tree, subdirectory):
    """Check updating external files."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    directory_tree.joinpath("file1").write_text("some updates")

    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    previous_commit = client.find_previous_commit(path)

    result = runner.invoke(cli, ["dataset", "update", "--external", "my-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    current_commit = client.find_previous_commit(path)
    assert current_commit != previous_commit


@pytest.mark.skip("renku update doesn't support new database, reenable once it does")
@pytest.mark.serial
def test_workflow_with_external_file(runner, client, directory_tree, run, subdirectory, no_lfs_size_limit):
    """Check using external files in workflows."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code, format_result_exception(result)

    source = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    output = client.path / DATA_DIR / "output.txt"

    assert 0 == run(args=("run", "wc", "-c"), stdin=source, stdout=output)

    previous_commit = client.find_previous_commit(output)

    # Update external file
    directory_tree.joinpath("file1").write_text("some updates")

    # Nothing is changed unless external files are updated
    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["dataset", "update", "--external", "my-data"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code, format_result_exception(result)

    assert 0 == run(args=("update", "--all"))
    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    current_commit = client.find_previous_commit(source)
    assert current_commit != previous_commit

    attributes = (client.path / ".gitattributes").read_text().split()
    assert "data/output.txt" in attributes


def test_immutability_for_files(directory_tree, runner, client, load_dataset_with_injection):
    """Test dataset's ID changes after a change to dataset files."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = load_dataset_with_injection("my-data", client)

    # Add some files
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code

    dataset = load_dataset_with_injection("my-data", client)
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    # Add the same files again; it should mutate because files addition dates change
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree)]).exit_code

    dataset = load_dataset_with_injection("my-data", client)
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    # Remove some files
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "-I", "file1", "--yes"]).exit_code

    dataset = load_dataset_with_injection("my-data", client)
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_for_adding_files_twice(directory_tree, runner, client, load_dataset_with_injection):
    """Test dataset's ID does not change changes if the same files are added again."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--create", str(directory_tree)]).exit_code
    old_dataset = load_dataset_with_injection("my-data", client)

    assert 1 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code
    dataset = load_dataset_with_injection("my-data", client)

    assert old_dataset.id == dataset.id


def test_immutability_after_external_update(runner, client, directory_tree, load_dataset_with_injection):
    """Test dataset's ID changes after updating external files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code
    old_dataset = load_dataset_with_injection("my-data", client)

    directory_tree.joinpath("file1").write_text("some updates")
    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code
    dataset = load_dataset_with_injection("my-data", client)

    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_after_no_update(runner, client, directory_tree, load_dataset_with_injection):
    """Test dataset's ID does not changes if no external file is updated."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code
    old_dataset = load_dataset_with_injection("my-data", client)

    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code
    dataset = load_dataset_with_injection("my-data", client)

    assert dataset.id == old_dataset.id


def test_datasets_provenance_after_create(runner, client, get_datasets_provenance_with_injection):
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

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
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

    assert not client.repo.is_dirty()


def test_datasets_provenance_after_create_when_adding(runner, client, get_datasets_provenance_with_injection):
    """Test datasets provenance is updated after creating a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--create", "my-data", "README.md"]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-data")

    assert dataset.initial_identifier == dataset.identifier
    assert dataset.derived_from is None
    assert dataset.same_as is None
    assert {"README.md"} == {Path(f.entity.path).name for f in dataset.dataset_files}

    assert not client.repo.is_dirty()


def test_datasets_provenance_after_edit(
    runner, client, load_dataset_with_injection, get_datasets_provenance_with_injection
):
    """Test datasets provenance is updated after editing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"], catch_exceptions=False).exit_code

    dataset = load_dataset_with_injection("my-data", client)

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_previous_version(current_version)

    assert_dataset_is_mutated(old=old_version, new=dataset)
    assert dataset.identifier == current_version.identifier
    assert old_version.initial_identifier == old_version.identifier
    assert set() == set(old_version.keywords)
    assert {"new-data"} == set(current_version.keywords)


def test_datasets_provenance_after_add(runner, client, directory_tree, get_datasets_provenance_with_injection):
    """Test datasets provenance is updated after adding data to a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--create", str(directory_tree / "file1")]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        dataset = datasets_provenance.get_by_name("my-data")

    path = os.path.join(DATA_DIR, "my-data", "file1")
    file = dataset.find_file(path)
    object_hash = client.repo.git.rev_parse(f"HEAD:{path}")

    assert object_hash in file.entity.id
    assert path in file.entity.id
    assert object_hash == file.entity.checksum
    assert path == file.entity.path


def test_datasets_provenance_after_multiple_adds(
    runner, client, directory_tree, get_datasets_provenance_with_injection
):
    """Test datasets provenance is re-using DatasetFile objects after multiple adds."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree / "dir1")]).exit_code

    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree / "file1")]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        provenance = datasets_provenance.get_provenance()

        assert 1 == len(provenance)

        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_by_id(current_version.derived_from.url_id)

    old_dataset_file_ids = {f.id for f in old_version.files}

    path = os.path.join(DATA_DIR, "my-data", "dir1", "file2")
    file2 = current_version.find_file(path)

    assert file2.id in old_dataset_file_ids


def test_datasets_provenance_after_add_with_overwrite(
    runner, client, directory_tree, get_datasets_provenance_with_injection
):
    """Test datasets provenance is updated if adding and overwriting same files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--create", str(directory_tree)]).exit_code

    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree)]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        provenance = datasets_provenance.get_provenance()

        assert 1 == len(provenance)

        current_version = datasets_provenance.get_by_name("my-data")
        old_version = datasets_provenance.get_by_id(current_version.derived_from.url_id)
    old_dataset_file_ids = {f.id for f in old_version.files}

    for dataset_file in current_version.files:
        assert not dataset_file.is_removed()
        # NOTE: DatasetFile should be recreated when adding the same file with the `--overwrite` option
        assert dataset_file.id not in old_dataset_file_ids


def test_datasets_provenance_after_file_unlink(
    runner, client, directory_tree, load_dataset_with_injection, get_datasets_provenance_with_injection
):
    """Test datasets provenance is updated after removing data."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code

    dataset = load_dataset_with_injection("my-data", client)
    with get_datasets_provenance_with_injection(client) as datasets_provenance:
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


def test_datasets_provenance_after_remove(
    runner, client, directory_tree, load_dataset_with_injection, get_datasets_provenance_with_injection
):
    """Test datasets provenance is updated after removing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree)]).exit_code

    dataset = load_dataset_with_injection("my-data", client)

    assert 0 == runner.invoke(cli, ["dataset", "rm", "my-data"]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")
        provenance = datasets_provenance.get_provenance()

    assert current_version is None
    # NOTE: We only keep the tail of provenance chain for each dataset in the provenance
    assert 1 == len(provenance)

    last_version = next(d for d in provenance)

    assert last_version.is_removed() is True
    assert_dataset_is_mutated(old=dataset, new=last_version)


@pytest.mark.serial
def test_datasets_provenance_after_update(runner, client, directory_tree, get_datasets_provenance_with_injection):
    """Test datasets provenance is updated after updating a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code

    directory_tree.joinpath("file1").write_text("some updates")
    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        current_version = datasets_provenance.get_by_name("my-data")

    assert current_version.identifier != current_version.initial_identifier


def test_datasets_provenance_after_adding_tag(
    runner, client, get_datasets_provenance_with_injection, load_dataset_with_injection
):
    """Test datasets provenance is updated after tagging a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = load_dataset_with_injection("my-data", client)

    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        provenance = datasets_provenance.get_provenance()
        current_version = datasets_provenance.get_by_name("my-data")

    assert 1 == len(provenance)
    assert current_version.identifier == current_version.initial_identifier
    assert current_version.derived_from is None
    assert current_version.identifier == old_dataset.identifier
    assert not client.repo.is_dirty()


def test_datasets_provenance_after_removing_tag(
    runner, client, get_datasets_provenance_with_injection, load_dataset_with_injection
):
    """Test datasets provenance is updated after removing a dataset's tag."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    old_dataset = load_dataset_with_injection("my-data", client)

    assert 0 == runner.invoke(cli, ["dataset", "rm-tags", "my-data", "42.0"]).exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        provenance = datasets_provenance.get_provenance()
        current_version = datasets_provenance.get_by_name("my-data")

    assert 1 == len(provenance)
    assert current_version.identifier == current_version.initial_identifier
    assert current_version.derived_from is None
    assert current_version.identifier == old_dataset.identifier
    assert not client.repo.is_dirty()


def test_datasets_provenance_multiple(
    runner, client, directory_tree, load_dataset_with_injection, get_datasets_provenance_with_injection
):
    """Test datasets provenance is updated after multiple dataset operations."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    v1 = load_dataset_with_injection("my-data", client)
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code

    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        tail_dataset = datasets_provenance.get_by_name("my-data", immutable=True)
        provenance = datasets_provenance.get_provenance()

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


def test_datasets_provenance_add_file(runner, client, directory_tree, load_dataset_with_injection):
    """Test add to dataset using graph command."""
    file1 = str(directory_tree.joinpath("file1"))
    assert 0 == runner.invoke(cli, ["dataset", "add", "--create", "my-data", file1]).exit_code
    dir1 = str(directory_tree.joinpath("dir1"))
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", dir1]).exit_code

    dataset = load_dataset_with_injection("my-data", client)

    assert {"file1", "file2", "file3"} == {Path(f.entity.path).name for f in dataset.files}


def test_immutability_of_dataset_files(runner, client, directory_tree, load_dataset_with_injection):
    """Test DatasetFiles are generated when their Entity changes."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree / "file1")]).exit_code

    file1 = os.path.join(DATA_DIR, "my-data", "file1")

    v1 = load_dataset_with_injection("my-data", client).find_file(file1)

    # DatasetFile changes when Entity is changed
    write_and_commit_file(client.repo, file1, "changed content")
    assert 0 == runner.invoke(cli, ["dataset", "update"]).exit_code
    v2 = load_dataset_with_injection("my-data", client).find_file(file1)

    assert v1.id != v2.id

    # DatasetFile doesn't change when Entity is unchanged
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree / "dir1" / "file2")]).exit_code
    v3 = load_dataset_with_injection("my-data", client).find_file(file1)

    assert v2.id == v3.id

    # DatasetFile changes when Entity is unchanged but is overwritten
    assert (
        0 == runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree / "file1")]).exit_code
    )
    v4 = load_dataset_with_injection("my-data", client).find_file(file1)

    assert v3.id != v4.id

    # DatasetFile changes if the file is removed
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "file1"], input="y").exit_code
    dataset = load_dataset_with_injection("my-data", client)
    v5 = next(f for f in dataset.dataset_files if f.is_removed())

    assert "file1" in v5.entity.path
    assert v4.id != v5.id


@pytest.mark.serial
def test_unauthorized_import(mock_kg, client, runner):
    """Test importing without a valid token."""
    client.set_value("http", "renku.ch", "not-renku-token", global_only=True)

    result = runner.invoke(
        cli, ["dataset", "import", "https://renku.ch/projects/user/project-name/datasets/123"], catch_exceptions=False
    )

    assert 1 == result.exit_code
    assert "Unauthorized access to knowledge graph" in result.output
    assert "renku login renku.ch" in result.output


@pytest.mark.serial
def test_authorized_import(mock_kg, client, runner):
    """Test importing with a valid token.

    NOTE: Returning 404 from KG means that the request was authorized. We don't implement a full import due to mocking
    complexity.
    """
    client.set_value("http", "renku.ch", "renku-token", global_only=True)

    result = runner.invoke(cli, ["dataset", "import", "https://renku.ch/projects/user/project-name/datasets/123"])

    assert 1 == result.exit_code
    assert "Unauthorized access to knowledge graph" not in result.output
    assert "Resource not found in knowledge graph" in result.output


def test_update_local_file(runner, client, directory_tree, load_dataset_with_injection):
    """Check updating local files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code

    file1 = Path(DATA_DIR) / "my-data" / directory_tree.name / "file1"
    file1.write_text("some updates")
    client.repo.git.add("--all")
    client.repo.index.commit("file1")
    new_checksum_file1 = get_object_hash(client.repo, file1)

    file2 = Path(DATA_DIR) / "my-data" / directory_tree.name / "dir1" / "file2"
    file2.write_text("some updates")
    client.repo.git.add("--all")
    client.repo.index.commit("file2")
    new_checksum_file2 = get_object_hash(client.repo, file2)

    old_dataset = load_dataset_with_injection("my-data", client)

    assert new_checksum_file1 != old_dataset.find_file(file1).entity.checksum
    assert new_checksum_file2 != old_dataset.find_file(file2).entity.checksum

    result = runner.invoke(cli, ["dataset", "update", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    dataset = load_dataset_with_injection("my-data", client)
    assert new_checksum_file1 == dataset.find_file(file1).entity.checksum
    assert new_checksum_file2 == dataset.find_file(file2).entity.checksum
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_update_local_deleted_file(runner, client, directory_tree, load_dataset_with_injection):
    """Check updating local deleted files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(directory_tree)]).exit_code

    file1 = Path(DATA_DIR) / "my-data" / directory_tree.name / "file1"
    file1.unlink()
    client.repo.git.add("--all")
    client.repo.index.commit("deleted file1")
    commit_sha_after_file1_delete = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["dataset", "update", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Some files are deleted." in result.output
    assert "Updated 0 files" in result.output
    assert commit_sha_after_file1_delete == client.repo.head.object.hexsha
    old_dataset = load_dataset_with_injection("my-data", client)
    assert old_dataset.find_file(file1)

    # NOTE: Update with `--delete`
    result = runner.invoke(cli, ["dataset", "update", "--delete", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Updated 0 files and deleted 1 files" in result.output
    assert commit_sha_after_file1_delete != client.repo.head.object.hexsha
    dataset = load_dataset_with_injection("my-data", client)
    assert dataset.find_file(file1) is None
    assert_dataset_is_mutated(old=old_dataset, new=dataset)

    result = runner.invoke(cli, ["dataset", "update", "--delete", "my-data"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Updated 0 files and deleted 0 files" in result.output
