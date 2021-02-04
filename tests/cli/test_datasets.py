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
"""Test ``dataset`` command."""

from __future__ import absolute_import, print_function

import json
import os
import shutil
import textwrap
from pathlib import Path

import pytest

from renku.cli import cli
from renku.core.commands.format.dataset_files import DATASET_FILES_COLUMNS, DATASET_FILES_FORMATS
from renku.core.commands.format.datasets import DATASETS_COLUMNS, DATASETS_FORMATS
from renku.core.commands.providers import DataverseProvider, ProviderFactory, ZenodoProvider
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.refs import LinkReference
from tests.utils import assert_dataset_is_mutated


def test_datasets_create_clean(runner, project, client):
    """Test creating a dataset in clean repository."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    dataset = client.load_dataset("dataset")
    assert dataset

    staged = client.repo.index.diff("HEAD")
    for file_path in staged:
        assert "datasets" not in file_path

    untracked = client.repo.untracked_files
    for file_path in untracked:
        assert "datasets" not in file_path


def test_datasets_create_with_metadata(runner, client, subdirectory):
    """Test creating a dataset with metadata."""
    result = runner.invoke(
        cli,
        [
            "dataset",
            "create",
            "my-dataset",
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
        ],
    )
    assert 0 == result.exit_code
    assert "OK" in result.output

    dataset = client.load_dataset("my-dataset")
    assert dataset.title == "Long Title"
    assert dataset.name == "my-dataset"
    assert dataset.description == "some description here"
    assert "John Doe" in [c.name for c in dataset.creators]
    assert "john.doe@mail.ch" in [c.email for c in dataset.creators]
    assert "John Smiths" in [c.name for c in dataset.creators]
    assert "john.smiths@mail.ch" in [c.email for c in dataset.creators]
    assert {"keyword-1", "keyword-2"} == set(dataset.keywords)


def test_dataset_show(runner, client, subdirectory):
    """Test creating a dataset with metadata."""
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
        ],
    )
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "show", "my-dataset"])
    assert 0 == result.exit_code
    assert "some description here" in result.output
    assert "Long Title" in result.output
    assert "keyword-1" in result.output
    assert "keyword-2" in result.output
    assert "Created: " in result.output
    assert "Name: my-dataset" in result.output
    assert "John Doe <john.doe@mail.ch>" in result.output
    assert "##" not in result.output


def test_datasets_create_different_names(runner, client):
    """Test creating datasets with same title but different name."""
    result = runner.invoke(cli, ["dataset", "create", "dataset-1", "--title", "title"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "dataset-2", "--title", "title"])
    assert 0 == result.exit_code
    assert "OK" in result.output


def test_datasets_create_with_same_name(runner, client):
    """Test creating datasets with same name."""
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 1 == result.exit_code
    assert 'Dataset exists: "dataset"' in result.output


@pytest.mark.parametrize(
    "name",
    ["any name /@#$!", "name longer than 24 characters", "semi valid-name", "dataset/new", "/dataset", "dataset/"],
)
def test_datasets_invalid_name(runner, client, name):
    """Test creating datasets with invalid name."""
    result = runner.invoke(cli, ["dataset", "create", name])
    assert 2 == result.exit_code
    assert 'name "{}" is not valid.'.format(name) in result.output


def test_datasets_create_dirty(runner, project, client):
    """Test creating a dataset in dirty repository."""
    # Create a file in root of the repository.
    with (client.path / "a").open("w") as fp:
        fp.write("a")

    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    dataset = client.load_dataset("dataset")
    assert dataset

    staged = client.repo.index.diff("HEAD")
    for file_path in staged:
        assert "datasets" not in file_path

    untracked = client.repo.untracked_files
    for file_path in untracked:
        assert "datasets" not in file_path


def test_datasets_create_dirty_exception_untracked(runner, project, client):
    """Test exception raise for untracked file in renku directory."""
    # 1. Create a problem.
    datasets_dir = client.path / RENKU_HOME / DatasetsApiMixin.DATASETS
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
    datasets_dir = client.path / RENKU_HOME / DatasetsApiMixin.DATASETS
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
    datasets_dir = client.path / RENKU_HOME / DatasetsApiMixin.DATASETS
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
    datasets_dir = client.path / RENKU_HOME / DatasetsApiMixin.DATASETS
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

    datasets_dir = client.path / RENKU_HOME / DatasetsApiMixin.DATASETS
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


def test_dataset_url_in_different_domain(runner, client):
    """Test URL is set correctly in a different Renku domain."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code

    renku_domain = os.environ.get("RENKU_DOMAIN")
    try:
        os.environ["RENKU_DOMAIN"] = "alternative-domain"

        with client.with_dataset("my-dataset") as dataset:
            assert dataset.url.startswith("https://alternative-domain")
            assert dataset.url == dataset._id
    finally:
        if renku_domain:
            os.environ["RENKU_DOMAIN"] = renku_domain
        else:
            del os.environ["RENKU_DOMAIN"]


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
def test_datasets_list_empty(output_format, runner, project):
    """Test listing without datasets."""
    format_option = "--format={0}".format(output_format)
    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code


@pytest.mark.parametrize("output_format", DATASETS_FORMATS.keys())
def test_datasets_list_non_empty(output_format, runner, project):
    """Test listing with datasets."""
    format_option = "--format={0}".format(output_format)
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "ls", format_option])
    assert 0 == result.exit_code
    assert "my-dataset" in result.output

    result = runner.invoke(cli, ["dataset", "ls", "--revision=HEAD~1", format_option])
    assert 0 == result.exit_code
    assert "my-dataset" not in result.output


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
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "ls", "--columns", columns])
    assert 0 == result.exit_code
    assert headers == result.output.split("\n").pop(0).split()
    for value in values:
        assert value in result.output


@pytest.mark.parametrize("column", DATASETS_COLUMNS.keys())
def test_datasets_list_columns_correctly(runner, project, column):
    """Test dataset listing only shows requested columns."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "test"]).exit_code

    result = runner.invoke(cli, ["dataset", "ls", "--columns", column])
    assert 0 == result.exit_code
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

    assert 0 == result.exit_code
    line = next(line for line in result.output.split("\n") if "test" in line)
    assert short_description in line
    assert description[: len(short_description) + 1] not in line


def test_add_and_create_dataset(directory_tree, runner, project, client, subdirectory):
    """Test add data to a non-existing dataset."""
    result = runner.invoke(cli, ["dataset", "add", "new-dataset", str(directory_tree)], catch_exceptions=False)
    assert 1 == result.exit_code
    assert 'Dataset "new-dataset" does not exist.' in result.output

    # Add succeeds with --create
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output

    # Further, add with --create fails
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 1 == result.exit_code


def test_add_and_create_dataset_with_lfs_warning(directory_tree, runner, project, client_with_lfs_warning):
    """Test add data with lfs warning."""

    # Add succeeds with --create
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", str(directory_tree)], catch_exceptions=False
    )
    assert 0 == result.exit_code
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
    assert 0 == result.exit_code

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
    assert 0 == result.exit_code
    assert "OK" in result.output

    filename = "filéàèû爱ಠ_ಠ.txt"
    new_file = tmpdir.join(filename)
    new_file.write(str("test"))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)],)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["log", "--format", "json-ld", "--strict", f"data/my-dataset/{filename}"])
    assert 0 == result.exit_code
    assert filename in result.output.encode("latin1").decode("unicode-escape")


def test_multiple_file_to_dataset(tmpdir, runner, project, client):
    """Test importing multiple data into a dataset at once."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    with client.with_dataset("dataset") as dataset:
        assert dataset.title == "dataset"

    paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "dataset"] + paths, catch_exceptions=False,)
    assert 0 == result.exit_code


def test_repository_file_to_dataset(runner, project, client, subdirectory):
    """Test adding a file from the repository into a dataset."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    a_path = client.path / "a"
    a_path.write_text("a content")

    client.repo.git.add(str(a_path))
    client.repo.git.commit(message="Added file a", no_verify=True)

    # add data
    result = runner.invoke(cli, ["dataset", "add", "dataset", str(a_path)], catch_exceptions=False,)
    assert 0 == result.exit_code

    with client.with_dataset("dataset") as dataset:
        assert dataset.title == "dataset"
        assert dataset.find_file("a") is not None


def test_relative_import_to_dataset(tmpdir, runner, client, subdirectory):
    """Test importing data from a directory structure."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    with client.with_dataset("dataset") as dataset:
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

    result = runner.invoke(cli, ["dataset", "add", "dataset"] + paths, catch_exceptions=False,)
    assert 0 == result.exit_code

    assert os.stat(client.path / DATA_DIR / "dataset" / "zero.txt")
    assert os.stat(client.path / DATA_DIR / "dataset" / "first" / "first.txt")
    assert os.stat(client.path / DATA_DIR / "dataset" / "first" / "second" / "second.txt")


@pytest.mark.parametrize(
    "params,message",
    [
        (["-s", "file", "https://example.com"], 'Cannot use "--source" with URLs or local files.'),
        (["-s", "file", "/some/local/path"], 'Cannot use "--source" with URLs or local files.'),
    ],
)
def test_usage_error_in_add_from_url(runner, client, params, message):
    """Test user's errors when adding URL/local file to a dataset."""
    result = runner.invoke(cli, ["dataset", "add", "remote", "--create"] + params, catch_exceptions=False,)
    assert 2 == result.exit_code
    assert message in result.output


def test_add_from_local_repo_warning(runner, client, data_repository, directory_tree):
    """Test a warning is printed when adding from a local git repo."""
    result = runner.invoke(cli, ["dataset", "add", "dataset", "--create", str(directory_tree)], catch_exceptions=False,)
    assert 0 == result.exit_code
    assert "Use remote's Git URL instead to enable lineage " in result.output


def test_add_data_directory(runner, client):
    """Test adding a dataset's data directory to it prints an error."""
    result = runner.invoke(cli, ["dataset", "create", "new-dataset"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "add", "new-dataset", "data/new-dataset"], catch_exceptions=False,)
    assert 2 == result.exit_code
    assert "Cannot add dataset's data directory recursively" in result.output


def test_dataset_add_with_copy(tmpdir, runner, project, client):
    """Test adding data to dataset with copy."""
    import os
    import stat

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    paths = []
    original_inodes = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        original_inodes.append(os.lstat(str(new_file))[stat.ST_INO])
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths,)
    assert 0 == result.exit_code

    received_inodes = []
    with client.with_dataset("my-dataset") as dataset:
        assert dataset.title == "my-dataset"

        for file_ in dataset.files:
            path_ = (client.path / file_.path).resolve()
            received_inodes.append(os.lstat(str(path_))[stat.ST_INO])

    # check that original inodes are within created ones
    for inode in received_inodes:
        assert inode not in original_inodes


@pytest.mark.serial
def test_dataset_add_many(tmpdir, runner, project, client):
    """Test adding many files to dataset."""

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    paths = []
    for i in range(1000):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths,)
    assert 0 == result.exit_code

    assert len(client.repo.head.commit.message) <= 100


def test_dataset_file_path_from_subdirectory(runner, client, subdirectory):
    """Test adding a file into a dataset and check path independent
    of the CWD """
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    a_path = client.path / "a"
    a_path.write_text("a text")

    client.repo.git.add(str(a_path))
    client.repo.git.commit(message="Added file a")

    # add data
    result = runner.invoke(cli, ["dataset", "add", "dataset", str(a_path)], catch_exceptions=False,)
    assert 0 == result.exit_code

    with client.with_dataset("dataset") as dataset:
        file_ = dataset.find_file("a")
        assert file_ is not None
        assert file_.full_path == client.path / "a"


def test_datasets_ls_files_tabular_empty(runner, project):
    """Test listing of data within empty dataset."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # list all files in dataset
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", "added,creators,dataset,path", "my-dataset"])
    assert 0 == result.exit_code

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
    assert 0 == result.exit_code


def test_datasets_ls_files_lfs(tmpdir, large_file, runner, project):
    """Test file listing lfs status."""
    # NOTE: create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # NOTE: create some data
    paths = []

    new_file = tmpdir.join("file_1")
    new_file.write(str(1))
    paths.append(str(new_file))

    paths.append(str(large_file))

    # NOTE: add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths, catch_exceptions=False,)
    assert 0 == result.exit_code

    # NOTE: check files
    result = runner.invoke(cli, ["dataset", "ls-files"])
    assert 0 == result.exit_code

    lines = result.output.split("\n")
    file1_entry = next(line for line in lines if "file_1" in line)
    file2_entry = next(line for line in lines if large_file.name in line)

    assert file1_entry
    assert file2_entry
    assert not file1_entry.endswith("*")
    assert file2_entry.endswith("*")


@pytest.mark.parametrize("column", DATASET_FILES_COLUMNS.keys())
def test_datasets_ls_files_columns_correctly(runner, project, column, directory_tree):
    """Test file listing only shows requested columns."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

    result = runner.invoke(cli, ["dataset", "ls-files", "--columns", column])
    assert 0 == result.exit_code
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
    assert 0 == result.exit_code

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

    assert 0 == result.exit_code
    # check output
    assert "file1" in result.output
    assert "file2" not in result.output
    assert "file3" in result.output

    # check directory pattern
    result = runner.invoke(cli, ["dataset", "ls-files", "--include=**/dir1/*"])

    assert 0 == result.exit_code
    # check output
    assert "file1" not in result.output
    assert "file2" in result.output
    assert "file3" in result.output


def test_datasets_ls_files_tabular_creators(runner, client, directory_tree):
    """Test listing of data within dataset with creators filters."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

    creator = client.load_dataset("my-dataset").creators[0].name

    assert creator is not None

    # check creators filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--creators={0}".format(creator)])
    assert 0 == result.exit_code

    # check output
    for file_ in directory_tree.rglob("*file*"):
        assert file_.name in result.output


def test_datasets_ls_files_correct_paths(runner, client, directory_tree):
    """Test listing of data within dataset and check that paths are correct."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree)]).exit_code

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--format=json-ld"])
    assert 0 == result.exit_code

    output = json.loads(result.output)
    for record in output:
        assert (client.path / record["http://www.w3.org/ns/prov#atLocation"]).exists()


def test_datasets_ls_files_with_name(directory_tree, runner, project):
    """Test listing of data within dataset with include/exclude filters."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset", "--title", "Long Title"])
    assert 0 == result.exit_code

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)], catch_exceptions=False,)
    assert 0 == result.exit_code

    # list files with name
    result = runner.invoke(cli, ["dataset", "ls-files", "my-dataset"])
    assert 0 == result.exit_code
    assert "dir1/file2" in result.output


def test_datasets_ls_files_correct_size(runner, client, directory_tree, large_file):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code

    path = client.path / DATA_DIR / "my-dataset" / "file1"
    shutil.copy(large_file, path)

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns=size, path"])
    assert 0 == result.exit_code

    line = next(line for line in result.output.split("\n") if "file1" in line)
    size = int(line.split()[0])

    assert 3 == size


def test_datasets_ls_files_correct_commit(runner, client, directory_tree):
    """Test ls-files shows the size stored in git and not the current file size."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "-c", str(directory_tree / "file1")]).exit_code

    commit = client.find_previous_commit(paths=client.path / DATA_DIR / "my-dataset" / "file1")

    # check include / exclude filters
    result = runner.invoke(cli, ["dataset", "ls-files", "--columns=commit,path"])
    assert 0 == result.exit_code

    line = next(line for line in result.output.split("\n") if "file1" in line)
    commit_sha = line.split()[0]

    assert commit.hexsha == commit_sha


def test_dataset_unlink_file_not_found(runner, project):
    """Test unlinking of file from dataset with no files found."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", "notthere.csv"])

    assert 2 == result.exit_code


def test_dataset_unlink_file_abort_unlinking(tmpdir, runner, project):
    """Test unlinking of file from dataset and aborting."""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)])
    assert 0 == result.exit_code

    # unlink file from dataset
    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename], input="n")
    assert 1 == result.exit_code

    # check output
    assert "Aborted!" in result.output


def test_dataset_unlink_file(tmpdir, runner, client, subdirectory):
    """Test unlinking of file and check removal from dataset"""
    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create data file
    new_file = tmpdir.join("datafile.csv")
    new_file.write("1,2,3")

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)])
    assert 0 == result.exit_code
    assert not client.repo.is_dirty()

    with client.with_dataset("my-dataset") as dataset:
        assert new_file.basename in {Path(file_.path).name for file_ in dataset.files}

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["dataset", "unlink", "my-dataset", "--include", new_file.basename, "-y"])
    assert 0 == result.exit_code
    assert not client.repo.is_dirty()

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after

    with client.with_dataset("my-dataset") as dataset:
        assert new_file.basename not in [file_.path.name for file_ in dataset.files]


def test_dataset_rm(runner, client, directory_tree, subdirectory):
    """Test removal of a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "--create", "my-dataset", str(directory_tree)]).exit_code

    dataset = client.load_dataset("my-dataset")
    assert client.load_dataset("my-dataset")
    assert (client.path / dataset.path).exists()

    result = runner.invoke(cli, ["dataset", "rm", "my-dataset"])

    assert 0 == result.exit_code, result.output
    assert "OK" in result.output
    assert not client.load_dataset("my-dataset")
    assert not (client.path / dataset.path).exists()

    result = runner.invoke(cli, ["doctor"], catch_exceptions=False)
    assert 0 == result.exit_code


def test_dataset_rm_failure(runner, client):
    """Test errors in removal of a dataset."""
    assert 2 == runner.invoke(cli, ["dataset", "rm"]).exit_code
    assert 1 == runner.invoke(cli, ["dataset", "rm", "does-not-exist"]).exit_code


def test_dataset_overwrite_no_confirm(runner, project):
    """Check dataset overwrite behaviour without confirmation."""
    result = runner.invoke(cli, ["dataset", "create", "rokstar"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    result = runner.invoke(cli, ["dataset", "create", "rokstar"])
    assert 1 == result.exit_code
    assert "OK" not in result.output


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit(runner, client, project, dirty, subdirectory):
    """Check dataset metadata editing."""
    if dirty:
        with (client.path / "dirty_file").open("w") as fp:
            fp.write("a")

    result = runner.invoke(cli, ["dataset", "create", "dataset", "-t", "original title", "-k", "keyword-1"])
    assert 0 == result.exit_code

    creator1 = "Forename1 Surname1 <name.1@mail.com> [Affiliation 1]"
    creator2 = "Forename2 Surname2"

    result = runner.invoke(
        cli,
        ["dataset", "edit", "dataset", "-d", " new description ", "-c", creator1, "-c", creator2],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert "Successfully updated: creators, description." in result.output
    warning_msg = "Warning: No email or wrong format for: Forename2 Surname2"
    assert warning_msg in result.output

    dataset = client.load_dataset("dataset")
    assert " new description " == dataset.description
    assert "original title" == dataset.title
    assert {creator1, creator2}.issubset({c.full_identity for c in dataset.creators})

    result = runner.invoke(cli, ["dataset", "edit", "dataset", "-t", " new title "], catch_exceptions=False)
    assert 0 == result.exit_code
    assert "Successfully updated: title." in result.output

    result = runner.invoke(
        cli, ["dataset", "edit", "dataset", "-k", "keyword-2", "-k", "keyword-3"], catch_exceptions=False
    )
    assert 0 == result.exit_code
    assert "Successfully updated: keywords." in result.output

    dataset = client.load_dataset("dataset")
    assert " new description " == dataset.description
    assert "new title" == dataset.title
    assert {creator1, creator2}.issubset({c.full_identity for c in dataset.creators})
    assert {"keyword-2", "keyword-3"} == set(dataset.keywords)


@pytest.mark.parametrize("dirty", [False, True])
def test_dataset_edit_no_change(runner, client, project, dirty):
    """Check metadata editing does not commit when there is no change."""
    result = runner.invoke(cli, ["dataset", "create", "dataset", "-t", "original title"])
    assert 0 == result.exit_code

    if dirty:
        with client.with_metadata() as project:
            project.name = "new-name"

    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["dataset", "edit", "dataset"], catch_exceptions=False)
    assert 0 == result.exit_code
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


def test_dataset_tag(tmpdir, runner, project, client, subdirectory):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False,)
    assert 0 == result.exit_code

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0"], catch_exceptions=False,)
    assert 0 == result.exit_code

    result = runner.invoke(
        cli, ["dataset", "tag", "my-dataset", "A", "-d", "short descriptiön"], catch_exceptions=False,
    )
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False,)
    assert 0 == result.exit_code


@pytest.mark.parametrize("form", ["tabular", "json-ld"])
def test_dataset_ls_tags(tmpdir, runner, project, client, form):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False,)
    assert 0 == result.exit_code

    commit1 = client.repo.head.commit.hexsha

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0", "-d", "first tag!"], catch_exceptions=False,)
    assert 0 == result.exit_code

    commit2 = client.repo.head.commit.hexsha

    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "aBc9.34-11_55.t"], catch_exceptions=False,)
    assert 0 == result.exit_code

    result = runner.invoke(
        cli, ["dataset", "ls-tags", "my-dataset", "--format={}".format(form)], catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert "1.0" in result.output
    assert "aBc9.34-11_55.t" in result.output
    assert "first tag!" in result.output
    assert commit1 in result.output
    assert commit2 in result.output


def test_dataset_rm_tag(tmpdir, runner, project, client, subdirectory):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False,)
    assert 0 == result.exit_code

    commit1 = client.repo.head.commit.hexsha

    # tag dataset
    result = runner.invoke(cli, ["dataset", "tag", "my-dataset", "1.0", "-d", "first tag!"], catch_exceptions=False,)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "ls-tags", "my-dataset"], catch_exceptions=False,)
    assert 0 == result.exit_code
    assert "1.0" in result.output
    assert "first tag!" in result.output
    assert commit1 in result.output

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "2.0"], catch_exceptions=False,)
    assert 2 == result.exit_code
    assert "not found" in result.output

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1.0"], catch_exceptions=False,)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1.0"], catch_exceptions=False,)
    assert 2 == result.exit_code
    assert "not found" in result.output


def test_dataset_rm_tags_multiple(tmpdir, runner, project, client):
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False,)
    assert 0 == result.exit_code

    for i in range(1, 4):
        # tag dataset
        result = runner.invoke(cli, ["dataset", "tag", "my-dataset", str(i)], catch_exceptions=False,)
        assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1", "2", "3"], catch_exceptions=False,)
    assert 0 == result.exit_code
    assert "1" not in result.output
    assert "2" not in result.output
    assert "3" not in result.output


def test_dataset_rm_tags_failure(tmpdir, runner, project, client):
    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False,)

    assert 1 == result.exit_code
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    # create some data
    new_file = tmpdir.join("file")
    new_file.write(str("test"))

    # add data to dataset
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(new_file)], catch_exceptions=False,)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "rm-tags", "my-dataset", "1"], catch_exceptions=False,)
    assert 2 == result.exit_code


def test_dataset_clean_up_when_add_fails(runner, client, subdirectory):
    """Test project is cleaned when dataset add fails for a new dataset."""
    # add a non-existing path to a new dataset
    result = runner.invoke(
        cli, ["dataset", "add", "--create", "new-dataset", "non-existing-file"], catch_exceptions=True,
    )

    assert 2 == result.exit_code
    ref = client.renku_path / "refs" / "datasets" / "new-dataset"
    assert not ref.is_symlink() and not ref.exists()


def test_avoid_empty_commits(runner, client, directory_tree):
    """Test no empty commit is created when adding existing data."""
    runner.invoke(cli, ["dataset", "create", "my-dataset"])

    commit_sha_before = client.repo.head.object.hexsha
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)])

    assert 0 == result.exit_code

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

    assert 0 == result.exit_code

    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_before != commit_sha_after

    commit_sha_before = commit_sha_after
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset2", str(directory_tree)])
    assert 0 == result.exit_code

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

    assert 0 == result.exit_code


def test_add_removes_local_path_information(runner, client, directory_tree):
    """Test added local paths are stored as relative path."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code

    with client.with_dataset("my-dataset") as dataset:
        relative_path = os.path.relpath(directory_tree, client.path)
        for file_ in dataset.files:
            assert file_.source.startswith(relative_path)
            assert file_.source.endswith(file_.name)
            assert file_.url.endswith(file_.path)


def test_pull_data_from_lfs(runner, client, tmpdir, subdirectory, no_lfs_size_limit):
    """Test pulling data from LFS using relative paths."""
    data = tmpdir.join("data.txt")
    data.write("DATA")

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-data", str(data)])
    assert 0 == result.exit_code
    attributes = (client.path / ".gitattributes").read_text().split()
    assert "data/my-data/data.txt" in attributes

    path = client.path / DATA_DIR / "my-data" / "data.txt"
    relative_path = os.path.relpath(path, os.getcwd())

    result = runner.invoke(cli, ["storage", "pull", relative_path])
    assert 0 == result.exit_code


def test_lfs_hook(runner, client, subdirectory, large_file):
    """Test committing large files to Git."""
    import git

    shutil.copy(large_file, client.path)
    client.repo.git.add("--all")

    # Commit fails when file is not tracked in LFS
    with pytest.raises(git.exc.HookExecutionError) as e:
        client.repo.index.commit("large files not in LFS")

    assert "You are trying to commit large files to Git" in e.value.stdout
    assert large_file.name in e.value.stdout

    # Can be committed after being tracked in LFS
    client.track_paths_in_storage(large_file.name)
    commit = client.repo.index.commit("large files tracked")
    assert "large files tracked" == commit.message


def test_lfs_hook_autocommit(runner, client, subdirectory, large_file):
    """Test committing large files to Git gets automatically added to lfs."""
    result = runner.invoke(cli, ["config", "set", "autocommit_lfs", "true"])
    assert 0 == result.exit_code

    shutil.copy(large_file, client.path)
    client.repo.git.add("--all")

    result = client.repo.git.commit(
        message="large files not in LFS",
        with_extended_output=True,
        env={"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"},
    )
    assert large_file.name in result[1]
    assert ".gitattributes" in result[1]
    assert "You are trying to commit large files to Git instead of Git-LFS" in result[2]
    assert "Adding files to LFS" in result[2]
    assert 'Tracking "large-file"' in result[2]


def test_lfs_hook_autocommit_env(runner, client, subdirectory, large_file):
    """Test committing large files to Git gets automatically added to lfs."""
    os.environ["AUTOCOMMIT_LFS"] = "true"

    shutil.copy(large_file, client.path)
    client.repo.git.add("--all")

    result = client.repo.git.commit(
        message="large files not in LFS",
        with_extended_output=True,
        env={"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"},
    )
    assert large_file.name in result[1]
    assert ".gitattributes" in result[1]
    assert "You are trying to commit large files to Git instead of Git-LFS" in result[2]
    assert "Adding files to LFS" in result[2]
    assert 'Tracking "large-file"' in result[2]


def test_lfs_hook_can_be_avoided(runner, project, subdirectory, large_file):
    """Test committing large files to Git."""
    result = runner.invoke(cli, ["--no-external-storage", "dataset", "add", "-c", "my-dataset", str(large_file)])
    assert 0 == result.exit_code
    assert "OK" in result.output


@pytest.mark.parametrize("external", [False, True])
def test_add_existing_files(runner, client, directory_tree, external, no_lfs_size_limit):
    """Check adding/overwriting existing files."""
    param = ["-e"] if external else []

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)] + param)

    assert 0 == result.exit_code

    path = Path(DATA_DIR) / "my-dataset" / directory_tree.name / "file1"

    dataset = client.load_dataset("my-dataset")
    assert dataset.find_file(path) is not None

    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)] + param)
    assert 1 == result.exit_code
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "Warning: No new file was added to project" in result.output
    assert "Error: There is nothing to commit." in result.output

    result = runner.invoke(cli, ["dataset", "add", "--overwrite", "my-dataset", str(directory_tree)] + param)
    assert 0 == result.exit_code
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
    assert 0 == result.exit_code
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output

    # Add existing and non-existing files
    directory_tree.joinpath("new-file").write_text("new-file")

    result = runner.invoke(cli, ["dataset", "add", "my-dataset", str(directory_tree)] + param)
    assert 0 == result.exit_code
    assert "These existing files were not overwritten" in result.output
    assert str(path) in result.output
    assert "OK" in result.output


def test_add_existing_files_updates_metadata(runner, client, large_file):
    """Check overwriting existing files updates their metadata."""
    # assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "--create", large_file]).exit_code
    result = runner.invoke(cli, ["dataset", "add", "my-dataset", "--create", str(large_file)])
    assert result.exit_code == 0, result.output

    path = Path(DATA_DIR) / "my-dataset" / large_file.name

    before = client.load_dataset("my-dataset").find_file(path)

    large_file.write_text("New modified content.")

    assert 0 == runner.invoke(cli, ["dataset", "add", "my-dataset", "--overwrite", str(large_file)]).exit_code

    after = client.load_dataset("my-dataset").find_file(path)
    assert before._id != after._id
    assert before._label != after._label
    assert before.added != after.added
    assert before.commit != after.commit
    assert before.path == after.path
    assert before.source == after.source
    assert before.url == after.url


def test_add_ignored_files(runner, client, directory_tree):
    """Check adding/force-adding ignored files."""
    source_path = directory_tree / ".DS_Store"
    source_path.write_text("ignored-file")
    path = client.path / DATA_DIR / "my-dataset" / directory_tree.name / ".DS_Store"
    relative_path = str(path.relative_to(client.path))

    result = runner.invoke(cli, ["dataset", "add", "-c", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code
    assert "Theses paths are ignored" in result.output
    assert str(source_path) in result.output
    assert "OK" in result.output

    with client.with_dataset("my-dataset") as dataset:
        assert dataset.find_file(relative_path) is None

    result = runner.invoke(cli, ["dataset", "add", "--force", "my-dataset", str(directory_tree)])
    assert 0 == result.exit_code
    assert "Theses paths are ignored" not in result.output
    assert str(source_path) not in result.output
    assert "OK" in result.output

    with client.with_dataset("my-dataset") as dataset:
        assert dataset.find_file(relative_path) is not None


def test_add_external_files(runner, client, directory_tree, no_lfs_size_limit):
    """Check adding external files."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code

    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    assert path.exists()
    assert path.is_symlink()
    external_path = directory_tree / "file1"
    assert path.resolve() == external_path

    dataset = client.load_dataset("my-data")
    assert dataset.find_file(path.relative_to(client.path)) is not None

    # Symbolic links should not be tracked
    attr_path = client.path / ".gitattributes"
    assert not attr_path.exists() or "file1" not in attr_path.read_text()


def test_overwrite_external_file(runner, client, directory_tree, subdirectory):
    """Check overwriting external and normal files."""
    # Add external file
    result = runner.invoke(cli, ["dataset", "add", "--create", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code

    # Cannot add the same file
    result = runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)])
    assert 1 == result.exit_code
    assert "Warning: No new file was added to project" in result.output

    # Can add the same file with --overwrite
    result = runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree)])
    assert 0 == result.exit_code
    pointer_files_deleted = list(client.renku_pointers_path.rglob("*")) == []
    assert pointer_files_deleted

    # Can add the same external file
    result = runner.invoke(cli, ["dataset", "add", "--external", "my-data", "--overwrite", str(directory_tree)])
    assert 0 == result.exit_code
    pointer_files_exist = len(list(client.renku_pointers_path.rglob("*"))) > 0
    assert pointer_files_exist


def test_remove_external_file(runner, client, directory_tree, subdirectory):
    """Test removal of external files."""
    result = runner.invoke(cli, ["dataset", "add", "--create", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code

    targets_before = {str(p.resolve()) for p in client.renku_pointers_path.rglob("*")}
    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"

    result = runner.invoke(cli, ["rm", str(path)])
    assert 0 == result.exit_code

    targets_after = {str(p.resolve()) for p in client.renku_pointers_path.rglob("*")}

    removed = targets_before - targets_after
    assert 1 == len(removed)
    assert removed.pop().endswith("/file1")


def test_unavailable_external_files(runner, client, directory_tree, subdirectory):
    """Check for external files that are not available."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code

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
    assert 0 == result.exit_code

    directory_tree.joinpath("file1").write_text("some updates")

    path = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    previous_commit = client.find_previous_commit(path)

    result = runner.invoke(cli, ["dataset", "update", "--external", "my-data"])
    assert 0 == result.exit_code

    current_commit = client.find_previous_commit(path)
    assert current_commit != previous_commit


@pytest.mark.serial
def test_workflow_with_external_file(runner, client, directory_tree, run, subdirectory, no_lfs_size_limit):
    """Check using external files in workflows."""
    result = runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)])
    assert 0 == result.exit_code

    source = client.path / DATA_DIR / "my-data" / directory_tree.name / "file1"
    output = client.path / DATA_DIR / "output.txt"

    assert 0 == run(args=("run", "wc", "-c"), stdin=source, stdout=output)

    previous_commit = client.find_previous_commit(output)

    # Update external file
    directory_tree.joinpath("file1").write_text("some updates")

    # Nothing is changed unless external files are updated
    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["dataset", "update", "--external", "my-data"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == run(args=("update", "--all",))
    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    current_commit = client.find_previous_commit(source)
    assert current_commit != previous_commit

    attributes = (client.path / ".gitattributes").read_text().split()
    assert "data/output.txt" in attributes


@pytest.mark.parametrize(
    "args", [["dataset", "create", "my-data"], ["dataset", "add", "--create", "my-data", "README.md"]]
)
def test_immutability_at_creation(runner, client, args):
    """Test first dataset's ID is the same as metadata directory."""
    assert 0 == runner.invoke(cli, args).exit_code

    dataset = client.load_dataset("my-data")
    assert str(dataset.path).endswith(dataset.identifier)


def test_immutability_for_files(directory_tree, runner, client):
    """Test dataset's ID changes after a change to dataset files."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = client.load_dataset("my-data")

    # Add some files
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code

    dataset = client.load_dataset("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    # Add the same files again; it should mutate because files addition dates change
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--overwrite", str(directory_tree)]).exit_code

    dataset = client.load_dataset("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)
    old_dataset = dataset

    # Remove some files
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "-I", "file1", "--yes"]).exit_code

    dataset = client.load_dataset("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_for_adding_files_twice(directory_tree, runner, client):
    """Test dataset's ID does not change changes if the same files are added again."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--create", str(directory_tree)]).exit_code
    old_dataset = client.load_dataset("my-data")

    assert 1 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code
    dataset = client.load_dataset("my-data")

    assert old_dataset._id == dataset._id


def test_immutability_after_edit(runner, client):
    """Test dataset's ID changes after editing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = client.load_dataset("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"]).exit_code

    dataset = client.load_dataset("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_after_external_update(runner, client, directory_tree):
    """Test dataset's ID changes after updating external files."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code
    old_dataset = client.load_dataset("my-data")

    directory_tree.joinpath("file1").write_text("some updates")
    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code
    dataset = client.load_dataset("my-data")

    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_immutability_after_no_update(runner, client, directory_tree):
    """Test dataset's ID does not changes if no external file is updated."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code
    old_dataset = client.load_dataset("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code
    dataset = client.load_dataset("my-data")

    assert dataset._id == old_dataset._id


def test_immutability_for_tags(runner, client):
    """Test dataset's ID does NOT changes after a change to dataset tags."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = client.load_dataset("my-data")

    # Add a tag
    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "new-tag"]).exit_code

    dataset = client.load_dataset("my-data")
    assert old_dataset._id == dataset._id
    old_dataset = dataset

    # Remove a tag
    assert 0 == runner.invoke(cli, ["dataset", "rm-tags", "my-data", "new-tag"]).exit_code

    dataset = client.load_dataset("my-data")
    assert old_dataset._id == dataset._id


def test_immutability_after_remove(directory_tree, runner, client):
    """Test dataset is mutated one final time when it is removed."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    old_dataset = client.load_dataset("my-data")

    assert 0 == runner.invoke(cli, ["dataset", "rm", "my-data"]).exit_code

    assert client.load_dataset("my-data") is None

    # Checkout previous commit that has dataset's final version
    client.repo.git.checkout("HEAD~")

    dataset = client.load_dataset("my-data")
    assert_dataset_is_mutated(old=old_dataset, new=dataset)


def test_datasets_provenance_after_create(runner, client_with_new_graph):
    """Test datasets provenance is updated after creating a dataset."""
    assert (
        0
        == runner.invoke(
            cli,
            [
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
            ],
        ).exit_code
    )

    dataset = client_with_new_graph.datasets_provenance.get_by_name("my-data")[0]

    assert "Long Title" == dataset.title
    assert "my-data" == dataset.name
    assert "some description here" == dataset.description
    assert "John Doe" in [c.name for c in dataset.creators]
    assert "john.doe@mail.ch" in [c.email for c in dataset.creators]
    assert "John Smiths" in [c.name for c in dataset.creators]
    assert "john.smiths@mail.ch" in [c.email for c in dataset.creators]
    assert {"keyword-1", "keyword-2"} == set(dataset.keywords)
    assert client_with_new_graph.project._id == dataset.project._id

    assert not client_with_new_graph.repo.is_dirty()


def test_datasets_provenance_after_edit(runner, client_with_new_graph):
    """Test datasets provenance is updated after editing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"]).exit_code

    dataset = client_with_new_graph.load_dataset("my-data")
    current_version = client_with_new_graph.datasets_provenance.get(dataset.identifier)
    old_version = client_with_new_graph.datasets_provenance.get(dataset.original_identifier)

    assert current_version.identifier != old_version.identifier
    assert current_version.name == old_version.name
    assert set() == set(old_version.keywords)
    assert {"new-data"} == set(current_version.keywords)


def test_datasets_provenance_after_add(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is updated after adding data to a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree / "file1")]).exit_code

    dataset = client_with_new_graph.datasets_provenance.get_by_name("my-data")[0]
    path = os.path.join(DATA_DIR, "my-data", "file1")
    file_ = dataset.find_file(path)
    object_hash = client_with_new_graph.repo.git.rev_parse(f"HEAD:{path}")

    assert object_hash in file_.entity._id
    assert path in file_.entity._id
    assert object_hash == file_.entity.checksum
    assert path == file_.entity.path


def test_datasets_provenance_not_updated_after_same_add(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is not updated if adding same files multiple times."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "--create", str(directory_tree)]).exit_code
    commit_sha_before = client_with_new_graph.repo.head.object.hexsha

    assert 1 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code
    commit_sha_after = client_with_new_graph.repo.head.object.hexsha

    datasets = client_with_new_graph.datasets_provenance.get_by_name("my-data")

    assert 1 == len(datasets)
    assert commit_sha_before == commit_sha_after


def test_datasets_provenance_after_file_unlink(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is updated after removing data."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code

    dataset = client_with_new_graph.load_dataset("my-data")
    current_version = client_with_new_graph.datasets_provenance.get(dataset.identifier)
    old_version = client_with_new_graph.datasets_provenance.get(dataset.original_identifier)
    path = os.path.join(DATA_DIR, "my-data", directory_tree.name, "file1")

    assert 1 == len(current_version.files)
    assert {path} == {f.entity.path for f in current_version.files}
    assert 3 == len(old_version.files)


def test_datasets_provenance_after_remove(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is updated after removing a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", "-c", str(directory_tree)]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "rm", "my-data"]).exit_code

    datasets = client_with_new_graph.datasets_provenance.get_by_name("my-data")
    current_version = next(d for d in datasets if d.identifier != d.original_identifier)

    assert current_version.date_deleted is not None


@pytest.mark.serial
def test_datasets_provenance_after_update(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is updated after updating a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "add", "-c", "--external", "my-data", str(directory_tree)]).exit_code

    directory_tree.joinpath("file1").write_text("some updates")
    assert 0 == runner.invoke(cli, ["dataset", "update", "--external"]).exit_code

    dataset = client_with_new_graph.load_dataset("my-data")
    current_version = client_with_new_graph.datasets_provenance.get(dataset.identifier)

    assert current_version.identifier != current_version.original_identifier


def test_datasets_provenance_after_adding_tag(runner, client_with_new_graph):
    """Test datasets provenance is updated after tagging a dataset."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code

    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    datasets = client_with_new_graph.datasets_provenance.get_by_name("my-data")

    assert 1 == len(datasets)
    assert "42.0" in [t.name for t in datasets[0].tags]


def test_datasets_provenance_after_removing_tag(runner, client_with_new_graph):
    """Test datasets provenance is updated after removing a dataset's tag."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code

    assert 0 == runner.invoke(cli, ["dataset", "rm-tags", "my-data", "42.0"]).exit_code

    datasets = client_with_new_graph.datasets_provenance.get_by_name("my-data")

    assert 1 == len(datasets)
    assert "42.0" not in [t.name for t in datasets[0].tags]


def test_datasets_provenance_multiple(runner, client_with_new_graph, directory_tree):
    """Test datasets provenance is updated after multiple dataset operations."""
    assert 0 == runner.invoke(cli, ["dataset", "create", "my-data"]).exit_code
    version_1 = client_with_new_graph.load_dataset("my-data")
    assert 0 == runner.invoke(cli, ["dataset", "edit", "my-data", "-k", "new-data"]).exit_code
    version_2 = client_with_new_graph.load_dataset("my-data")
    assert 0 == runner.invoke(cli, ["dataset", "add", "my-data", str(directory_tree)]).exit_code
    version_3 = client_with_new_graph.load_dataset("my-data")
    assert 0 == runner.invoke(cli, ["dataset", "tag", "my-data", "42.0"]).exit_code
    version_4 = client_with_new_graph.load_dataset("my-data")
    assert 0 == runner.invoke(cli, ["dataset", "unlink", "my-data", "--include", "*/dir1/*"], input="y").exit_code
    version_5 = client_with_new_graph.load_dataset("my-data")

    datasets_provenance = client_with_new_graph.datasets_provenance

    assert datasets_provenance.get(version_1.identifier)
    assert datasets_provenance.get(version_2.identifier)
    assert datasets_provenance.get(version_3.identifier)
    assert datasets_provenance.get(version_4.identifier)
    assert datasets_provenance.get(version_5.identifier)
