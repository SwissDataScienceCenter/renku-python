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
"""Dataset tests."""
import copy
import datetime
import os
import shutil
import stat
from pathlib import Path

import pytest
from git import Repo

from renku.core import errors
from renku.core.commands.dataset import add_to_dataset, create_dataset, file_unlink, list_datasets, list_files
from renku.core.errors import OperationError, ParameterError
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.datasets import Dataset
from renku.core.models.provenance.agents import Person
from renku.core.utils.contexts import chdir
from tests.utils import assert_dataset_is_mutated, raises


@pytest.mark.serial
@pytest.mark.parametrize(
    "scheme, path, overwrite, error",
    [
        ("", "temp", False, None),
        ("file://", "temp", True, None),
        ("", "tempp", False, errors.ParameterError),
        ("http://", "example.com/file1", False, None),
        ("https://", "example.com/file1", True, None),
        ("bla://", "file", False, errors.UrlSchemeNotSupported),
    ],
)
def test_data_add(scheme, path, overwrite, error, client, directory_tree, dataset_responses):
    """Test data import."""
    with raises(error):
        if path == "temp":
            path = str(directory_tree / "file1")

        with client.with_dataset("dataset", create=True) as d:
            d.creators = [Person(name="me", email="me@example.com", id="me_id")]

            client.add_data_to_dataset(d, ["{}{}".format(scheme, path)], overwrite=overwrite)

        target_path = os.path.join(DATA_DIR, "dataset", "file1")

        with open(target_path) as f:
            assert f.read() == "123"

        assert d.find_file(target_path)

        # check that the imported file is read-only
        assert not os.access(target_path, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        # check the linking
        if scheme in ("", "file://"):
            shutil.rmtree("./data/dataset")
            with client.with_dataset("dataset") as d:
                d.creators = [Person(name="me", email="me@example.com", id="me_id")]
                client.add_data_to_dataset(d, ["{}{}".format(scheme, path)], overwrite=True)
            assert os.path.exists(target_path)


def test_data_add_recursive(directory_tree, client):
    """Test recursive data imports."""
    with client.with_dataset("dataset", create=True) as dataset:
        dataset.creators = [Person(name="me", email="me@example.com", id="me_id")]
        client.add_data_to_dataset(dataset, [str(directory_tree / "dir1")])

        assert os.path.basename(os.path.dirname(dataset.files[0].path)) == "dir1"


def test_git_repo_import(client_with_datasets, data_repository):
    """Test adding data from a local git repository."""
    dataset = client_with_datasets.load_dataset("dataset-1")
    client_with_datasets.add_data_to_dataset(dataset, [os.path.join(os.path.dirname(data_repository.git_dir), "dir1")])
    path2 = os.path.join(DATA_DIR, "dataset-1", "dir1", "file2")
    path3 = os.path.join(DATA_DIR, "dataset-1", "dir1", "file3")

    assert os.stat(path2)
    assert os.stat(path3)
    assert {path2, path3} == {f.path for f in dataset.files}


@pytest.mark.parametrize(
    "creators",
    [
        [Person(name="me", email="me@example.com")],
        [{"http://schema.org/name": "me", "http://schema.org/email": "me@example.com",}],
    ],
)
def test_creator_parse(creators):
    """Test that different options for specifying creators work."""
    dataset = Dataset(name="dataset", creators=creators)
    creator = Person(name="me", email="me@example.com")
    assert creator in dataset.creators

    # email check
    with pytest.raises(ValueError):
        Person(name="me", email="meexample.com")

    # creators must be a set or list of dicts or Person
    with pytest.raises(ValueError):
        Dataset(name="dataset", creators=["name"])


def test_creators_with_same_email(tmp_path):
    """Test creators with different names and same email address."""
    creators = [Person(name="me", email="me@example.com"), Person(name="me2", email="me@example.com")]
    dataset = Dataset(name="dataset", creators=creators)
    path = tmp_path / "dataset.yml"
    dataset.to_yaml(path=path)

    dataset = Dataset.from_yaml(path)
    assert 1 == len(dataset.creators)
    assert dataset.creators[0].name in ["me", "me2"]


def test_dataset_serialization(client_with_datasets):
    """Test dataset (de)serialization."""
    dataset = client_with_datasets.load_dataset("dataset-1")

    def read_value(key):
        return dataset_metadata.get(key)[0].get("@value")

    flattened_metadata = dataset.as_jsonld()
    dataset = Dataset.from_jsonld(flattened_metadata)

    # assert that all attributes found in metadata are set in the instance
    assert dataset.date_created
    assert dataset.creators
    assert dataset.identifier
    assert dataset.title
    assert dataset.path
    assert dataset._project

    dataset_metadata = [m for m in flattened_metadata if "Dataset" in str(m["@type"])][0]

    # check values
    assert str(dataset.date_created.isoformat()) == read_value("http://schema.org/dateCreated")
    assert dataset.identifier == read_value("http://schema.org/identifier")
    assert dataset.title == read_value("http://schema.org/name")
    assert dataset.path == read_value("http://www.w3.org/ns/prov#atLocation")


def test_create_dataset_custom_message(project):
    """Test create dataset custom message."""
    create_dataset().with_commit_message("my dataset").build().execute("ds1", title="", description="", creators=[])

    last_commit = Repo(".").head.commit
    assert "my dataset" == last_commit.message


def test_list_datasets_default(project):
    """Test a default dataset listing."""
    create_dataset().with_commit_message("my dataset").build().execute("ds1", title="", description="", creators=[])

    datasets = list_datasets().build().execute().output

    assert isinstance(datasets, list)
    assert "ds1" in [dataset.title for dataset in datasets]


def test_list_files_default(project, tmpdir):
    """Test a default file listing."""
    create_dataset().with_commit_message("my dataset").build().execute("ds1", title="", description="", creators=[])
    data_file = tmpdir / Path("some-file")
    data_file.write_text("1,2,3", encoding="utf-8")

    add_to_dataset().build().execute([str(data_file)], "ds1")
    files = list_files().build().execute(datasets=["ds1"]).output

    assert isinstance(files, list)
    assert "some-file" in [file_.name for file_ in files]


def test_unlink_default(directory_tree, client):
    """Test unlink default behaviour."""
    with chdir(client.path):
        create_dataset().build().execute("dataset")
        add_to_dataset().build().execute([str(directory_tree / "dir1")], "dataset")

    with pytest.raises(ParameterError):
        file_unlink().build().execute("dataset", (), ())


def test_mutate(client):
    """Test metadata change after dataset mutation."""
    dataset = Dataset(
        client=client,
        name="my-dataset",
        creators=[Person.from_string("John Doe <john.doe@mail.com>")],
        date_published=datetime.datetime.now(datetime.timezone.utc),
        same_as="http://some-url",
    )
    old_dataset = copy.deepcopy(dataset)

    dataset.mutate()

    mutator = Person.from_git(client.repo)
    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)


def test_mutator_is_added_once(client):
    """Test mutator of a dataset is added only once to its creators list."""
    mutator = Person.from_git(client.repo)

    dataset = Dataset(
        client=client,
        name="my-dataset",
        creators=[mutator],
        date_published=datetime.datetime.now(datetime.timezone.utc),
        same_as="http://some-url",
    )
    old_dataset = copy.deepcopy(dataset)

    dataset.mutate()

    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)
    assert 1 == len(dataset.creators)


def test_mutate_is_done_once():
    """Test dataset mutation can be done only once."""
    dataset = Dataset(name="my-dataset", creators=[])
    before_id = dataset._id

    dataset.mutate()
    after_id = dataset._id

    assert before_id != after_id

    dataset.mutate()
    assert after_id == dataset._id


def test_cannot_mutate_immutable_dataset():
    """Check immutable datasets cannot be modified."""
    dataset = Dataset(name="my-dataset", creators=[], immutable=True)

    with pytest.raises(OperationError):
        dataset.mutate()
