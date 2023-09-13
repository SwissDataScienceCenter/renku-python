#
# Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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

from renku.command.dataset import (
    add_to_dataset_command,
    create_dataset_command,
    file_unlink_command,
    list_datasets_command,
    list_files_command,
)
from renku.core import errors
from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.dataset.context import DatasetContext
from renku.core.dataset.dataset_add import add_to_dataset
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.tag import get_dataset_by_tag
from renku.core.errors import ParameterError
from renku.core.util.contexts import chdir
from renku.core.util.git import get_git_user
from renku.core.util.urls import get_slug
from renku.domain_model.dataset import Dataset, Url
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.infrastructure.repository import Repository
from tests.utils import assert_dataset_is_mutated, load_dataset, raises


@pytest.mark.serial
@pytest.mark.parametrize(
    "scheme, path, overwrite, error",
    [
        ("", "temp", False, None),
        ("file://", "temp", True, None),
        ("", "tempp", False, errors.ParameterError),
        ("http://", "example.com/file1", False, None),
        ("https://", "example.com/file1", True, None),
    ],
)
def test_data_add(scheme, path, overwrite, error, project_with_injection, directory_tree, dataset_responses):
    """Test data import."""
    with raises(error):
        if path == "temp":
            path = str(directory_tree / "file1")

        dataset = add_to_dataset("dataset", [f"{scheme}{path}"], overwrite=overwrite, create=True)

        target_path = os.path.join(DATA_DIR, "dataset", "file1")

        with open(target_path) as f:
            assert f.read() == "file1 content"

        assert dataset.find_file(target_path)

        # check that the imported file is read-only
        assert not os.access(target_path, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        # check the linking
        if scheme in ("", "file://"):
            shutil.rmtree("./data/dataset")
            # NOTE: To simulate loading from persistent storage like what a separate renku command would do
            dataset.freeze()
            add_to_dataset("dataset", [f"{scheme}{path}"], overwrite=True)
            assert os.path.exists(target_path)


@pytest.mark.parametrize(
    "slash, target",
    [
        (False, "data/dataset/dir1/file2"),
        (True, "data/dataset/file2"),
    ],
)
def test_data_add_trailing_slash(slash, target, directory_tree, project_with_injection):
    """Test recursive data imports."""

    dataset = add_to_dataset("dataset", [str(directory_tree / "dir1") + ("/" if slash else "")], create=True)

    file = next(f for f in dataset.files if f.entity.path.endswith("file2"))
    assert file.entity.path == target


def test_data_add_recursive(directory_tree, project_with_injection):
    """Test recursive data imports."""
    dataset = add_to_dataset("dataset", [str(directory_tree / "dir1")], create=True)

    assert os.path.basename(os.path.dirname(dataset.files[0].entity.path)) == "dir1"


def test_creator_parse():
    """Test that different options for specifying creators work."""
    creator = Person(name="me", email="me@example.com")
    dataset = Dataset(name="dataset", creators=[creator])
    assert creator in dataset.creators

    # email check
    with pytest.raises(ValueError):
        Person(name="me", email="meexample.com")

    # creators must be a set or list of dicts or Person
    with pytest.raises(ValueError):
        Dataset(name="dataset", creators=["name"])


def test_creators_with_same_email(project_with_injection):
    """Test creators with different names and same email address."""
    with DatasetContext(slug="dataset", create=True) as dataset:
        dataset.creators = [Person(name="me", email="me@example.com"), Person(name="me2", email="me@example.com")]
        DatasetsProvenance().add_or_update(dataset)

    dataset = load_dataset("dataset")

    assert 2 == len(dataset.creators)
    assert {c.name for c in dataset.creators} == {"me", "me2"}


def test_create_dataset_command_custom_message(project):
    """Test create dataset custom message."""
    create_dataset_command().with_commit_message("my dataset").with_database(write=True).build().execute(
        "ds1", name="", description="", creators=[]
    )

    last_commit = Repository(".").head.commit
    assert "my dataset" == last_commit.message.splitlines()[0]


def test_list_datasets_default(project):
    """Test a default dataset listing."""
    create_dataset_command().with_commit_message("my dataset").with_database(write=True).build().execute(
        "ds1", name="", description="", creators=[]
    )

    datasets = list_datasets_command().with_database().build().execute().output

    assert isinstance(datasets, list)
    assert "ds1" in [dataset.name for dataset in datasets]


def test_list_files_default(project, tmpdir):
    """Test a default file listing."""
    create_dataset_command().with_commit_message("my dataset").with_database(write=True).build().execute(
        "ds1", name="", description="", creators=[]
    )
    data_file = tmpdir / Path("some-file")
    data_file.write_text("1,2,3", encoding="utf-8")

    add_to_dataset_command().build().execute("ds1", [str(data_file)])
    files = list_files_command().build().execute(datasets=["ds1"]).output

    assert isinstance(files, list)
    assert "some-file" in [Path(f.entity.path).name for f in files]


def test_unlink_default(directory_tree, project):
    """Test unlink default behaviour."""
    with chdir(project.path):
        create_dataset_command().with_database(write=True).build().execute("dataset")
        add_to_dataset_command().with_database(write=True).build().execute(
            dataset_slug="dataset", urls=[str(directory_tree / "dir1")]
        )

    with pytest.raises(ParameterError):
        file_unlink_command().build().execute("dataset", (), ())


@pytest.mark.xfail
def test_mutate(project):
    """Test metadata change after dataset mutation."""
    dataset = Dataset(
        name="my-dataset",
        creators=[Person.from_string("John Doe <john.doe@mail.com>")],
        date_published=datetime.datetime.now(datetime.timezone.utc),
        same_as=Url(url_str="http://some-url"),
    )
    old_dataset = copy.deepcopy(dataset)

    dataset.mutate()

    mutator = get_git_user(project.repository)
    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)


@pytest.mark.xfail
def test_mutator_is_added_once(project):
    """Test mutator of a dataset is added only once to its creators list."""
    mutator = get_git_user(project.repository)

    dataset = Dataset(
        name="my-dataset",
        creators=[mutator],
        date_published=datetime.datetime.now(datetime.timezone.utc),
        same_as=Url(url_str="http://some-url"),
    )
    old_dataset = copy.deepcopy(dataset)

    dataset.mutate()

    assert_dataset_is_mutated(old=old_dataset, new=dataset, mutator=mutator)
    assert 1 == len(dataset.creators)


@pytest.mark.xfail
def test_mutate_is_done_once():
    """Test dataset mutation can be done only once."""
    dataset = Dataset(name="my-dataset", creators=[])
    before_id = dataset._id

    dataset.mutate()
    after_id = dataset._id

    assert before_id != after_id

    dataset.mutate()
    assert after_id == dataset._id


@pytest.mark.parametrize(
    "name, slug",
    [
        ("UPPER-CASE", "upper-case"),
        (" a name  ", "a_name"),
        ("..non-alphanumeric start or end-_", "non-alphanumeric_start_or_end"),
        ("double ..__._- non-alphanumeric", "double_non-alphanumeric"),
        ("double..dots", "double.dots"),
        ("nön-àsçîï", "non-ascii"),
        ("ends in .lock", "ends_in_lock"),
    ],
)
def test_dataset_name_slug(name, slug):
    """Test slug generation from name."""
    assert slug == get_slug(name)


@pytest.mark.integration
def test_get_dataset_by_tag(with_injection, tmp_path):
    """Test getting datasets by a given tag."""
    url = "https://gitlab.dev.renku.ch/renku-python-integration-tests/lego-datasets.git"
    repository = Repository.clone_from(url=url, path=tmp_path / "repo")

    with project_context.with_path(repository.path), with_injection():
        dataset_gateway = DatasetGateway()

        parts_dataset = dataset_gateway.get_by_slug("parts")

        returned_datasets = get_dataset_by_tag(dataset=parts_dataset, tag="v1")
        selected_tag = next(tag for tag in dataset_gateway.get_all_tags(parts_dataset) if tag.name == "v1")

        assert selected_tag.dataset_id.value == returned_datasets.id

        # Get a non-existing tag
        assert get_dataset_by_tag(dataset=parts_dataset, tag="v42") is None
