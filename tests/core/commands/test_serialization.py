# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Serialization tests for renku models."""

import datetime
from urllib.parse import urljoin

from renku.core.management.client import LocalClient
from renku.core.models.datasets import Dataset
from renku.core.utils.uuid import is_uuid


def test_dataset_deserialization(client_with_datasets):
    """Test Dataset deserialization."""
    dataset = Dataset.from_yaml(client_with_datasets.get_dataset_path("dataset-1"), client=client_with_datasets)

    dataset_types = {
        "date_created": [datetime.datetime],
        "creators": [list],
        "description": [str, type(None)],
        "files": [list],
        "identifier": [str],
        "keywords": [list],
    }

    for attribute, type_ in dataset_types.items():
        assert type(dataset.__getattribute__(attribute)) in type_

    creator_types = {"email": str, "_id": str, "name": str, "affiliation": str}

    creator = client_with_datasets.load_dataset("dataset-1").creators[0]

    for attribute, type_ in creator_types.items():
        assert type(getattr(creator, attribute)) is type_


def test_dataset_files_empty_metadata(dataset_metadata):
    """Check parsing metadata of dataset files with empty filename."""
    dataset = Dataset.from_jsonld(dataset_metadata, client=LocalClient("."),)
    files = [file.filename for file in dataset.files if not file.filename]

    if files:
        assert None in files


def test_uuid_migration(dataset_metadata, client):
    """Test migration of id with UUID."""
    dataset = Dataset.from_jsonld(dataset_metadata, client=client)

    assert is_uuid(dataset.identifier)
    assert urljoin("https://localhost/datasets/", dataset.identifier) == dataset._id


def test_dataset_creator_email(dataset_metadata):
    """Check that creators without an email are assigned a blank node."""
    # modify the dataset metadata to change the creator
    dataset = Dataset.from_jsonld(dataset_metadata, client=LocalClient("."),)

    dataset.creators[0]._id = "mailto:None"
    dataset_broken = Dataset.from_jsonld(dataset.as_jsonld(), client=LocalClient("."))
    assert "mailto:None" not in dataset_broken.creators[0]._id


def test_calamus(client, dataset_metadata_before_calamus):
    """Check Calamus loads project correctly."""
    dataset = Dataset.from_jsonld(dataset_metadata_before_calamus, client=LocalClient("."))

    file_ = dataset.find_file("data/dataverse/external/data.txt")
    assert file_.external is True
    assert "file://../../../../tmp/data.txt" == file_.url

    file_ = dataset.find_file("data/dataverse/local/result.csv")
    assert file_.external is False
    assert "file://../../../../tmp/result.csv" == file_.url


def test_dataset_with_multiple_project_version(client_with_datasets):
    """Test deserialization of a dataset where contains different project versions."""
    max_version = "42000"

    # Change project version for a single file
    with client_with_datasets.with_dataset("dataset-2") as dataset:
        file_ = dataset.find_file(dataset.data_dir / "file1")
        file_._project.version = max_version

    dataset = client_with_datasets.load_dataset("dataset-2")

    assert {max_version} == {f._project.version for f in dataset.files}
