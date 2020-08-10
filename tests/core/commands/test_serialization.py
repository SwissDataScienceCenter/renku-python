# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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

import yaml

from renku.core.management.client import LocalClient
from renku.core.models.datasets import Dataset
from renku.core.utils.uuid import is_uuid


def test_dataset_serialization(client, dataset, data_file):
    """Test Dataset serialization."""

    def load_dataset(name):
        with open(str(client.get_dataset_path(name))) as f:
            return yaml.safe_load(f)

    d_dict = load_dataset("dataset")

    expected_fields = [
        "@id",
        "http://www.w3.org/2000/01/rdf-schema#label",
        "http://schema.org/isPartOf",
        "http://schema.org/dateCreated",
        "http://schema.org/creator",
        "http://schema.org/datePublished",
        "http://schema.org/description",
        "http://schema.org/hasPart",
        "http://schema.org/identifier",
        "http://schema.org/inLanguage",
        "http://schema.org/keywords",
        "http://schema.org/license",
        "http://schema.org/name",
        "http://www.w3.org/ns/prov#atLocation",
        "http://schema.org/url",
        "http://schema.org/version",
    ]
    for field in expected_fields:
        assert field in d_dict

    assert not d_dict["http://schema.org/hasPart"]
    client.add_data_to_dataset(dataset, [str(data_file)])
    dataset.to_yaml()
    d_dict = load_dataset("dataset")
    assert d_dict["http://schema.org/hasPart"]


def test_dataset_deserialization(client, dataset):
    """Test Dataset deserialization."""
    dataset_ = Dataset.from_yaml(client.get_dataset_path("dataset"), client=client)

    dataset_types = {
        "date_created": [datetime.datetime],
        "creators": [list],
        "description": [str, type(None)],
        "files": [list],
        "identifier": [str],
        "keywords": [list],
    }

    for attribute, type_ in dataset_types.items():
        assert type(dataset_.__getattribute__(attribute)) in type_

    creator_types = {"email": str, "_id": str, "name": str, "affiliation": str}

    creator = dataset.creators[0]

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
    dataset_broken = Dataset.from_jsonld(dataset.asjsonld(), client=LocalClient("."))
    assert "mailto:None" not in dataset_broken.creators[0]._id


def test_calamus(client, dataset_metadata_before_calamus):
    """Check Calamus loads project correctly."""
    dataset = Dataset.from_jsonld(dataset_metadata_before_calamus, client=LocalClient("."))
    assert "Open Source at Harvard" == dataset.title
    assert "51db02ad-3cba-47e2-84d0-5ee5914bd654" == dataset.identifier
    assert "51db02ad-3cba-47e2-84d0-5ee5914bd654" == dataset._label
    assert "Harvard University" == dataset.creators[0].affiliation
    assert "Durbin, Philip" == dataset.creators[0].name
    assert "Durbin, Philip" == dataset.creators[0].label
    assert dataset.date_created is None
    assert "2019-07-03T00:00:00" == dataset.date_published.isoformat("T")
    assert "The tabular file contains information" in dataset.description
    assert "https://doi.org/10.7910/DVN/TJCLKP" == dataset.same_as.url
    assert "3" == dataset.tags[0].name
    assert "Tag 3 created by renku import" == dataset.tags[0].description
    assert isinstance(dataset.license, dict)
    assert "https://creativecommons.org/publicdomain/zero/1.0/" in str(dataset.license)

    file_ = dataset.find_file("data/dataverse/IQSS-UNF.json")
    assert "https://dataverse.harvard.edu/api/access/datafile/3371500" == file_.url
    assert "2020-06-15T08:37:04.571573+00:00" == file_.added.isoformat("T")
    assert file_.based_on is None

    file_ = dataset.find_file("data/dataverse/git/index.ipynb")
    assert "https://github.com/SwissDataScienceCenter/r10e-ds-py.git" == file_.based_on.url
    assert "notebooks/index.ipynb@f98325d81c700f4b86ee05c2154e94d43ca068b8" == file_.based_on._label
    assert file_.based_on.based_on is None
    assert "https://github.com/SwissDataScienceCenter/r10e-ds-py.git" == file_.url

    file_ = dataset.find_file("data/dataverse/external/data.txt")
    assert file_.external is True
    assert "file://../../../../tmp/data.txt" == file_.url

    file_ = dataset.find_file("data/dataverse/local/result.csv")
    assert file_.external is False
    assert "file://../../../../tmp/result.csv" == file_.url
