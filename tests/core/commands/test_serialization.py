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

import pytest

from renku.core.management.migrations.models import v9 as old_datasets
from renku.core.utils.uuid import is_uuid


def test_dataset_deserialization(client_with_datasets, load_dataset_with_injection):
    """Test Dataset deserialization."""
    dataset = load_dataset_with_injection("dataset-1", client_with_datasets)

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

    creator_types = {"email": str, "id": str, "name": str, "affiliation": str}

    creator = load_dataset_with_injection("dataset-1", client_with_datasets).creators[0]

    for attribute, type_ in creator_types.items():
        assert type(getattr(creator, attribute)) is type_


@pytest.mark.xfail
def test_uuid_migration(dataset_metadata, client):
    """Test migration of id with UUID."""
    dataset = old_datasets.Dataset.from_jsonld(dataset_metadata)

    assert is_uuid(dataset.identifier)
    assert urljoin("/datasets/", dataset.identifier) == dataset.id


def test_dataset_creator_email(dataset_metadata):
    """Check that creators without an email are assigned a blank node."""
    # modify the dataset metadata to change the creator
    dataset = old_datasets.Dataset.from_jsonld(dataset_metadata)

    dataset.creators[0].id = "mailto:None"
    dataset_broken = old_datasets.Dataset.from_jsonld(dataset.as_jsonld())
    assert "mailto:None" not in dataset_broken.creators[0].id
