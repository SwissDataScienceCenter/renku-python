# -*- coding: utf-8 -*-
#
# Copyright 2017-2019- Swiss Data Science Center (SDSC)
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

import yaml


def test_dataset_serialization(client, dataset, data_file):
    """Test Dataset serialization."""

    def load_dataset(name):
        with open(str(client.dataset_path(name))) as f:
            return yaml.safe_load(f)

    d_dict = load_dataset('dataset')

    expected_fields = [
        '_id', '_label', '_project', 'created', 'creator', 'date_published',
        'description', 'files', 'identifier', 'in_language', 'keywords',
        'license', 'name', 'path', 'url', 'version'
    ]
    for field in expected_fields:
        assert field in d_dict

    assert not d_dict['files']
    client.add_data_to_dataset(dataset, [str(data_file)])
    dataset.to_yaml()
    d_dict = load_dataset('dataset')
    assert d_dict['files']


def test_dataset_deserialization(client, dataset):
    """Test Dataset deserialization."""
    from renku.models.datasets import Dataset
    dataset_ = Dataset.from_yaml(client.dataset_path('dataset'), client=client)

    dataset_types = {
        'created': datetime.datetime,
        'creator': list,
        'description': str,
        'files': list,
        'identifier': str,
        'keywords': list,
    }

    for attribute, type_ in dataset_types.items():
        assert type(dataset_.__getattribute__(attribute)) is type_

    creator_types = {'email': str, '_id': str, 'name': str, 'affiliation': str}

    creator = dataset.creator[0]

    for attribute, type_ in creator_types.items():
        assert type(creator.get(attribute)) is type_
