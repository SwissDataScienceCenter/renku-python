# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Dataset core tests."""

from pathlib import Path

import pytest

from renku.core import errors
from renku.core.dataset.dataset_add import get_dataset_file_path_within_dataset
from renku.core.dataset.providers.s3 import parse_s3_uri
from renku.domain_model.dataset import Dataset


@pytest.mark.parametrize(
    "uri, endpoint, bucket, path",
    [
        ("s3://no.bucket.path/", "no.bucket.path", "", ""),
        ("s3://no.bucket.path///", "no.bucket.path", "", ""),
        ("s3://no.path/bucket/", "no.path", "bucket", ""),
        ("S3://uppercase.scheme/bucket/path", "uppercase.scheme", "bucket", "path"),
        ("s3://slashes.are.stripped///bucket///path/to/data//", "slashes.are.stripped", "bucket", "path/to/data"),
    ],
)
def test_valid_s3_uri(uri, endpoint, bucket, path):
    """Test valid s3 URI are parsed correctly."""
    parsed_endpoint, parsed_bucket, parsed_path = parse_s3_uri(uri=uri)

    assert endpoint == parsed_endpoint
    assert bucket == parsed_bucket
    assert path == parsed_path


@pytest.mark.parametrize("uri", ["https://invalid.scheme/bucket/", "s3:no-endpoint/bucket/path"])
def test_invalid_s3_uri(uri):
    """Test invalid s3 URI raise an error."""
    with pytest.raises(errors.ParameterError):
        parse_s3_uri(uri=uri)


@pytest.mark.parametrize(
    "entity_path, within_dataset_path",
    [
        ("data/my-data/path.csv", "path.csv"),
        ("data/my-data/within/sub/path.csv", "within/sub/path.csv"),
        ("path.csv", "path.csv"),
        ("within/sub/path.csv", "within/sub/path.csv"),
    ],
)
def test_get_within_dataset_path(entity_path, within_dataset_path):
    """Test getting paths of dataset files relative to dataset's datadir."""
    dataset = Dataset(name="my-data", datadir=Path("data/my-data"))

    path = get_dataset_file_path_within_dataset(dataset=dataset, entity_path=entity_path)

    assert within_dataset_path == str(path)
