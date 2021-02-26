# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 Swiss Data Science Center (SDSC)
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
"""Dataverse unit tests."""
import pytest

from renku.core.commands.providers.dataverse import DataverseExporter, _DataverseDeposition
from renku.core.commands.providers.doi import DOIProvider
from renku.core.errors import RenkuImportError


def test_dataverse_exporter_init():
    """Check construction of a dataverse exporter."""
    exporter = DataverseExporter(dataset="my-dataset", access_token="let-me-in",)

    assert exporter
    assert "my-dataset" == exporter.dataset
    assert "let-me-in" == exporter.access_token


def test_dataverse_deposition_init():
    """Check construction of dataverse deposition."""
    deposit = _DataverseDeposition(access_token="let-me-in", server_url="http://sss",)

    assert deposit
    assert "let-me-in" == deposit.access_token
    assert "http://sss" == deposit.server_url
    assert not deposit.dataset_pid


def test_doi_serializer():
    """Check doi serialization."""
    data = {
        "id": 1,
        "doi": "doi@doi.35446",
        "url": "https://doi.org/35446",
        "type": "dataset",
        "categories": [1, 2, 3],
        "author": "myname",
        "contributor": "mycontributor",
        "version": 34,
        "issued": "15.09.2020",
        "title": "my title",
        "abstract": "my awesome dataset",
        "language": "english",
        "publisher": "me",
        "container-title": "yes",
        "missing": True,
    }

    with pytest.raises(RenkuImportError):
        DOIProvider._serialize(data)
