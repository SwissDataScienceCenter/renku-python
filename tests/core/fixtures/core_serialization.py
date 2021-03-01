# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku core fixtures for serialization testing."""
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def dataset_metadata():
    """Return dataset metadata fixture."""
    from renku.core.models.jsonld import NoDatesSafeLoader

    file_path = Path(__file__).parent / ".." / ".." / "data" / "doi-dataset.yml"

    data = yaml.load(file_path.read_text(), Loader=NoDatesSafeLoader)
    yield data


@pytest.fixture
def dataset_metadata_before_calamus():
    """Return dataset metadata fixture."""
    from renku.core.models.jsonld import NoDatesSafeLoader

    path = Path(__file__).parent / ".." / ".." / "data" / "dataset-v0.10.4-before-calamus.yml"
    yield yaml.load(path.read_text(), Loader=NoDatesSafeLoader)
