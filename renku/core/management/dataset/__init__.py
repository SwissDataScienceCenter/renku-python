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
"""Renku management dataset logic."""

from typing import Optional

from renku.core import errors
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.models.dataset import Dataset


def get_dataset(name, strict=False, immutable=False) -> Optional[Dataset]:
    """Return a dataset based on its name."""
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(name, immutable=immutable)

    if not dataset and strict:
        raise errors.DatasetNotFound(name=name)

    return dataset
