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
"""Dataset/DatasetFile view model."""

from typing import Optional

from renku.domain_model.dataset import Dataset, DatasetFile


class DatasetViewModel:
    """A view model for a ``Dataset``."""

    def __init__(self, name: str, same_as: Optional[str]):
        self.name: str = name
        self.same_as: Optional[str] = same_as

    @classmethod
    def from_dataset(cls, dataset: Dataset) -> "DatasetViewModel":
        """Create view model from ``Dataset``."""
        return cls(name=dataset.name, same_as=dataset.same_as.value if dataset.same_as else None)


class DatasetFileViewModel:
    """A view model for a ``DatasetFile``."""

    def __init__(self, path: str, external: bool, deleted: bool, source: Optional[str], dataset: DatasetViewModel):
        self.path: str = path
        self.source: Optional[str] = source
        self.external: bool = external
        self.deleted: bool = deleted
        self.dataset: DatasetViewModel = dataset

    @classmethod
    def from_dataset_file(cls, dataset_file: DatasetFile, dataset: Dataset) -> "DatasetFileViewModel":
        """Create view model from ``DatasetFile``."""
        return cls(
            path=dataset_file.entity.path,
            source=dataset_file.source,
            external=dataset_file.is_external,
            deleted=dataset_file.date_removed is not None,
            dataset=DatasetViewModel.from_dataset(dataset),
        )
