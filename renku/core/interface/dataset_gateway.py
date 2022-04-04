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
"""Renku dataset gateway interface."""

from abc import ABC
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from renku.domain_model.dataset import Dataset, DatasetTag


class IDatasetGateway(ABC):
    """Interface for the DatasetGateway."""

    def get_by_id(self, id: str) -> "Dataset":
        """Get a dataset by id."""
        raise NotImplementedError

    def get_by_name(self, name: str) -> Optional["Dataset"]:
        """Get a dataset by id."""
        raise NotImplementedError

    def get_all_active_datasets(self) -> List["Dataset"]:
        """Get all datasets."""
        raise NotImplementedError

    def get_provenance_tails(self) -> List["Dataset"]:
        """Return the provenance for all datasets."""
        raise NotImplementedError

    def get_all_tags(self, dataset: "Dataset") -> List["DatasetTag"]:
        """Return the list of all tags for a dataset."""
        raise NotImplementedError

    def add_tag(self, dataset: "Dataset", tag: "DatasetTag"):
        """Add a tag from a dataset."""
        raise NotImplementedError

    def remove_tag(self, dataset: "Dataset", tag: "DatasetTag"):
        """Remove a tag from a dataset."""
        raise NotImplementedError

    def add_or_remove(self, dataset: "Dataset") -> None:
        """Add or remove a dataset."""
        raise NotImplementedError
