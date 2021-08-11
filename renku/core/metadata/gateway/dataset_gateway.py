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
"""Renku dataset gateway interface."""

from typing import List, Optional

from persistent.list import PersistentList

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.metadata.database import Database
from renku.core.models.dataset import Dataset, DatasetTag


class DatasetGateway(IDatasetGateway):
    """Gateway for dataset database operations."""

    database = inject.attr(Database)

    def get_by_id(self, id: str) -> Optional[Dataset]:
        """Get a dataset by id."""
        return self.database.get_by_id(id)

    def get_by_name(self, name: str) -> Optional[Dataset]:
        """Get a dataset by id."""
        return self.database["datasets"].get(name)

    def get_all_datasets(self) -> List[Dataset]:
        """Return all datasets."""
        return list(self.database["datasets"].values())

    def get_provenance(self) -> List[Dataset]:
        """Return the provenance for all datasets."""
        return list(self.database["datasets-provenance-tails"].values())

    def get_all_tags(self, dataset: Dataset) -> List[DatasetTag]:
        """Return the list of all tags for a dataset."""
        return list(self.database["datasets-tags"].get(dataset.name, []))

    def add_tag(self, dataset: Dataset, tag: DatasetTag):
        """Add a tag from a dataset."""
        tags: PersistentList = self.database["datasets-tags"].get(dataset.name)
        if not tags:
            tags = PersistentList()
            self.database["datasets-tags"].add(tags, key=dataset.name)

        assert tag.dataset_id == dataset.id, f"Tag has wrong dataset id: {tag.dataset_id} != {dataset.id}"

        tags.append(tag)

    def remove_tag(self, dataset: Dataset, tag: DatasetTag):
        """Remove a tag from a dataset."""
        tags: PersistentList = self.database["datasets-tags"].get(dataset.name)
        for t in tags:
            if t.name == tag.name:
                tags.remove(t)
                break

    def add_or_remove(self, dataset: Dataset) -> None:
        """Add or remove a dataset."""

        if dataset.date_removed:
            self.database["datasets"].pop(dataset.name, None)
            self.database["datasets-tags"].pop(dataset.name, None)
        else:
            self.database["datasets"].add(dataset)

        self.database["datasets-provenance-tails"].pop(dataset.derived_from, None)
        self.database["datasets-provenance-tails"].add(dataset)
