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
"""Datasets Provenance."""

from datetime import datetime
from typing import List, Optional

from renku.core import errors
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.models.dataset import Dataset
from renku.core.models.provenance.agent import Person
from renku.core.utils import communication


class DatasetsProvenance:
    """A set of datasets."""

    dataset_gateway = inject.attr(IDatasetGateway)

    @property
    def datasets(self) -> List[Dataset]:
        """Return an iterator of datasets."""
        return self.dataset_gateway.get_all_datasets()

    def get_by_id(self, id: str, immutable=False) -> Optional[Dataset]:
        """Return a dataset by its id."""
        try:
            dataset = self.dataset_gateway.get_by_id(id)
        except errors.ObjectNotFoundError:
            pass
        else:
            assert isinstance(dataset, Dataset)
            if dataset.immutable and immutable:
                return dataset

            return dataset.copy()

    def get_by_name(self, name: str, immutable=False) -> Optional[Dataset]:
        """Return a dataset by its name."""
        dataset = self.dataset_gateway.get_by_name(name)
        if not dataset:
            return
        if not dataset.immutable or immutable:
            return dataset

        return dataset.copy()

    def get_provenance(self):
        """Return the provenance for all datasets."""
        return self.dataset_gateway.get_provenance()

    def get_previous_version(self, dataset: Dataset) -> Optional[Dataset]:
        """Return the previous version of a dataset if any."""
        if not dataset.derived_from:
            return
        return self.get_by_id(dataset.derived_from)

    def add_or_update(self, dataset: Dataset, date: datetime = None, creator: Person = None):
        """Add/update a dataset according to its new content.

        NOTE: This functions always mutates the dataset.
        """
        assert isinstance(dataset, Dataset)

        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self.get_by_name(dataset.name)

        if current_dataset:
            assert (
                not current_dataset.is_removed()
            ), f"Adding/Updating a removed dataset '{dataset.name}:{dataset.identifier}'"

            dataset.update_files_from(current_dataset, date=date)

            # NOTE: Always mutate a dataset to make sure an old identifier is not reused
            dataset.derive_from(current_dataset, creator=creator)
        else:
            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

            # NOTE: This happens in migrations of broken projects
            current_dataset = self.get_by_id(dataset.id)
            if current_dataset:
                dataset.replace_identifier()

        self.dataset_gateway.add_or_remove(dataset)

    def add_or_replace(self, dataset: Dataset, date: datetime = None):
        """Add/replace a dataset."""
        assert isinstance(dataset, Dataset)

        current_dataset = self.get_by_name(dataset.name, immutable=True)

        if current_dataset:
            dataset.update_files_from(current_dataset, date=date)

            # NOTE: Copy metadata to the current dataset
            current_dataset.update_metadata_from(dataset)
            current_dataset.dataset_files = dataset.dataset_files
            dataset = current_dataset
        else:
            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

            # NOTE: This happens in migrations of broken projects
            current_dataset = self.get_by_id(dataset.id)
            if current_dataset:
                dataset.replace_identifier()

        self.dataset_gateway.add_or_remove(dataset)

    def remove(self, dataset, date: datetime = None, creator: Person = None):
        """Remove a dataset."""
        assert isinstance(dataset, Dataset)

        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self.dataset_gateway.get_by_name(dataset.name)

        if current_dataset:
            assert not current_dataset.is_removed(), f"Removing a removed dataset '{dataset.name}:{dataset.identifier}'"

            # NOTE: We always assign a new identifier to make sure an old identifier is not reused
            dataset.derive_from(current_dataset, creator=creator)
        else:
            # TODO: Should we raise here when migrating
            communication.warn(f"Deleting non-existing dataset '{dataset.name}'")

            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

            # NOTE: This happens in migrations of broken projects
            current_dataset = self.get_by_id(dataset.id)
            if current_dataset:
                dataset.replace_identifier()

        dataset.remove(date)

        self.dataset_gateway.add_or_remove(dataset)
