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
"""Datasets Provenance."""

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union, overload
from uuid import UUID

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.util import communication

try:
    from typing_extensions import Literal  # NOTE: Required for Python 3.7 compatibility
except ImportError:
    from typing import Literal  # type: ignore

if TYPE_CHECKING:
    from renku.domain_model.dataset import Dataset, DatasetTag
    from renku.domain_model.provenance.agent import Person


class DatasetsProvenance:
    """A set of datasets."""

    dataset_gateway = inject.attr(IDatasetGateway)

    @property
    def datasets(self) -> List["Dataset"]:
        """Return an iterator of datasets."""
        return self.dataset_gateway.get_all_active_datasets()

    def get_by_id(self, id: str, immutable: bool = False) -> Optional["Dataset"]:
        """Return a dataset by its id."""
        from renku.domain_model.dataset import Dataset

        try:
            dataset = self.dataset_gateway.get_by_id(id)
        except errors.ObjectNotFoundError:
            pass
        else:
            assert isinstance(dataset, Dataset)
            if not dataset.immutable or immutable:
                return dataset

            return dataset.copy()
        return None

    @overload
    def get_by_name(
        self, name: str, *, immutable: bool = False, strict: Literal[False] = False
    ) -> Optional["Dataset"]:  # noqa: D102
        ...

    @overload
    def get_by_name(self, name: str, *, immutable: bool = False, strict: Literal[True]) -> "Dataset":  # noqa: D102
        ...

    def get_by_name(
        self, name: str, immutable: bool = False, strict: bool = False
    ) -> Union[Optional["Dataset"], "Dataset"]:
        """Return a dataset by its name.

        Args:
            name(str): Name of the dataset
            immutable(bool): Whether the dataset will be used as an immutable instance or will be modified (Default
                value = False).
            strict(bool): Whether to raise an exception if the dataset doesn't exist or not (Default value = False)

        Returns:
            Optional[Dataset]: Dataset with the specified name if exists.
        """
        dataset = self.dataset_gateway.get_by_name(name)
        if not dataset:
            if strict:
                raise errors.DatasetNotFound(name=name)

            return None
        if not dataset.immutable or immutable:
            return dataset

        return dataset.copy()

    def get_provenance_tails(self):
        """Return the provenance for all datasets."""
        return self.dataset_gateway.get_provenance_tails()

    def get_previous_version(self, dataset: "Dataset") -> Optional["Dataset"]:
        """Return the previous version of a dataset if any."""
        if dataset.is_derivation():
            return self.get_by_id(dataset.derived_from.url_id)  # type: ignore

        return None

    def add_or_update(self, dataset: "Dataset", date: Optional[datetime] = None, creator: Optional["Person"] = None):
        """Add/update a dataset according to its new content.

        NOTE: This functions always mutates the dataset.
        """
        from renku.domain_model.dataset import Dataset

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

        self.dataset_gateway.add_or_remove(dataset)

    def remove(self, dataset, date: datetime = None, creator: "Person" = None):
        """Remove a dataset."""
        from renku.domain_model.dataset import Dataset

        assert isinstance(dataset, Dataset)

        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self.dataset_gateway.get_by_name(dataset.name)

        if current_dataset:
            assert not current_dataset.is_removed(), f"Removing a removed dataset '{dataset.name}:{dataset.identifier}'"

            # NOTE: We always assign a new identifier to make sure an old identifier is not reused
            dataset.derive_from(current_dataset, creator=creator)
        else:
            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

        dataset.remove(date)
        self.dataset_gateway.add_or_remove(dataset)

    def update_during_migration(
        self,
        dataset: "Dataset",
        commit_sha: str,
        date: Optional[datetime] = None,
        tags: Optional[List["DatasetTag"]] = None,
        remove=False,
        replace=False,
        preserve_identifiers: bool = False,
    ):
        """Add, update, remove, or replace a dataset in migration."""
        from renku.domain_model.dataset import Dataset

        assert isinstance(dataset, Dataset)
        assert not (remove and replace), "Cannot remove and replace"

        def update_dataset(existing, new) -> "Dataset":
            """Update existing dataset with the new dataset metadata."""
            existing.update_metadata_from(new, exclude=["date_created", "derived_from", "same_as"])
            existing.dataset_files = new.dataset_files
            return existing

        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self.get_by_name(dataset.name, immutable=True)

        new_identifier = self._create_dataset_identifier(commit_sha, dataset.identifier)
        dataset_with_same_id = self.get_by_id(dataset.id, immutable=True)

        if dataset_with_same_id and preserve_identifiers:
            dataset.update_files_from(dataset_with_same_id, date=date)

            dataset = update_dataset(existing=dataset_with_same_id, new=dataset)
        elif current_dataset:
            dataset.update_files_from(current_dataset, date=date)

            if replace:
                # NOTE: Copy metadata to the current dataset
                dataset = update_dataset(existing=current_dataset, new=dataset)
            else:
                identifier = new_identifier if dataset_with_same_id else dataset.identifier
                date_created = date if dataset_with_same_id else dataset.date_created
                dataset.derive_from(current_dataset, creator=None, identifier=identifier, date_created=date_created)
        else:
            if remove:
                communication.warn(f"Deleting non-existing dataset '{dataset.name}'")

            if dataset.derived_from:
                communication.warn(
                    f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"
                )
                dataset.derived_from = None

            # NOTE: This happens in migrations of broken projects
            if dataset_with_same_id:
                dataset.replace_identifier(new_identifier)

        if remove:
            dataset.remove()
        else:
            self._process_dataset_tags(dataset, tags)

        self.dataset_gateway.add_or_remove(dataset)

    @staticmethod
    def _create_dataset_identifier(commit_sha: str, identifier: str) -> str:
        uuid = f"{commit_sha[:20]}{identifier[-12:]}"
        return UUID(uuid).hex

    def get_all_tags(self, dataset: "Dataset") -> List["DatasetTag"]:
        """Return the list of all tags for a dataset."""
        return self.dataset_gateway.get_all_tags(dataset)

    def add_tag(self, dataset: "Dataset", tag: "DatasetTag"):
        """Add a tag from a dataset."""
        self.dataset_gateway.add_tag(dataset, tag)

    def remove_tag(self, dataset: "Dataset", tag: "DatasetTag"):
        """Remove a tag from a dataset."""
        self.dataset_gateway.remove_tag(dataset, tag)

    def _process_dataset_tags(self, dataset: "Dataset", tags: Optional[List["DatasetTag"]]):
        from renku.domain_model.dataset import DatasetTag, Url

        if not tags:
            return

        current_tag_names = [t.name for t in self.get_all_tags(dataset)]
        for tag in tags:
            if tag.name in current_tag_names:
                continue
            tag = DatasetTag(
                dataset_id=Url(url_id=dataset.id),
                date_created=tag.date_created,
                description=tag.description,
                name=tag.name,
            )
            self.add_tag(dataset, tag)
