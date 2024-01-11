# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

import deal
from persistent.list import PersistentList

from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.domain_model.dataset import Dataset, DatasetTag
from renku.domain_model.project_context import project_context


class DatasetGateway(IDatasetGateway):
    """Gateway for dataset database operations."""

    def __init__(self):
        # NOTE: Set ``slug`` as the index-attribute for ``datasets`` index in this version of renku
        project_context.database["datasets"]._v_main_attribute = "slug"

    def get_by_id(self, id: str) -> Dataset:
        """Get a dataset by id."""
        dataset = project_context.database.get_by_id(id)
        assert isinstance(dataset, Dataset)
        return dataset

    def get_by_slug(self, slug: str) -> Optional[Dataset]:
        """Get a dataset by slug."""
        return project_context.database["datasets"].get(slug)

    def get_all_active_datasets(self) -> List[Dataset]:
        """Return all datasets."""
        return list(project_context.database["datasets"].values())

    def get_provenance_tails(self) -> List[Dataset]:
        """Return the provenance for all datasets."""
        return list(project_context.database["datasets-provenance-tails"].values())

    def get_all_tags(self, dataset: Dataset) -> List[DatasetTag]:
        """Return the list of all tags for a dataset."""
        return list(project_context.database["datasets-tags"].get(dataset.slug, []))

    @deal.pre(lambda _: _.tag.date_created is None or _.tag.date_created >= project_context.project.date_created)
    def add_tag(self, dataset: Dataset, tag: DatasetTag):
        """Add a tag from a dataset."""
        tags: PersistentList = project_context.database["datasets-tags"].get(dataset.slug)
        if not tags:
            tags = PersistentList()
            project_context.database["datasets-tags"].add(tags, key=dataset.slug)

        assert tag.dataset_id.value == dataset.id, f"Tag has wrong dataset id: {tag.dataset_id.value} != {dataset.id}"

        tags.append(tag)

    def remove_tag(self, dataset: Dataset, tag: DatasetTag):
        """Remove a tag from a dataset."""
        tags: PersistentList = project_context.database["datasets-tags"].get(dataset.slug)
        for t in tags:
            if t.name == tag.name:
                tags.remove(t)
                project_context.database.remove_from_cache(t)
                break

    # NOTE: Enable this again once we properly deal with `date_created` on imported Renku datasets
    # @deal.pre(
    #     lambda _: _.dataset.date_created is None or _.dataset.date_created >= project_context.project.date_created
    # )
    def add_or_remove(self, dataset: Dataset) -> None:
        """Add or remove a dataset."""
        database = project_context.database

        if dataset.date_removed:
            database["datasets"].pop(dataset.slug, None)
            database["datasets-tags"].pop(dataset.slug, None)
        else:
            database["datasets"].add(dataset)

        if dataset.derived_from:
            database["datasets-provenance-tails"].pop(dataset.derived_from.url_id, None)
        database["datasets-provenance-tails"].add(dataset)
