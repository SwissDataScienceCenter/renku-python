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
"""Dataset context managers."""

from pathlib import Path
from typing import Optional

from renku.core import errors
from renku.core.dataset.dataset import create_dataset
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.domain_model.dataset import Dataset
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person


class DatasetContext:
    """Dataset context manager for metadata changes."""

    def __init__(
        self,
        name: str,
        create: Optional[bool] = False,
        commit_database: Optional[bool] = False,
        creator: Optional[Person] = None,
        datadir: Optional[Path] = None,
        storage: Optional[str] = None,
    ) -> None:
        self.name = name
        self.create = create
        self.commit_database = commit_database
        self.creator = creator
        self.dataset_provenance = DatasetsProvenance()
        self.dataset: Optional[Dataset] = None
        self.datadir: Optional[Path] = datadir
        self.storage = storage

    def __enter__(self):
        """Enter context."""
        self.dataset = self.dataset_provenance.get_by_name(name=self.name)
        if self.dataset is None:
            if not self.create:
                raise errors.DatasetNotFound(name=self.name)

            # NOTE: Don't update provenance when creating here because it will be updated later
            self.dataset = create_dataset(
                name=self.name, update_provenance=False, datadir=self.datadir, storage=self.storage
            )
        elif self.create:
            raise errors.DatasetExistsError(self.name)

        return self.dataset

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context."""
        if exc_type:
            # TODO use a general clean-up strategy: https://github.com/SwissDataScienceCenter/renku-python/issues/736
            # NOTE: Re-raise exception
            return False

        if self.dataset and self.commit_database:
            self.datasets_provenance = DatasetsProvenance()
            self.datasets_provenance.add_or_update(self.dataset, creator=self.creator)
            project_context.database.commit()
