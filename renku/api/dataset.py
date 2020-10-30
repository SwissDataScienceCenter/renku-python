# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""API Dataset."""

from renku.api.project import get_current_project
from renku.core.models.datasets import Dataset as CoreDataset


class Dataset:
    """API Dataset model."""

    ATTRIBUTES = CoreDataset.EDITABLE_FIELDS + ["name"]

    def __init__(self):
        self._dataset = None

    @classmethod
    def from_dataset_metadata(cls, path, client):
        """Create an instance from Dataset metadata."""
        self = cls()
        self._dataset = CoreDataset.from_yaml(path=path, client=client)

        return self

    @staticmethod
    def list():
        """List all datasets in a project."""
        project = get_current_project()
        client = project.client
        return [Dataset.from_dataset_metadata(p, client) for p in client.datasets.keys()]

    def __getattr__(self, name):
        """Return dataset's attribute."""
        if self._dataset and name in self.ATTRIBUTES:
            return getattr(self._dataset, name, None)

        raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")
