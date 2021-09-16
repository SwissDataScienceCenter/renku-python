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
r"""Renku API Dataset.

Dataset class allows listing datasets and files inside a Renku project and
accessing their metadata.

To get a list of available datasets in a Renku project use ``list`` method:

.. code-block:: python

    from renku.api import Dataset

    datasets = Dataset.list()

You can then access metadata of a dataset like ``name``, ``title``,
``keywords``, etc. To get the list of files inside a dataset use ``files``
property:

.. code-block:: python

    for dataset_file in dataset.files:
        print(dataset_file.path)

"""

from operator import attrgetter

from renku.api.models.project import ensure_project_context
from renku.core.management.command_builder.database_dispatcher import DatabaseDispatcher
from renku.core.metadata.gateway.dataset_gateway import DatasetGateway
from renku.core.models import dataset as core_dataset


class Dataset:
    """API Dataset model."""

    _ATTRIBUTES = [
        "creators",
        "date_created",
        "date_published",
        "description",
        "keywords",
        "license",
        "name",
        "title",
        "url",
        "version",
    ]

    def __init__(self):
        self._dataset = None

        for name in self._ATTRIBUTES:
            setattr(self, name, None)
        self._files = []

    @classmethod
    def _from_dataset(cls, dataset: core_dataset.Dataset):
        """Create an instance from Dataset metadata."""
        self = cls()
        self._dataset = dataset
        self._files = [DatasetFile._from_dataset_file(f) for f in self._dataset.files]

        return self

    @staticmethod
    @ensure_project_context
    def list(project):
        """List all datasets in a project."""
        client = project.client
        if not client or not client.has_graph_files():
            return []
        database_dispatcher = DatabaseDispatcher()
        database_dispatcher.push_database_to_stack(client.database_path)
        dataset_gateway = DatasetGateway()
        dataset_gateway.database_dispatcher = database_dispatcher

        return [Dataset._from_dataset(d) for d in dataset_gateway.get_all_datasets()]

    def __getattribute__(self, name):
        dataset = object.__getattribute__(self, "_dataset")
        if dataset is not None and name in Dataset._ATTRIBUTES:
            return getattr(dataset, name)

        return object.__getattribute__(self, name)

    @property
    def files(self):
        """Return a list of dataset files."""
        return self._files


class DatasetFile:
    """API DatasetFile model."""

    _ATTRIBUTES = ["date_added", "name", "path", "entity"]

    @classmethod
    @ensure_project_context
    def _from_dataset_file(cls, dataset_file: core_dataset.DatasetFile, project):
        """Create an instance from Dataset metadata."""
        self = cls()
        self._dataset_file = dataset_file
        self.full_path = project.path / dataset_file.entity.path

        return self

    def __getattribute__(self, name):
        dataset_file = object.__getattribute__(self, "_dataset_file")
        if dataset_file is not None and name in DatasetFile._ATTRIBUTES:
            if name == "path":
                name = "entity.path"
            getter = attrgetter(name)
            return getter(dataset_file)

        return object.__getattribute__(self, name)
