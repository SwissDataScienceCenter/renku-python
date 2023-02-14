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
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from renku.domain_model import dataset as core_dataset
from renku.domain_model.project_context import has_graph_files
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.ui.api.util import ensure_project_context

if TYPE_CHECKING:
    from renku.ui.api import Project


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
    def _from_dataset(cls, dataset: core_dataset.Dataset) -> "Dataset":
        """Create an instance from Dataset metadata.

        Args:
            dataset(core_dataset.Dataset): The core dataset to wrap.

        Returns:
            Dataset: An API ``Dataset`` wrapping a core dataset.
        """
        self = cls()
        self._dataset = dataset
        self._files = [DatasetFile._from_dataset_file(f) for f in self._dataset.files]

        return self

    @staticmethod
    @ensure_project_context
    def list(project: "Project") -> List["Dataset"]:
        """List all datasets in a project.

        Args:
            project: The current project

        Returns:
            List["Dataset"]: A list of all datasets in the supplied project.
        """
        if not project.repository or not has_graph_files():
            return []

        dataset_gateway = DatasetGateway()

        return [Dataset._from_dataset(d) for d in dataset_gateway.get_all_active_datasets()]

    def __getattribute__(self, name):
        dataset = object.__getattribute__(self, "_dataset")
        if dataset is not None and name in Dataset._ATTRIBUTES:
            return getattr(dataset, name)

        return object.__getattribute__(self, name)

    @property
    def files(self):
        """Return list of existing files."""
        return self._files


class DatasetFile:
    """API DatasetFile model."""

    _ATTRIBUTES = ["date_added", "name", "path", "entity"]

    _dataset_file: core_dataset.DatasetFile
    full_path: Optional[Path] = None

    @classmethod
    @ensure_project_context
    def _from_dataset_file(cls, dataset_file: core_dataset.DatasetFile, project: "Project"):
        """Create an instance from Dataset metadata.

        Args:
            dataset_file(core_dataset.DatasetFile): The ``DatasetFile`` to wrap.
            project: The current project.

        Returns:
            An API ``DatasetFile`` wrapping a core dataset file.
        """
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
