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

from renku.api.models.project import ensure_project_context
from renku.core.models.datasets import Dataset as CoreDataset


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
    def __from_yaml(cls, path, client):
        """Create an instance from Dataset metadata."""
        self = cls()
        self._dataset = CoreDataset.from_yaml(path=path, client=client)
        self._files = [DatasetFile(f) for f in self._dataset.files]

        return self

    @staticmethod
    @ensure_project_context
    def list(project):
        """List all datasets in a project."""
        client = project.client
        return [Dataset.__from_yaml(p, client) for p in client.get_datasets_metadata_files()] if client else []

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

    _ATTRIBUTES = ["added", "full_path", "name", "path"]

    def __init__(self, dataset_file):
        self._dataset_file = dataset_file

        for name in self._ATTRIBUTES:
            setattr(self, name, None)

    def __getattribute__(self, name):
        dataset_file = object.__getattribute__(self, "_dataset_file")
        if dataset_file is not None and name in DatasetFile._ATTRIBUTES:
            return getattr(dataset_file, name)

        return object.__getattribute__(self, name)
