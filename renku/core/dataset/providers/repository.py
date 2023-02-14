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
"""Base class for online repository data providers."""

import abc
import re
from pathlib import Path
from typing import TYPE_CHECKING, List

from renku.core.dataset.providers.api import ImporterApi

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata
    from renku.domain_model.dataset import Dataset


class RepositoryImporter(ImporterApi, abc.ABC):
    """Online repository importer."""

    def download_files(self, destination: Path, extract: bool) -> List["DatasetAddMetadata"]:
        """Download dataset files from the remote provider."""
        from renku.core.dataset.providers.web import download_files

        urls, names = zip(*[(f.source, f.filename) for f in self.provider_dataset_files])

        return download_files(urls=urls, destination=destination, names=names, extract=extract)

    def tag_dataset(self, name: str) -> None:
        """Create a tag for the dataset ``name`` if the remote dataset has a tag/version."""
        from renku.core.dataset.tag import add_dataset_tag

        if self.provider_dataset.version:
            add_dataset_tag(
                dataset_name=name,
                tag=re.sub("[^a-zA-Z0-9.-_]", "_", self.provider_dataset.version),
                description=f"Tag {self.provider_dataset.version} created by renku import",
            )

    def copy_extra_metadata(self, new_dataset: "Dataset") -> None:
        """Copy provider specific metadata once the dataset is created."""
        pass


def make_request(url, accept: str = "application/json"):
    """Execute network request."""
    from renku.core.util import requests

    response = requests.get(url, headers={"Accept": accept})
    if response.status_code != 200:
        raise LookupError("record not found. Status: {}".format(response.status_code))

    return response
