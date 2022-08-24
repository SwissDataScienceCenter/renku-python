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
"""Dataset constants."""

from pathlib import Path

from renku.core.constant import RENKU_HOME
from renku.core.management.repository import RepositoryApiMixin

POINTERS = "pointers"
"""Directory for storing external pointer files."""

DATASET_IMAGES = "dataset_images"
"""Directory for dataset images."""

REFS = "refs"
"""Define a name of the folder with references in the Renku folder."""


def renku_dataset_images_path(client):
    """Return a ``Path`` instance of Renku dataset metadata folder."""
    return client.path / client.renku_home / DATASET_IMAGES


def renku_pointers_path(client):
    """Return a ``Path`` instance of Renku pointer files folder."""
    path = client.path / client.renku_home / POINTERS
    path.mkdir(exist_ok=True)
    return path


DATASET_METADATA_PATHS = [
    Path(RENKU_HOME) / RepositoryApiMixin.DATABASE_PATH,
    Path(RENKU_HOME) / DATASET_IMAGES,
    Path(RENKU_HOME) / POINTERS,
    Path(RENKU_HOME) / REFS,
    ".gitattributes",
    ".gitignore",
]
