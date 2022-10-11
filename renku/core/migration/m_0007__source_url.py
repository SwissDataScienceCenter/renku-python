# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""DatasetFile source and url migrations."""

from renku.core.migration.models.v7 import get_project_datasets
from renku.core.migration.utils import generate_dataset_file_url


def migrate(_):
    """Migration function."""
    _fix_dataset_file_source_and_url()


def _fix_dataset_file_source_and_url():
    for dataset in get_project_datasets():
        for file in dataset.files:
            file.source = file.url
            file.url = generate_dataset_file_url(filepath=file.path)

            if file.source:
                file.source = file.source.replace("file://", "")

            if file.based_on:
                file.based_on.source = file.based_on.url

        dataset.to_yaml()
