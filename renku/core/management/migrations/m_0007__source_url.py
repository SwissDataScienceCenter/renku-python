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

from renku.core.management.migrations.models.v7 import get_client_datasets
from renku.core.models.datasets import generate_dataset_file_url


def migrate(client):
    """Migration function."""
    _fix_dataset_file_source_and_url(client)


def _fix_dataset_file_source_and_url(client):
    for dataset in get_client_datasets(client):
        for file_ in dataset.files:
            file_.source = file_.url
            file_.url = generate_dataset_file_url(client=client, filepath=file_.path)

            if file_.source:
                file_.source = file_.source.replace("file://", "")

            if file_.based_on:
                file_.based_on.source = file_.based_on.url

        dataset.to_yaml()
