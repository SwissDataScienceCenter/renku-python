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
"""DatasetFile metadata migrations."""

from renku.core.utils.migrate import get_client_datasets


def migrate(client):
    """Migration function."""
    _remove_file_creator(client)


def _remove_file_creator(client):
    for dataset in get_client_datasets(client):
        for file_ in dataset.files:
            file_.metadata.pop('http://schema.org/creator', None)

        dataset.to_yaml()
