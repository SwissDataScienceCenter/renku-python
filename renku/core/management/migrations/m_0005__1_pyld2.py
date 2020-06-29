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
"""Migrate datasets with type scoped contexts for support with pyld 2.0."""

import re


def migrate(client):
    """Migration function."""
    _migrate_datasets(client)


def _migrate_datasets(client):
    """Migrate type scoped contexts of datasets."""
    paths = (client.path / client.renku_datasets_path).rglob(client.METADATA)

    for path in paths:
        with path.open('r') as dataset:
            content = dataset.read()

        content = re.sub(r'"([^"])+_prov:([^"]+)":', '"\1_prov_\2":', content)
        content = re.sub(
            r'"([^"])+_wfprov:([^"]+)":', '"\1_wfprov_\2":', content
        )
        content = re.sub(
            r'"([^"])+_schema:([^"]+)":', '"\1_schema_\2":', content
        )

        with path.open('w') as dataset:
            dataset.write(content)
