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
"""Serializers for datasets."""
import textwrap

from renku.core.models.json import dumps

from .tabulate import tabulate


def tabular(client, datasets, *, columns=None):
    """Format datasets with a tabular output."""
    if not columns:
        columns = "id,date_created,name,creators,tags,version"

    _create_dataset_short_description(datasets)

    return tabulate(collection=datasets, columns=columns, columns_mapping=DATASETS_COLUMNS)


def _create_dataset_short_description(datasets):
    for dataset in datasets:
        lines = textwrap.wrap(dataset.description, width=64, max_lines=5) if dataset.description else []
        dataset.short_description = "\n".join(lines)


def jsonld(client, datasets, **kwargs):
    """Format datasets as JSON-LD."""
    data = [dataset.as_jsonld() for dataset in datasets]
    return dumps(data, indent=2)


DATASETS_FORMATS = {
    "tabular": tabular,
    "json-ld": jsonld,
}
"""Valid formatting options."""

DATASETS_COLUMNS = {
    "id": ("identifier", "id"),
    "created": ("date_created", None),
    "date_created": ("date_created", None),
    "short_name": ("name", None),
    "name": ("name", None),
    "creators": ("creators_csv", "creators"),
    "creators_full": ("creators_full_csv", "creators"),
    "tags": ("tags_csv", "tags"),
    "version": ("version", None),
    "title": ("title", "title"),
    "keywords": ("keywords_csv", "keywords"),
    "description": ("short_description", "description"),
}
