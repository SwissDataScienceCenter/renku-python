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
"""Serializers for dataset list files."""
from .tabulate import tabulate


def tabular(client, records, *, columns=None):
    """Format dataset files with a tabular output.

    :param client: LocalClient instance.
    :param records: Filtered collection.
    :param columns: List of columns to display
    """
    if not columns:
        columns = 'added,creators,dataset,full_path'

    return tabulate(
        collection=records,
        columns=columns,
        columns_mapping=DATASET_FILES_COLUMNS
    )


def jsonld(client, records, **kwargs):
    """Format dataset files as JSON-LD.

    :param client: LocalClient instance.
    :param records: Filtered collection.
    """
    from renku.core.models.json import dumps
    from renku.core.models.jsonld import asjsonld

    data = [asjsonld(record) for record in records]
    return dumps(data, indent=2)


DATASET_FILES_FORMATS = {
    'tabular': tabular,
    'json-ld': jsonld,
}
"""Valid formatting options."""

DATASET_FILES_COLUMNS = {
    'added': ('added', None),
    'creators': ('creators_csv', 'creators'),
    'creators_full': ('creators_full_csv', 'creators'),
    'dataset': ('title', 'dataset'),
    'full_path': ('full_path', None),
    'path': ('path', None),
    'short_name': ('short_name', 'dataset short_name'),
}
