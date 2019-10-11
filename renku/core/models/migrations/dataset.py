# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Migrations for dataset."""
import os
from pathlib import Path


def migrate_dataset_schema(data):
    """Migrate from old dataset formats."""
    if 'authors' not in data:
        return

    data['@context']['creator'] = data['@context'].pop(
        'authors', {'@container': 'list'}
    )

    data['creator'] = data.pop('authors', {})
    for file_name, file_ in data.get('files', {}).items():
        file_['creator'] = file_.pop('authors', {})

    return data


def migrate_absolute_paths(data):
    """Migrate dataset paths to use relative path."""
    raw_path = data.get('path', '.')
    path = Path(raw_path)

    if path.is_absolute():
        try:
            data['path'] = path.relative_to(os.getcwd())
        except ValueError:
            elements = raw_path.split('/')
            index = elements.index('.renku')
            data['path'] = Path('/'.join(elements[index:]))

    files = data.get('files', [])

    if isinstance(files, dict):
        files = files.values()

    for file_ in files:
        path = Path(file_.get('path'), '.')
        if path.is_absolute():
            file_['path'] = path.relative_to((os.getcwd()))

    return data


def migrate_doi_identifier(data):
    """If the dataset has a doi, make identifier be based on it."""
    from renku.core.utils.doi import is_doi, extract_doi

    if is_doi(data.get('_id', '')):
        data['identifier'] = extract_doi(data.get('_id'))
        data['same_as'] = data['_id']
        if data.get('@context'):
            data['@context'].setdefault('same_as', 'schema:sameAs')
    return data


DATASET_MIGRATIONS = [
    migrate_absolute_paths,
    migrate_dataset_schema,
]
