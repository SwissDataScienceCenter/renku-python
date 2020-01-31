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
"""Migrations for dataset."""
import os
import uuid
from pathlib import Path


def migrate_dataset_schema(data):
    """Migrate from old dataset formats."""
    if 'authors' not in data:
        return

    data['@context']['creator'] = data['@context'].pop(
        'authors', {'@container': 'list'}
    )

    data['creator'] = data.pop('authors', {})

    files = data.get('files', [])

    if isinstance(files, dict):
        files = files.values()
    for file_ in files:
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
        files = list(files.values())

    for file_ in files:
        path = Path(file_.get('path'), '.')
        if path.is_absolute():
            file_['path'] = path.relative_to((os.getcwd()))
    data['files'] = files
    return data


def migrate_doi_identifier(data):
    """If the dataset _id is doi, make it a UUID."""
    from renku.core.utils.doi import is_doi
    from renku.core.utils.uuid import is_uuid

    _id = data.get('_id', '')
    identifier = data.get('identifier', '')

    if not is_uuid(_id):
        if not is_uuid(identifier):
            data['identifier'] = str(uuid.uuid4())
        if is_doi(data.get('_id', '')):
            data['same_as'] = {'@type': ['schema:URL'], 'url': data['_id']}
            if data.get('@context'):
                data['@context'].setdefault(
                    'same_as', {
                        '@id': 'schema:sameAs',
                        '@type': 'schema:URL',
                        '@context': {
                            '@version': '1.1',
                            'url': 'schema:url',
                            'schema': 'http://schema.org/'
                        }
                    }
                )
        data['_id'] = data['identifier']
    return data


def migrate_same_as_structure(data):
    """Changes sameAs string to schema:URL object."""
    same_as = data.get('same_as')

    if same_as and isinstance(same_as, str):
        data['same_as'] = {'@type': ['schema:URL'], 'url': same_as}

        if data.get('@context'):
            data['@context'].setdefault(
                'same_as', {
                    '@id': 'schema:sameAs',
                    '@type': 'schema:URL',
                    '@context': {
                        '@version': '1.1',
                        'url': 'schema:url',
                        'schema': 'http://schema.org/'
                    }
                }
            )

    return data
