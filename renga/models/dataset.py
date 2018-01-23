# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Model objects representing datasets."""

import json
import os
import shutil
import uuid
from datetime import datetime
from urllib.parse import urlparse

from marshmallow import Schema, fields, post_load

try:
    from pathlib import Path
except ImportError:  # pragma: no cover
    from pathlib2 import Path


class DatasetSchema(Schema):
    """Schema for the Dataset objects."""

    name = fields.String()
    identifier = fields.UUID(default=str(uuid.uuid4()))
    files = fields.List(fields.String)
    date_imported = fields.DateTime()
    import_from = fields.String(dump_to='imported_from')

    @post_load
    def make_dataset(self, data):
        """Return a dataset instance."""
        return Dataset(makedirs=False, **data)


class Dataset(object):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file')

    def __init__(self,
                 name,
                 data_dir=None,
                 import_from=None,
                 makedirs=True,
                 repo=None,
                 **kwargs):
        """Create a Dataset instance."""
        self.name = name
        self.identifier = uuid.uuid4()

        if data_dir:
            self.data_dir = Path(data_dir)
        elif repo:
            self.data_dir = repo.path.joinpath('data').absolute()
        else:
            self.data_dir = Path('./data').absolute()

        self.path = self.data_dir.joinpath(name)

        if makedirs:
            try:
                os.makedirs(self.path)
            except FileExistsError:
                raise FileExistsError('This dataset already exists.')

        self.import_from = import_from
        self.files = []
        self.repo = repo

        if import_from:
            self.import_data()

        self.commit()

    def import_data(self, import_from=None, data_dir=None):
        """Import the data into the data directory."""
        import_from = import_from or self.import_from
        self.import_from = import_from

        data_dir = data_dir or self.data_dir

        if not isinstance(import_from, list):
            import_from = [import_from]

        self.files = [
            self._import_from_url(self.path, url) for url in import_from
        ]
        self.date_imported = datetime.now()
        self.commit()

    def commit(self):
        """Write the dataset metadata to disk."""
        with open(self.path.joinpath(self.name + '.meta.json'), 'w') as f:
            f.write(self.json)
        return self.json

    @property
    def json(self):
        """Dump the json for this dataset."""
        return DatasetSchema().dumps(self).data

    def to_dict(self):
        """Return a dictionary with the metadata for this dataset."""
        return DatasetSchema().dump(self).data

    @staticmethod
    def _import_from_url(import_path, url):
        """Process an import from url and return the location on disk."""
        u = urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme))

        if u.scheme in ('', 'file'):
            shutil.copy(u.path, import_path)

        return import_path / Path(os.path.basename(u.path))

    @staticmethod
    def from_json(metadata_file):
        """Return a Dataset object deserialized from json on disk."""
        with open(metadata_file) as f:
            return DatasetSchema().load(json.load(f)).data
