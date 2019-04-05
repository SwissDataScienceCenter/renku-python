# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Zenodo API integration."""
import re
from urllib.parse import urlparse

import attr
import requests

from renku.cli._providers.doi import DOIProvider

BASE_URL = 'https://zenodo.org/api/'


@attr.s
class ZenodoFile:
    """Zenodo record file."""

    checksum = attr.ib()
    links = attr.ib()
    bucket = attr.ib()
    key = attr.ib()
    size = attr.ib()
    type = attr.ib()


@attr.s
class ZenodoRecord:
    """Zenodo record."""

    id = attr.ib()
    conceptdoi = attr.ib()
    conceptrecid = attr.ib()

    doi = attr.ib()
    files = attr.ib()
    links = attr.ib()
    metadata = attr.ib()
    owners = attr.ib()
    revision = attr.ib()
    stats = attr.ib()

    created = attr.ib()
    updated = attr.ib()

    _zenodo = attr.ib(kw_only=True)

    @property
    def last_version(self):
        """Check if record is at last possible version."""
        return self.version['is_last']

    @property
    def version(self):
        """Get record version."""
        return self.metadata['relations']['version'][0]

    @property
    def title(self):
        """Get record title."""
        return re.sub(r'\W+', '', self.metadata['title']).lower()

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if len(self.files) == 0:
            raise LookupError('no files have been found')

        return [ZenodoFile(**file_) for file_ in self.files]


class ZenodoProvider:
    """zenodo.org registry API provider."""

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        return urlparse(uri).path.split('/')[-1]

    def find_record(self, uri, is_doi=False):
        """Retrieves a record from Zenodo.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :param is_doi: Flag indicating if given URI is DOI.
        :return: ``ZenodoRecord``
        """
        if is_doi:
            return self.find_record_by_doi(uri)

        return self.get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve DOI and make record for retrieved record id."""
        doi = DOIProvider(uri=doi).as_obj()
        return self.get_record(ZenodoProvider.record_id(doi.URL))

    def get_record(self, uri):
        """Retrieve record metadata and return ``ZenodoRecord``."""
        _url = BASE_URL + 'records/' + ZenodoProvider.record_id(uri)
        response = requests.get(_url)
        if response.status_code != 200:
            raise LookupError('record not found')

        return ZenodoRecord(**response.json(), zenodo=self)
