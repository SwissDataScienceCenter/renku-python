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
import pathlib
import re
import urllib
from urllib.parse import urlparse

import attr
import requests

from renku.cli._providers.api import ProviderApi
from renku.cli._providers.doi import DOIProvider

ZENODO_BASE_URL = 'https://zenodo.org'
ZENODO_BASE_PATH = 'api'


def make_records_url(record_id):
    """Create URL to access record by ID."""
    return urllib.parse.urljoin(
        ZENODO_BASE_URL,
        pathlib.posixpath.join(ZENODO_BASE_PATH, 'records', record_id)
    )


@attr.s
class ZenodoFile:
    """Zenodo record file."""

    checksum = attr.ib()
    links = attr.ib()
    bucket = attr.ib()
    key = attr.ib()
    size = attr.ib()
    type = attr.ib()

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        return urllib.parse.urlparse(self.links['self'])

    @property
    def name(self):
        """Get file name."""
        return self.remote_url.path.split('/')[-1]


@attr.s
class ZenodoRecord:
    """Zenodo record."""

    id = attr.ib()
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

    conceptdoi = attr.ib(default=None)
    _zenodo = attr.ib(kw_only=True, default=None)

    @property
    def last_version(self):
        """Check if record is at last possible version."""
        return self.version['is_last']

    @property
    def version(self):
        """Get record version."""
        return self.metadata['relations']['version'][0]

    @property
    def display_version(self):
        """Get display version."""
        return 'v{0}'.format(self.version['index'])

    @property
    def display_name(self):
        """Get record display name."""
        return '{0}_{1}'.format(
            re.sub(r'\W+', '', self.metadata['title']).lower()[:16],
            self.display_version
        )

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if len(self.files) == 0:
            raise LookupError('no files have been found')

        return [ZenodoFile(**file_) for file_ in self.files]


@attr.s
class ZenodoProvider(ProviderApi):
    """zenodo.org registry API provider."""

    is_doi = attr.ib(default=False)

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        return urlparse(uri).path.split('/')[-1]

    def find_record(self, uri):
        """Retrieves a record from Zenodo.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :return: ``ZenodoRecord``
        """
        if self.is_doi:
            return self.find_record_by_doi(uri)

        return self.get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve the DOI and make a record for the retrieved record id."""
        doi = DOIProvider().find_record(doi)
        return self.get_record(ZenodoProvider.record_id(doi.URL))

    def get_record(self, uri):
        """Retrieve record metadata and return ``ZenodoRecord``."""
        record_id = ZenodoProvider.record_id(uri)
        response = requests.get(make_records_url(record_id))
        if response.status_code != 200:
            raise LookupError('record not found')

        return ZenodoRecord(**response.json(), zenodo=self)
