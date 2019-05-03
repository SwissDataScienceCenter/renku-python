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
"""DOI API integration."""
import re
import urllib

import attr
import requests

from renku.cli._providers.api import ProviderApi

doi_regexp = re.compile(
    r'(doi:\s*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d+(.\d+)*/.+)$',
    flags=re.I
)
"""See http://en.wikipedia.org/wiki/Digital_object_identifier."""

DOI_BASE_URL = 'https://dx.doi.org'


def make_doi_url(doi):
    """Create URL to access DOI metadata."""
    return urllib.parse.urljoin(DOI_BASE_URL, doi)


@attr.s
class DOIMetadata:
    """Response from doi.org for DOI metadata."""

    id = attr.ib(kw_only=True)
    DOI = attr.ib(kw_only=True)
    URL = attr.ib(kw_only=True)
    type = attr.ib(kw_only=True, default=None)
    categories = attr.ib(kw_only=True, default=None)
    author = attr.ib(kw_only=True, default=None)
    version = attr.ib(kw_only=True, default=None)
    issued = attr.ib(kw_only=True, default=None)
    title = attr.ib(kw_only=True, default=None)
    abstract = attr.ib(kw_only=True, default=None)
    language = attr.ib(kw_only=True, default=None)
    publisher = attr.ib(kw_only=True, default=None)


@attr.s
class DOIProvider(ProviderApi):
    """doi.org registry API provider."""

    headers = attr.ib(
        default={'accept': 'application/vnd.citationstyles.csl+json'}
    )
    timeout = attr.ib(default=3)

    @staticmethod
    def is_doi(uri):
        """Check if uri is DOI."""
        return doi_regexp.match(uri)

    def _query(self, doi):
        """Retrieve metadata for given doi."""
        url = doi
        if doi.startswith('http') is False:
            url = make_doi_url(doi)

        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise LookupError('record not found')

        return response

    def find_record(self, uri):
        """Finds DOI record."""
        response = self._query(uri).json()
        return DOIMetadata(**response)
