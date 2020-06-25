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
"""DOI API integration."""
import urllib

import attr

from renku.core.commands.providers.api import ProviderApi
from renku.core.utils.doi import extract_doi, is_doi
from renku.core.utils.requests import retry

DOI_BASE_URL = 'https://dx.doi.org'


def make_doi_url(doi):
    """Create URL to access DOI metadata."""
    urlparts = urllib.parse.urlparse(doi)
    if urlparts.scheme == 'doi':
        urlparts = urlparts._replace(scheme='')
        doi = urlparts.geturl()
    return urllib.parse.urljoin(DOI_BASE_URL, doi)


@attr.s
class DOIMetadataSerializer:
    """Response from doi.org for DOI metadata."""

    id = attr.ib(kw_only=True)

    doi = attr.ib(kw_only=True)

    url = attr.ib(kw_only=True)

    type = attr.ib(kw_only=True, default=None)

    categories = attr.ib(kw_only=True, default=None)

    author = attr.ib(kw_only=True, default=None)

    contributor = attr.ib(kw_only=True, default=None)

    version = attr.ib(kw_only=True, default=None)

    issued = attr.ib(kw_only=True, default=None)

    title = attr.ib(kw_only=True, default=None)

    abstract = attr.ib(kw_only=True, default=None)

    language = attr.ib(kw_only=True, default=None)

    publisher = attr.ib(kw_only=True, default=None)

    container_title = attr.ib(kw_only=True, default=None)

    copyright = attr.ib(kw_only=True, default=None)


@attr.s
class DOIProvider(ProviderApi):
    """doi.org registry API provider."""

    headers = attr.ib(
        default={'accept': 'application/vnd.citationstyles.csl+json'}
    )
    timeout = attr.ib(default=3)

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        return bool(is_doi(uri))

    @staticmethod
    def _serialize(response):
        """Serialize HTTP response for DOI."""
        return DOIMetadataSerializer(
            **{
                key.replace('-', '_').lower(): value
                for key, value in response.items()
            }
        )

    def _query(self, doi):
        """Retrieve metadata for given doi."""
        doi = extract_doi(doi)
        url = make_doi_url(doi)

        with retry() as session:
            response = session.get(url, headers=self.headers)
            if response.status_code != 200:
                raise LookupError(
                    'record not found. Status: {}'.format(
                        response.status_code
                    )
                )

            return response

    def find_record(self, uri, client=None):
        """Finds DOI record."""
        response = self._query(uri).json()
        return DOIProvider._serialize(response)

    def get_exporter(self, dataset, secret):
        """Implements interface ProviderApi."""
        pass
