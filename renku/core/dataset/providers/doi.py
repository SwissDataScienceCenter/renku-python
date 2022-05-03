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

from renku.core.dataset.providers.api import ProviderApi, ProviderRecordSerializerApi
from renku.core.errors import RenkuImportError
from renku.core.util.doi import extract_doi, is_doi

DOI_BASE_URL = "https://dx.doi.org"


def make_doi_url(doi):
    """Create URL to access DOI metadata."""
    parsed_url = urllib.parse.urlparse(doi)
    if parsed_url.scheme == "doi":
        parsed_url = parsed_url._replace(scheme="")
        doi = parsed_url.geturl()
    return urllib.parse.urljoin(DOI_BASE_URL, doi)


class DOIMetadataSerializer(ProviderRecordSerializerApi):
    """Response from `doi.org <http://doi.org>`_ for DOI metadata."""

    def __init__(
        self,
        id,
        doi,
        url,
        abstract=None,
        author=None,
        categories=None,
        container_title=None,
        contributor=None,
        copyright=None,
        issued=None,
        language=None,
        publisher=None,
        title=None,
        type=None,
        version=None,
    ):
        super().__init__(uri=url)

        self.id = id
        self.doi = doi

        self.abstract = abstract
        self.author = author
        self.categories = categories
        self.container_title = container_title
        self.contributor = contributor
        self.copyright = copyright
        self.issued = issued
        self.language = language
        self.publisher = publisher
        self.title = title
        self.type = type
        self._version = version

    @property
    def version(self) -> str:
        """Get record version."""
        return self._version

    @property
    def latest_uri(self) -> str:
        """Get URI of the latest version."""
        return self.url

    def as_dataset(self, client):
        """Deserialize this record to a ``ProviderDataset``."""
        raise NotImplementedError

    def is_last_version(self, uri) -> bool:
        """Check if record is at last possible version."""
        return True


class DOIProvider(ProviderApi):
    """`doi.org <http://doi.org>`_ registry API provider."""

    def __init__(self, headers=None, timeout=3):
        self.timeout = timeout
        self.headers = headers if headers is not None else {"accept": "application/vnd.citationstyles.csl+json"}

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        return bool(is_doi(uri))

    @staticmethod
    def _serialize(response):
        """Serialize HTTP response for DOI."""
        data = {key.replace("-", "_").lower(): value for key, value in response.items()}
        try:
            serializer = DOIMetadataSerializer(**data)
            return serializer
        except TypeError as exp:
            raise RenkuImportError(exp, "doi metadata could not be serialized")

    def _query(self, doi):
        """Retrieve metadata for given doi."""
        from renku.core.util import requests

        doi = extract_doi(doi)
        url = make_doi_url(doi)

        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise LookupError("record not found. Status: {}".format(response.status_code))

        return response

    def find_record(self, uri, client=None, **kwargs) -> DOIMetadataSerializer:
        """Finds DOI record."""
        response = self._query(uri).json()
        return DOIProvider._serialize(response)

    def get_exporter(self, dataset, secret):
        """Implements interface ProviderApi."""
        pass
