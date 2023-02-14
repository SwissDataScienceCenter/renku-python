# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
from pathlib import Path
from typing import Optional

from renku.core import errors
from renku.core.dataset.providers.api import ImporterApi, ImportProviderInterface, ProviderApi, ProviderPriority
from renku.core.util.doi import extract_doi, is_doi

DOI_BASE_URL = "https://dx.doi.org"


class DOIProvider(ProviderApi, ImportProviderInterface):
    """`doi.org <http://doi.org>`_ registry API provider."""

    priority = ProviderPriority.HIGHER
    name = "DOI"

    def __init__(self, uri: Optional[str], headers=None, timeout=3):
        super().__init__(uri=uri)

        self.timeout = timeout
        self.headers = headers if headers is not None else {"accept": "application/vnd.citationstyles.csl+json"}

    @staticmethod
    def supports(uri) -> bool:
        """Whether or not this provider supports a given URI."""
        return bool(is_doi(uri))

    def get_importer(self, **kwargs) -> "DOIImporter":
        """Get import manager."""
        from renku.core.util import requests

        def query(doi):
            """Retrieve metadata for given doi."""
            doi = extract_doi(doi)
            url = make_doi_url(doi)

            response = requests.get(url, headers=self.headers)

            if response.status_code != 200:
                raise LookupError("record not found. Status: {}".format(response.status_code))

            return response

        def serialize(response):
            """Serialize HTTP response for DOI."""
            json_data = response.json()
            data = {key.replace("-", "_").lower(): value for key, value in json_data.items()}
            try:
                return DOIImporter(**data)
            except TypeError:
                raise errors.DatasetImportError("doi metadata could not be serialized")

        query_response = query(self.uri)
        return serialize(query_response)


class DOIImporter(ImporterApi):
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
        super().__init__(uri=url, original_uri=url)

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
        return self.uri

    def fetch_provider_dataset(self):
        """Deserialize this record to a ``ProviderDataset``."""
        raise NotImplementedError

    def is_latest_version(self) -> bool:
        """Check if record is at last possible version."""
        return True

    def download_files(self, destination: Path, extract: bool):
        """Download dataset files from the remote provider."""
        raise NotImplementedError

    def tag_dataset(self, name: str) -> None:
        """Create a tag for the dataset ``name`` if the remote dataset has a tag/version."""
        raise NotImplementedError

    def copy_extra_metadata(self, new_dataset) -> None:
        """Copy provider specific metadata once the dataset is created."""
        raise NotImplementedError


def make_doi_url(doi):
    """Create URL to access DOI metadata."""
    parsed_url = urllib.parse.urlparse(doi)
    if parsed_url.scheme == "doi":
        parsed_url = parsed_url._replace(scheme="")
        doi = parsed_url.geturl()
    return urllib.parse.urljoin(DOI_BASE_URL, doi)
