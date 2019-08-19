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
"""Dataverse API integration."""
import pathlib
import urllib
import urllib.parse as urlparse

import attr
import requests

from renku.cli._providers.api import ExporterApi, ProviderApi
from renku.cli._providers.doi import DOIProvider
from renku.models.datasets import Dataset, DatasetFile

DATAVERSE_BASE_URL = 'https://dataverse.harvard.edu/'
DATAVERSE_API_PATH = 'api'

DATAVERSE_METADATA_URL = 'datasets/export'
DATAVERSE_EXPORTER = 'schema.org'


def make_records_url(record_id):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(DATAVERSE_BASE_URL))
    url_parts[2] = pathlib.posixpath.join(
        DATAVERSE_API_PATH, DATAVERSE_METADATA_URL
    )
    args_dict = {'exporter': DATAVERSE_EXPORTER, 'persistentId': record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


@attr.s
class DataverseProvider(ProviderApi):
    """Dataverse API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default='application/json')

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        parsed = urlparse.urlparse(uri)
        return urlparse.parse_qs(parsed.query)['persistentId'][0]

    def make_request(self, uri):
        """Execute network request."""
        response = requests.get(uri, headers={'Accept': self._accept})
        if response.status_code != 200:
            raise LookupError('record not found')
        return response

    def find_record(self, uri):
        """Retrieves a record from Dataverse.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :return: ``DataverseRecord``
        """
        if self.is_doi:
            return self.find_record_by_doi(uri)

        return self.get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve the DOI and make a record for the retrieved record id."""
        doi = DOIProvider().find_record(doi)
        record_id = DataverseProvider.record_id(doi.url)
        uri = make_records_url(record_id)
        return self.get_record(uri)

    def get_record(self, uri):
        """Retrieve metadata and return ``DataverseRecordSerializer``."""
        response = self.make_request(uri)

        return DataverseRecordSerializer(
            json=response.json(), dataverse=self, uri=uri
        )

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return DataverseExporter(dataset=dataset, access_token=access_token)


@attr.s
class DataverseRecordSerializer:
    """Dataverse record Serializer."""

    _dataverse = attr.ib(default=None, kw_only=True)

    _uri = attr.ib(default=None, kw_only=True)

    _json = attr.ib(default=None, kw_only=True)

    def is_last_version(self, uri):
        """Check if record is at last possible version."""
        return True

    @property
    def files(self):
        """Get all file metadata entries."""
        return [{k.strip('@'): v
                 for k, v in f.items()} for f in self._json['distribution']]

    def get_jsonld(self):
        """Get record metadata as jsonld."""
        response = self._dataverse.accept_jsonld().make_request(self._uri)
        self._jsonld = response.json()
        return self._jsonld

    def get_files(self):
        """Get Dataverse files metadata as ``DataverseFileSerializer``."""
        if len(self.files) == 0:
            raise LookupError('no files have been found')

        return [DataverseFileSerializer(**file_) for file_ in self.files]

    def as_dataset(self, client):
        """Deserialize `DataverseRecordSerializer` to `Dataset`."""
        files = self.get_files()
        dataset = Dataset.from_jsonld(self._json, client=client)

        serialized_files = []
        for file_ in files:
            remote_ = file_.remote_url
            dataset_file = DatasetFile(
                url=remote_,
                id=file_.name,
                filename=file_.name,
                filesize=file_.contentSize,
                filetype=file_.fileFormat,
                dataset=dataset.name,
                path='',
            )
            serialized_files.append(dataset_file)

        dataset.files = serialized_files

        if isinstance(dataset.url, dict) and '_id' in dataset.url:
            dataset.url = urllib.parse.urlparse(dataset.url.pop('_id'))
            dataset.url = dataset.url.geturl()

        return dataset


@attr.s
class DataverseFileSerializer:
    """Dataverse record file."""

    name = attr.ib(default=None, kw_only=True)

    fileFormat = attr.ib(default=None, kw_only=True)

    contentSize = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    contentUrl = attr.ib(default=None, kw_only=True)

    _type = attr.ib(default=None, kw_only=True)

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        return urllib.parse.urlparse(self.contentUrl)


class DataverseExporter(ExporterApi):
    """Dataverse export manager."""

    pass
