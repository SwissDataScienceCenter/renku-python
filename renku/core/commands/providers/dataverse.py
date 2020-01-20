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
"""Dataverse API integration."""
import pathlib
import re
import urllib
import urllib.parse as urlparse

import attr

from renku.core.commands.providers.api import ExporterApi, ProviderApi
from renku.core.commands.providers.doi import DOIProvider
from renku.core.models.datasets import Dataset, DatasetFile
from renku.core.utils.doi import extract_doi, is_doi
from renku.core.utils.requests import retry

DATAVERSE_API_PATH = 'api'

DATAVERSE_VERSION_API = 'info/version'
DATAVERSE_METADATA_API = 'datasets/export'
DATAVERSE_FILE_API = 'access/datafile/:persistentId/'
DATAVERSE_EXPORTER = 'schema.org'


def check_dataverse_uri(url):
    """Check if an URL points to a dataverse instance."""
    url_parts = list(urlparse.urlparse(url))
    url_parts[2] = pathlib.posixpath.join(
        DATAVERSE_API_PATH, DATAVERSE_VERSION_API
    )

    url_parts[3:6] = [''] * 3
    version_url = urlparse.urlunparse(url_parts)

    with retry() as session:
        response = session.get(version_url)

        if response.status_code != 200:
            return False

        version_data = response.json()

        if 'status' not in version_data or 'data' not in version_data:
            return False

        version_info = version_data['data']

        if 'version' not in version_info or 'build' not in version_info:
            return False

        return True


def check_dataverse_doi(doi):
    """Check if a DOI points to a dataverse dataset."""
    try:
        doi = DOIProvider().find_record(doi)
    except LookupError:
        return False

    return check_dataverse_uri(doi.url)


def make_records_url(record_id, base_url):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = pathlib.posixpath.join(
        DATAVERSE_API_PATH, DATAVERSE_METADATA_API
    )
    args_dict = {'exporter': DATAVERSE_EXPORTER, 'persistentId': record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


def make_file_url(file_id, base_url):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = pathlib.posixpath.join(
        DATAVERSE_API_PATH, DATAVERSE_FILE_API
    )
    args_dict = {'persistentId': file_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


@attr.s
class DataverseProvider(ProviderApi):
    """Dataverse API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default='application/json')

    @staticmethod
    def supports(uri):
        """Check if provider supports a given uri."""
        is_doi_ = is_doi(uri)

        is_dataverse_uri = is_doi_ is None and check_dataverse_uri(uri)
        is_dataverse_doi = is_doi_ and check_dataverse_doi(is_doi_.group(0))

        return is_dataverse_uri or is_dataverse_doi

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        parsed = urlparse.urlparse(uri)
        return urlparse.parse_qs(parsed.query)['persistentId'][0]

    def make_request(self, uri):
        """Execute network request."""
        with retry() as session:
            response = session.get(uri, headers={'Accept': self._accept})
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
            doi = DOIProvider().find_record(uri)
            uri = doi.url

        uri = self.get_export_uri(uri)

        return self.get_record(uri)

    def get_export_uri(self, uri):
        """Gets a dataverse api export URI from a dataverse entry."""
        record_id = DataverseProvider.record_id(uri)
        uri = make_records_url(record_id, uri)
        return uri

    def get_record(self, uri):
        """Retrieve metadata and return ``DataverseRecordSerializer``."""
        response = self.make_request(uri)

        return DataverseRecordSerializer(
            json=response.json(), dataverse=self, uri=uri
        )

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        raise NotImplementedError()


@attr.s
class DataverseRecordSerializer:
    """Dataverse record Serializer."""

    _dataverse = attr.ib(default=None, kw_only=True)

    _uri = attr.ib(default=None, kw_only=True)

    _json = attr.ib(default=None, kw_only=True)

    def is_last_version(self, uri):
        """Check if record is at last possible version."""
        return True

    def _convert_json_property_name(self, property_name):
        """Removes '@' and converts names to snake_case."""
        property_name = property_name.strip('@')
        property_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', property_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', property_name).lower()

    @property
    def files(self):
        """Get all file metadata entries."""
        file_list = []

        for f in self._json['distribution']:
            mapped_file = {
                self._convert_json_property_name(k): v
                for k, v in f.items()
            }
            mapped_file['parent_url'] = self._uri
            file_list.append(mapped_file)
        return file_list

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
                url=remote_.geturl(),
                id=file_._id if file_._id else file_.name,
                filename=file_.name,
                filesize=file_.content_size,
                filetype=file_.file_format,
                path='',
            )
            serialized_files.append(dataset_file)

        dataset.files = serialized_files

        return dataset


@attr.s
class DataverseFileSerializer:
    """Dataverse record file."""

    _id = attr.ib(default=None, kw_only=True)

    identifier = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, kw_only=True)

    file_format = attr.ib(default=None, kw_only=True)

    content_size = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    content_url = attr.ib(default=None, kw_only=True)

    parent_url = attr.ib(default=None, kw_only=True)

    _type = attr.ib(default=None, kw_only=True)

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        if self.content_url is not None:
            return urllib.parse.urlparse(self.content_url)

        if self.identifier is None:
            return None

        doi = extract_doi(self.identifier)

        if doi is None:
            return None

        file_url = make_file_url('doi:' + doi, self.parent_url)

        return urllib.parse.urlparse(file_url)


class DataverseExporter(ExporterApi):
    """Dataverse export manager."""

    pass
