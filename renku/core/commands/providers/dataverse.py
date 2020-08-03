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
import json
import pathlib
import re
import urllib
from pathlib import Path
from string import Template
from urllib import parse as urlparse

import attr
import requests
from marshmallow import pre_load
from tqdm import tqdm

from renku.core import errors
from renku.core.commands.providers.api import ExporterApi, ProviderApi
from renku.core.commands.providers.doi import DOIProvider
from renku.core.models.datasets import Dataset, DatasetFile, DatasetSchema
from renku.core.models.provenance.agents import PersonSchema
from renku.core.utils.doi import extract_doi, is_doi
from renku.core.utils.requests import retry

from .dataverse_metadata_templates import AUTHOR_METADATA_TEMPLATE, \
    CONTACT_METADATA_TEMPLATE, DATASET_METADATA_TEMPLATE

DATAVERSE_API_PATH = 'api/v1'

DATAVERSE_VERSION_API = 'info/version'
DATAVERSE_METADATA_API = 'datasets/export'
DATAVERSE_FILE_API = 'access/datafile/:persistentId/'
DATAVERSE_EXPORTER = 'schema.org'


class _DataverseDatasetSchema(DatasetSchema):
    """Schema for Dataverse datasets."""

    @pre_load
    def fix_data(self, data, **kwargs):
        """Fix data that is received from Dataverse."""
        # Fix context
        context = data.get('@context')
        if context and isinstance(context, str):
            if context == 'http://schema.org':
                context = 'http://schema.org/'
            data['@context'] = {'@base': context, '@vocab': context}

        # Add type to creators
        creators = data.get('creator', [])
        for c in creators:
            c['@type'] = [str(t) for t in PersonSchema.opts.rdf_type]

        # Fix license to be a string
        license = data.get('license')
        if license and isinstance(license, dict):
            data['license'] = license.get('url', '')

        return data


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

    _server_url = attr.ib(default=None)

    _dataverse_name = attr.ib(default=None)

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

    def _make_request(self, uri):
        """Execute network request."""
        with retry() as session:
            response = session.get(uri, headers={'Accept': self._accept})
            if response.status_code != 200:
                raise LookupError(
                    'record not found. Status: {}'.format(
                        response.status_code
                    )
                )
            return response

    def find_record(self, uri, client=None):
        """Retrieves a record from Dataverse.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :return: ``DataverseRecord``
        """
        if self.is_doi:
            doi = DOIProvider().find_record(uri)
            uri = doi.url

        uri = self._get_export_uri(uri)

        return self._get_record(uri)

    def _get_export_uri(self, uri):
        """Gets a dataverse api export URI from a dataverse entry."""
        record_id = DataverseProvider.record_id(uri)
        uri = make_records_url(record_id, uri)
        return uri

    def _get_record(self, uri):
        """Retrieve metadata and return ``DataverseRecordSerializer``."""
        response = self._make_request(uri)

        return DataverseRecordSerializer(
            json=response.json(), dataverse=self, uri=uri
        )

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return DataverseExporter(
            dataset=dataset,
            access_token=access_token,
            server_url=self._server_url,
            dataverse_name=self._dataverse_name
        )

    def set_parameters(
        self, client, *, dataverse_server_url, dataverse_name, **kwargs
    ):
        """Set and validate required parameters for a provider."""
        CONFIG_BASE_URL = 'server_url'

        if not dataverse_server_url:
            dataverse_server_url = client.get_value(
                'dataverse', CONFIG_BASE_URL
            )
        else:
            client.set_value(
                'dataverse',
                CONFIG_BASE_URL,
                dataverse_server_url,
                global_only=True
            )

        if not dataverse_server_url:
            raise errors.ParameterError('Dataverse server URL is required.')

        if not dataverse_name:
            raise errors.ParameterError('Dataverse name is required.')

        self._server_url = dataverse_server_url
        self._dataverse_name = dataverse_name


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

    def get_files(self):
        """Get Dataverse files metadata as ``DataverseFileSerializer``."""
        if not self.files:
            raise LookupError(
                'no files have been found - deposit is empty or protected'
            )

        return [DataverseFileSerializer(**file_) for file_ in self.files]

    def as_dataset(self, client):
        """Deserialize `DataverseRecordSerializer` to `Dataset`."""
        files = self.get_files()
        dataset = Dataset.from_jsonld(
            self._json, client=client, schema_class=_DataverseDatasetSchema
        )

        if dataset.description and not dataset.description.strip():
            dataset.description = None

        for creator in dataset.creators:
            if creator.affiliation == '':
                creator.affiliation = None

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


@attr.s
class DataverseExporter(ExporterApi):
    """Dataverse export manager."""

    dataset = attr.ib(kw_only=True)

    access_token = attr.ib(kw_only=True)

    _server_url = attr.ib(kw_only=True, default=None)

    _dataverse_name = attr.ib(kw_only=True, default=None)

    def set_access_token(self, access_token):
        """Set access token."""
        self.access_token = access_token

    def access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urljoin(
            self._server_url, '/dataverseuser.xhtml?selectTab=apiTokenTab'
        )

    def export(self, publish, **kwargs):
        """Execute export process."""
        deposition = _DataverseDeposition(
            server_url=self._server_url, access_token=self.access_token
        )
        metadata = self._get_dataset_metadata()
        response = deposition.create_dataset(
            dataverse_name=self._dataverse_name, metadata=metadata
        )
        dataset_pid = response.json()['data']['persistentId']

        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file_ in self.dataset.files:
                try:
                    path = Path(file_.path).relative_to(self.dataset.data_dir)
                except ValueError:
                    path = Path(file_.path)
                deposition.upload_file(
                    full_path=file_.full_path, path_in_dataset=path
                )
                progressbar.update(1)

        if publish:
            deposition.publish_dataset()

        return dataset_pid

    def _get_dataset_metadata(self):
        authors, contacts = self._get_creators()
        metadata_template = Template(DATASET_METADATA_TEMPLATE)
        metadata = metadata_template.substitute(
            name=_escape_json_string(self.dataset.title),
            authors=json.dumps(authors),
            contacts=json.dumps(contacts),
            description=_escape_json_string(self.dataset.description)
        )
        return json.loads(metadata)

    def _get_creators(self):
        authors = []
        contacts = []

        for creator in self.dataset.creators:
            name = creator.name or ''
            affiliation = creator.affiliation or ''
            email = creator.email or ''

            author_template = Template(AUTHOR_METADATA_TEMPLATE)
            author = author_template.substitute(
                name=_escape_json_string(name),
                affiliation=_escape_json_string(affiliation)
            )
            authors.append(json.loads(author))

            contact_template = Template(CONTACT_METADATA_TEMPLATE)
            contact = contact_template.substitute(
                name=_escape_json_string(name), email=email
            )
            contacts.append(json.loads(contact))

        return authors, contacts


@attr.s
class _DataverseDeposition:
    """Dataverse record for deposit."""

    access_token = attr.ib(kw_only=True)
    server_url = attr.ib(kw_only=True)
    dataset_pid = attr.ib(kw_only=True, default=None)

    DATASET_CREATE_PATH = 'dataverses/{dataverseName}/datasets'
    FILE_UPLOAD_PATH = 'datasets/:persistentId/add'
    DATASET_PUBLISH_PATH = 'datasets/:persistentId/actions/:publish'

    def create_dataset(self, dataverse_name, metadata):
        """Create a dataset in a given dataverse."""
        api_path = self.DATASET_CREATE_PATH.format(
            dataverseName=dataverse_name
        )
        url = self._make_url(api_path=api_path)

        response = self._post(url=url, json=metadata)
        self._check_response(response)

        self.dataset_pid = response.json()['data']['persistentId']

        return response

    def upload_file(self, full_path, path_in_dataset):
        """Upload a file to a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError('Dataset not created.')

        url = self._make_url(
            self.FILE_UPLOAD_PATH, persistentId=self.dataset_pid
        )

        params = {'directoryLabel': str(path_in_dataset.parent)}
        data = dict(jsonData=json.dumps(params))

        files = {'file': (path_in_dataset.name, open(full_path, 'rb'))}

        response = self._post(url=url, data=data, files=files)
        self._check_response(response)

        return response

    def publish_dataset(self):
        """Publish a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError('Dataset not created.')

        url = self._make_url(
            self.DATASET_PUBLISH_PATH,
            persistentId=self.dataset_pid,
            type='major'
        )

        response = self._post(url=url)
        self._check_response(response)

        return response

    def _make_url(self, api_path, **query_params):
        """Create URL for creating a dataset."""
        url_parts = urlparse.urlparse(self.server_url)
        path = pathlib.posixpath.join(DATAVERSE_API_PATH, api_path)

        query_params = urllib.parse.urlencode(query_params)
        url_parts = url_parts._replace(path=path, query=query_params)
        return urllib.parse.urlunparse(url_parts)

    def _post(self, url, json=None, data=None, files=None):
        headers = {'X-Dataverse-key': self.access_token}
        try:
            with retry() as session:
                return session.post(
                    url=url,
                    json=json,
                    data=data,
                    files=files,
                    headers=headers
                )
        except requests.exceptions.RequestException as e:
            raise errors.ExportError('Cannot POST to remote server.') from e

    @staticmethod
    def _check_response(response):
        if response.status_code not in [200, 201, 202]:
            if response.status_code == 401:
                raise errors.AuthenticationError(
                    'Access unauthorized - update access token.'
                )
            json_res = response.json()
            raise errors.ExportError(
                'HTTP {} - Cannot export dataset: {}'.format(
                    response.status_code, json_res['message']
                    if 'message' in json_res else json_res['status']
                )
            )


def _escape_json_string(value):
    """Create a JSON-safe string."""
    if isinstance(value, str):
        return json.dumps(value)[1:-1]
    return value
