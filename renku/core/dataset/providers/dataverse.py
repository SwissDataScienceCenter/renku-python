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
from typing import TYPE_CHECKING, Any, Dict
from urllib import parse as urlparse

import attr
from tqdm import tqdm

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.dataset.providers.api import ExporterApi, ProviderApi, ProviderRecordSerializerApi
from renku.core.dataset.providers.dataverse_metadata_templates import (
    AUTHOR_METADATA_TEMPLATE,
    CONTACT_METADATA_TEMPLATE,
    DATASET_METADATA_TEMPLATE,
)
from renku.core.dataset.providers.doi import DOIProvider
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.util import communication
from renku.core.util.doi import extract_doi, is_doi
from renku.core.util.file_size import bytes_to_unit

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset

DATAVERSE_API_PATH = "api/v1"

DATAVERSE_VERSION_API = "info/version"
DATAVERSE_METADATA_API = "datasets/export"
DATAVERSE_VERSIONS_API = "datasets/:persistentId/versions"
DATAVERSE_FILE_API = "access/datafile/:persistentId/"
DATAVERSE_EXPORTER = "schema.org"

DATAVERSE_SUBJECTS = [
    "Agricultural Sciences",
    "Arts and Humanities",
    "Astronomy and Astrophysics",
    "Business and Management",
    "Chemistry",
    "Computer and Information Science",
    "Earth and Environmental Sciences",
    "Engineering",
    "Law",
    "Mathematical Sciences",
    "Medicine, Health and Life Sciences",
    "Physics",
    "Social Sciences",
    "Other",
]


def check_dataverse_uri(url):
    """Check if an URL points to a dataverse instance."""
    from renku.core.util import requests

    url_parts = list(urlparse.urlparse(url))
    url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSION_API)

    url_parts[3:6] = [""] * 3
    version_url = urlparse.urlunparse(url_parts)

    response = requests.get(version_url)

    if response.status_code != 200:
        return False

    version_data = response.json()

    if "status" not in version_data or "data" not in version_data:
        return False

    version_info = version_data["data"]

    if "version" not in version_info or "build" not in version_info:
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
    url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_METADATA_API)
    args_dict = {"exporter": DATAVERSE_EXPORTER, "persistentId": record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


def make_versions_url(record_id, base_url):
    """Create URL to access the versions of a record."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSIONS_API)
    args_dict = {"exporter": DATAVERSE_EXPORTER, "persistentId": record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


def make_file_url(file_id, base_url):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_FILE_API)
    args_dict = {"persistentId": file_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


@attr.s
class DataverseProvider(ProviderApi):
    """Dataverse API provider."""

    is_doi = attr.ib(default=False)

    _server_url = attr.ib(default=None)

    _dataverse_name = attr.ib(default=None)

    @staticmethod
    def supports(uri):
        """Check if provider supports a given URI."""
        is_doi_ = is_doi(uri)

        is_dataverse_uri = is_doi_ is None and check_dataverse_uri(uri)
        is_dataverse_doi = is_doi_ and check_dataverse_doi(is_doi_.group(0))

        return is_dataverse_uri or is_dataverse_doi

    @staticmethod
    def supports_export():
        """Whether this provider supports dataset export."""
        return True

    @staticmethod
    def supports_import():
        """Whether this provider supports dataset import."""
        return True

    @staticmethod
    def export_parameters():
        """Returns parameters that can be set for export."""
        return {
            "dataverse-server": ("Dataverse server URL.", str),
            "dataverse-name": ("Dataverse name to export to.", str),
        }

    @staticmethod
    def record_id(uri):
        """Extract record id from URI."""
        parsed = urlparse.urlparse(uri)
        return urlparse.parse_qs(parsed.query)["persistentId"][0]

    def find_record(self, uri, **kwargs) -> "DataverseRecordSerializer":
        """Retrieves a record from Dataverse.

        Args:
            uri: DOI or URL.

        Returns:
            DataverseRecordSerializer: The found record

        """
        if self.is_doi:
            doi = DOIProvider().find_record(uri)
            uri = doi.url

        uri = self._get_export_uri(uri)
        response = _make_request(uri)

        return DataverseRecordSerializer(json=response.json(), uri=uri)

    @staticmethod
    def _get_export_uri(uri):
        """Gets a dataverse api export URI from a dataverse entry."""
        record_id = DataverseProvider.record_id(uri)
        uri = make_records_url(record_id, uri)
        return uri

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return DataverseExporter(
            dataset=dataset, access_token=access_token, server_url=self._server_url, dataverse_name=self._dataverse_name
        )

    @inject.autoparams()
    def set_parameters(self, client_dispatcher: IClientDispatcher, *, dataverse_server, dataverse_name, **kwargs):
        """Set and validate required parameters for a provider."""
        config_base_url = "server_url"

        client = client_dispatcher.current_client

        if not dataverse_server:
            dataverse_server = client.get_value("dataverse", config_base_url)
        else:
            client.set_value("dataverse", config_base_url, dataverse_server, global_only=True)

        if not dataverse_server:
            raise errors.ParameterError("Dataverse server URL is required.")

        if not dataverse_name:
            raise errors.ParameterError("Dataverse name is required.")

        self._server_url = dataverse_server
        self._dataverse_name = dataverse_name


class DataverseRecordSerializer(ProviderRecordSerializerApi):
    """Dataverse record serializer."""

    def __init__(self, uri: str, json: Dict[str, Any]):
        super().__init__(uri=uri)
        self._json: Dict[str, Any] = json

    def is_last_version(self, uri):
        """Check if record is at last possible version."""
        return True

    @staticmethod
    def _convert_json_property_name(property_name):
        """Removes '@' and converts names to snake_case."""
        property_name = property_name.strip("@")
        property_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", property_name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", property_name).lower()

    @property
    def files(self):
        """Get all file metadata entries."""
        file_list = []

        for f in self._json["distribution"]:
            mapped_file = {self._convert_json_property_name(k): v for k, v in f.items()}
            mapped_file["parent_url"] = self._uri
            file_list.append(mapped_file)
        return file_list

    @property
    def version(self):
        """Get the major and minor version of this dataset."""
        uri = make_versions_url(DataverseProvider.record_id(self._uri), self._uri)
        response = _make_request(uri).json()
        newest_version = response["data"][0]
        return "{}.{}".format(newest_version["versionNumber"], newest_version["versionMinorNumber"])

    @property
    def latest_uri(self):
        """Get URI of latest version."""
        return self._uri

    def get_files(self):
        """Get Dataverse files metadata as ``DataverseFileSerializer``."""
        files = self.files
        if not files:
            raise LookupError("no files have been found - deposit is empty or protected")

        return [DataverseFileSerializer(**file_) for file_ in files]

    def as_dataset(self, client) -> "ProviderDataset":
        """Deserialize ``DataverseRecordSerializer`` to ``Dataset``."""

        from marshmallow import pre_load

        from renku.command.schema.agent import PersonSchema
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile, ProviderDatasetSchema

        class _DataverseDatasetSchema(ProviderDatasetSchema):
            """Schema for Dataverse datasets."""

            @pre_load
            def fix_data(self, data, **kwargs):
                """Fix data that is received from Dataverse."""
                # Fix context
                context = data.get("@context")
                if context and isinstance(context, str):
                    if context == "http://schema.org":
                        context = "http://schema.org/"
                    data["@context"] = {"@base": context, "@vocab": context}

                # Add type to creators
                creators = data.get("creator", [])
                for c in creators:
                    c["@type"] = [str(t) for t in PersonSchema.opts.rdf_type]

                # Fix license to be a string
                license = data.get("license")
                if license and isinstance(license, dict):
                    data["license"] = license.get("url", "")

                return data

        files = self.get_files()
        dataset = ProviderDataset.from_jsonld(data=self._json, schema_class=_DataverseDatasetSchema)

        dataset.version = self.version

        if dataset.description and not dataset.description.strip():
            dataset.description = None

        for creator in dataset.creators:
            if creator.affiliation == "":
                creator.affiliation = None

        self._files_info = [
            ProviderDatasetFile(
                source=file.remote_url.geturl(),
                filename=Path(file.name).name,
                checksum="",
                size_in_mb=bytes_to_unit(file.content_size, "mi"),
                filetype=file.file_format,
                path="",
            )
            for file in files
        ]

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

        file_url = make_file_url("doi:" + doi, self.parent_url)

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
        return urllib.parse.urljoin(self._server_url, "/dataverseuser.xhtml?selectTab=apiTokenTab")

    def export(self, publish, client=None, **kwargs):
        """Execute export process."""
        deposition = _DataverseDeposition(server_url=self._server_url, access_token=self.access_token)
        metadata = self._get_dataset_metadata()
        response = deposition.create_dataset(dataverse_name=self._dataverse_name, metadata=metadata)
        dataset_pid = response.json()["data"]["persistentId"]

        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file in self.dataset.files:
                try:
                    path = (client.path / file.entity.path).relative_to(self.dataset.data_dir)
                except ValueError:
                    path = Path(file.entity.path)
                filepath = client.repository.copy_content_to_file(path=file.entity.path, checksum=file.entity.checksum)
                deposition.upload_file(full_path=filepath, path_in_dataset=path)
                progressbar.update(1)

        if publish:
            deposition.publish_dataset()

        return dataset_pid

    def _get_dataset_metadata(self):
        authors, contacts = self._get_creators()
        subject = self._get_subject()
        metadata_template = Template(DATASET_METADATA_TEMPLATE)
        metadata = metadata_template.substitute(
            name=_escape_json_string(self.dataset.title),
            authors=json.dumps(authors),
            contacts=json.dumps(contacts),
            description=_escape_json_string(self.dataset.description),
            subject=subject,
        )
        return json.loads(metadata)

    @staticmethod
    def _get_subject():
        text_prompt = "Subject of this dataset: \n\n"
        text_prompt += "\n".join(f"{s}\t[{i}]" for i, s in enumerate(DATAVERSE_SUBJECTS, start=1))
        text_prompt += "\n\nSubject"

        selection = communication.prompt(text_prompt, type=int, default=len(DATAVERSE_SUBJECTS)) or 0

        return DATAVERSE_SUBJECTS[selection - 1]

    def _get_creators(self):
        authors = []
        contacts = []

        for creator in self.dataset.creators:
            name = creator.name or ""
            affiliation = creator.affiliation or ""
            email = creator.email or ""

            author_template = Template(AUTHOR_METADATA_TEMPLATE)
            author = author_template.substitute(
                name=_escape_json_string(name), affiliation=_escape_json_string(affiliation)
            )
            authors.append(json.loads(author))

            contact_template = Template(CONTACT_METADATA_TEMPLATE)
            contact = contact_template.substitute(name=_escape_json_string(name), email=email)
            contacts.append(json.loads(contact))

        return authors, contacts


@attr.s
class _DataverseDeposition:
    """Dataverse record for deposit."""

    access_token = attr.ib(kw_only=True)
    server_url = attr.ib(kw_only=True)
    dataset_pid = attr.ib(kw_only=True, default=None)

    DATASET_CREATE_PATH = "dataverses/{dataverseName}/datasets"
    FILE_UPLOAD_PATH = "datasets/:persistentId/add"
    DATASET_PUBLISH_PATH = "datasets/:persistentId/actions/:publish"

    def create_dataset(self, dataverse_name, metadata):
        """Create a dataset in a given dataverse."""
        api_path = self.DATASET_CREATE_PATH.format(dataverseName=dataverse_name)
        url = self._make_url(api_path=api_path)

        response = self._post(url=url, json=metadata)
        self._check_response(response)

        self.dataset_pid = response.json()["data"]["persistentId"]

        return response

    def upload_file(self, full_path, path_in_dataset):
        """Upload a file to a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError("Dataset not created.")

        url = self._make_url(self.FILE_UPLOAD_PATH, persistentId=self.dataset_pid)

        params = {"directoryLabel": str(path_in_dataset.parent)}
        data = dict(jsonData=json.dumps(params))

        files = {"file": (path_in_dataset.name, open(full_path, "rb"))}

        response = self._post(url=url, data=data, files=files)
        self._check_response(response)

        return response

    def publish_dataset(self):
        """Publish a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError("Dataset not created.")

        url = self._make_url(self.DATASET_PUBLISH_PATH, persistentId=self.dataset_pid, type="major")

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
        from renku.core.util import requests

        headers = {"X-Dataverse-key": self.access_token}
        try:
            return requests.post(url=url, json=json, data=data, files=files, headers=headers)
        except errors.RequestError as e:
            raise errors.ExportError("Cannot POST to remote server.") from e

    @staticmethod
    def _check_response(response):
        if response.status_code not in [200, 201, 202]:
            if response.status_code == 401:
                raise errors.AuthenticationError("Access unauthorized - update access token.")
            json_res = response.json()
            raise errors.ExportError(
                "HTTP {} - Cannot export dataset: {}".format(
                    response.status_code, json_res["message"] if "message" in json_res else json_res["status"]
                )
            )


def _escape_json_string(value):
    """Create a JSON-safe string."""
    if isinstance(value, str):
        return json.dumps(value)[1:-1]
    return value


def _make_request(uri):
    """Execute network request."""
    from renku.core.util import requests

    response = requests.get(uri, headers={"Accept": "application/json"})
    if response.status_code != 200:
        raise LookupError("record not found. Status: {}".format(response.status_code))
    return response
