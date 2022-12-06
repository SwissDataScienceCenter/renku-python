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
"""Dataverse API integration."""

import json
import posixpath
import re
import urllib
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib import parse as urlparse

from renku.core import errors
from renku.core.config import get_value, set_value
from renku.core.dataset.providers.api import (
    ExporterApi,
    ExportProviderInterface,
    ImportProviderInterface,
    ProviderApi,
    ProviderPriority,
)
from renku.core.dataset.providers.dataverse_metadata_templates import (
    AUTHOR_METADATA_TEMPLATE,
    CONTACT_METADATA_TEMPLATE,
    DATASET_METADATA_TEMPLATE,
)
from renku.core.dataset.providers.doi import DOIProvider
from renku.core.dataset.providers.repository import RepositoryImporter, make_request
from renku.core.util import communication
from renku.core.util.doi import extract_doi, get_doi_url, is_doi
from renku.core.util.urls import remove_credentials
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset, ProviderParameter
    from renku.domain_model.dataset import Dataset, DatasetTag

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


class DataverseProvider(ProviderApi, ExportProviderInterface, ImportProviderInterface):
    """Dataverse API provider."""

    priority = ProviderPriority.HIGH
    name = "Dataverse"

    def __init__(self, uri: Optional[str], is_doi: bool = False):
        super().__init__(uri=uri)

        self.is_doi = is_doi
        self._server_url = None
        self._dataverse_name = None
        self._publish: bool = False

    @staticmethod
    def supports(uri):
        """Check if provider supports a given URI."""
        is_doi_ = is_doi(uri)

        is_dataverse_uri = is_doi_ is None and check_dataverse_uri(uri)
        is_dataverse_doi = is_doi_ and check_dataverse_doi(is_doi_.group(0))

        return is_dataverse_uri or is_dataverse_doi

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter("dataverse-server", help="Dataverse server URL.", type=str),
            ProviderParameter("dataverse-name", help="Dataverse name to export to.", type=str),
            ProviderParameter("publish", help="Publish the exported dataset.", is_flag=True),
        ]

    @staticmethod
    def record_id(uri):
        """Extract record id from URI."""
        parsed = urlparse.urlparse(uri)
        return urlparse.parse_qs(parsed.query)["persistentId"][0]

    def get_importer(self, **kwargs) -> "DataverseImporter":
        """Get importer for a record from Dataverse.

        Returns:
            DataverseImporter: The found record
        """

        def get_export_uri(uri):
            """Gets a dataverse api export URI from a dataverse entry."""
            record_id = DataverseProvider.record_id(uri)
            return make_records_url(record_id, uri)

        uri = self.uri
        if self.is_doi:
            doi = DOIProvider(uri=uri).get_importer()
            uri = doi.uri

        uri = get_export_uri(uri)
        response = make_request(uri)

        return DataverseImporter(json=response.json(), uri=uri, original_uri=self.uri)

    def get_exporter(
        self,
        dataset: "Dataset",
        *,
        tag: Optional["DatasetTag"],
        dataverse_server: str = None,
        dataverse_name: str = None,
        publish: bool = False,
        **kwargs,
    ) -> "ExporterApi":
        """Create export manager for given dataset."""

        def set_export_parameters():
            """Set and validate required parameters for exporting for a provider."""

            server = dataverse_server
            config_base_url = "server_url"
            if not server:
                server = get_value("dataverse", config_base_url)
            else:
                set_value("dataverse", config_base_url, server, global_only=True)

            if not server:
                raise errors.ParameterError("Dataverse server URL is required.")

            if not dataverse_name:
                raise errors.ParameterError("Dataverse name is required.")

            self._server_url = server  # type: ignore
            self._dataverse_name = dataverse_name  # type: ignore
            self._publish = publish

        set_export_parameters()
        return DataverseExporter(dataset=dataset, server_url=self._server_url, dataverse_name=self._dataverse_name)


class DataverseImporter(RepositoryImporter):
    """Dataverse record serializer."""

    def __init__(self, uri: str, original_uri: str, json: Dict[str, Any]):
        super().__init__(uri=uri, original_uri=original_uri)
        self._json: Dict[str, Any] = json

    def is_latest_version(self):
        """Check if record is at last possible version."""
        return True

    @staticmethod
    def _convert_json_property_name(property_name):
        """Removes '@' and converts names to snake_case."""
        property_name = property_name.strip("@")
        property_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", property_name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", property_name).lower()

    @property
    def version(self):
        """Get the major and minor version of this dataset."""
        uri = make_versions_url(DataverseProvider.record_id(self._uri), self._uri)
        response = make_request(uri).json()
        newest_version = response["data"][0]
        return "{}.{}".format(newest_version["versionNumber"], newest_version["versionMinorNumber"])

    @property
    def latest_uri(self):
        """Get URI of latest version."""
        return self._uri

    def get_files(self):
        """Get Dataverse files metadata as ``DataverseFileSerializer``."""
        files = []

        for f in self._json["distribution"]:
            mapped_file = {self._convert_json_property_name(k): v for k, v in f.items()}
            mapped_file["parent_url"] = self._uri
            files.append(mapped_file)

        if not files:
            raise LookupError("no files have been found - deposit is empty or protected")

        return [DataverseFileSerializer(**file) for file in files]

    def fetch_provider_dataset(self) -> "ProviderDataset":
        """Deserialize a ``Dataset``."""
        from marshmallow import pre_load

        from renku.command.schema.agent import PersonSchema
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile, ProviderDatasetSchema
        from renku.domain_model.dataset import Url, generate_default_name

        class DataverseDatasetSchema(ProviderDatasetSchema):
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
        dataset = ProviderDataset.from_jsonld(data=self._json, schema_class=DataverseDatasetSchema)
        dataset.version = self.version
        dataset.name = generate_default_name(title=dataset.title or "", version=dataset.version)
        dataset.same_as = (
            Url(url_str=get_doi_url(dataset.identifier))
            if is_doi(dataset.identifier)
            else Url(url_id=remove_credentials(self.original_uri))
        )

        if dataset.description and not dataset.description.strip():
            dataset.description = None

        for creator in dataset.creators:
            if creator.affiliation == "":
                creator.affiliation = None

        self._provider_dataset_files = [
            ProviderDatasetFile(
                source=file.remote_url.geturl(),
                filename=Path(file.name).name,
                checksum="",
                filesize=file.content_size,
                filetype=file.file_format,
                path="",
            )
            for file in files
        ]

        self._provider_dataset = dataset
        return self._provider_dataset


class DataverseFileSerializer:
    """Dataverse record file."""

    def __init__(
        self,
        *,
        content_size=None,
        content_url=None,
        description=None,
        file_format=None,
        id=None,
        identifier=None,
        name=None,
        parent_url=None,
        type=None,
    ):
        self.content_size = content_size
        self.content_url = content_url
        self.description = description
        self.file_format = file_format
        self.id = id
        self.identifier = identifier
        self.name = name
        self.parent_url = parent_url
        self.type = type

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


class DataverseExporter(ExporterApi):
    """Dataverse export manager."""

    def __init__(self, *, dataset, server_url=None, dataverse_name=None, publish=False):
        super().__init__(dataset)
        self._access_token = None
        self._server_url = server_url
        self._dataverse_name = dataverse_name
        self._publish = publish

    def set_access_token(self, access_token):
        """Set access token."""
        self._access_token = access_token

    def get_access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urljoin(self._server_url, "/dataverseuser.xhtml?selectTab=apiTokenTab")

    def export(self, **kwargs):
        """Execute export process."""
        from renku.domain_model.dataset import get_file_path_in_dataset

        deposition = _DataverseDeposition(server_url=self._server_url, access_token=self._access_token)
        metadata = self._get_dataset_metadata()
        response = deposition.create_dataset(dataverse_name=self._dataverse_name, metadata=metadata)
        dataset_pid = response.json()["data"]["persistentId"]
        repository = project_context.repository

        with communication.progress("Uploading files ...", total=len(self.dataset.files)) as progressbar:
            for file in self.dataset.files:
                filepath = repository.copy_content_to_file(path=file.entity.path, checksum=file.entity.checksum)
                path_in_dataset = get_file_path_in_dataset(dataset=self.dataset, dataset_file=file)
                deposition.upload_file(full_path=filepath, path_in_dataset=path_in_dataset)
                progressbar.update()

        if self._publish:
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


class _DataverseDeposition:
    """Dataverse record for deposit."""

    def __init__(self, *, access_token, server_url, dataset_pid=None):
        self.access_token = access_token
        self.server_url = server_url
        self.dataset_pid = dataset_pid

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
        path = posixpath.join(DATAVERSE_API_PATH, api_path)

        query_params_str = urllib.parse.urlencode(query_params)
        url_parts = url_parts._replace(path=path, query=query_params_str)
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
        from renku.core.util import requests

        try:
            requests.check_response(response=response)
        except errors.RequestError:
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


def check_dataverse_uri(url):
    """Check if an URL points to a dataverse instance."""
    from renku.core.util import requests

    url_parts = list(urlparse.urlparse(url))
    url_parts[2] = posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSION_API)

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
        doi = DOIProvider(uri=doi).get_importer()
    except LookupError:
        return False

    return check_dataverse_uri(doi.uri)


def make_records_url(record_id, base_url):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = posixpath.join(DATAVERSE_API_PATH, DATAVERSE_METADATA_API)
    args_dict = {"exporter": DATAVERSE_EXPORTER, "persistentId": record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


def make_versions_url(record_id, base_url):
    """Create URL to access the versions of a record."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSIONS_API)
    args_dict = {"exporter": DATAVERSE_EXPORTER, "persistentId": record_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)


def make_file_url(file_id, base_url):
    """Create URL to access record by ID."""
    url_parts = list(urlparse.urlparse(base_url))
    url_parts[2] = posixpath.join(DATAVERSE_API_PATH, DATAVERSE_FILE_API)
    args_dict = {"persistentId": file_id}
    url_parts[4] = urllib.parse.urlencode(args_dict)
    return urllib.parse.urlunparse(url_parts)
