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
"""Zenodo API integration."""

import json
import os
import posixpath
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.parse import urlparse

from renku.core import errors
from renku.core.dataset.providers.api import (
    ExporterApi,
    ExportProviderInterface,
    ImportProviderInterface,
    ProviderApi,
    ProviderPriority,
)
from renku.core.dataset.providers.repository import RepositoryImporter, make_request
from renku.core.util import communication
from renku.core.util.doi import is_doi
from renku.core.util.urls import remove_credentials
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset, ProviderParameter
    from renku.domain_model.dataset import Dataset, DatasetTag


ZENODO_BASE_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org/"

ZENODO_API_PATH = "api"

ZENODO_DEPOSIT_PATH = "deposit"
ZENODO_PUBLISH_PATH = "record"

ZENODO_PUBLISH_ACTION_PATH = "depositions/{0}/actions/publish"
ZENODO_METADATA_URL = "depositions/{0}"
ZENODO_FILES_URL = "depositions/{0}/files"
ZENODO_NEW_DEPOSIT_URL = "depositions"


class ZenodoProvider(ProviderApi, ExportProviderInterface, ImportProviderInterface):
    """Zenodo registry API provider."""

    priority = ProviderPriority.HIGH
    name = "Zenodo"

    def __init__(self, uri: Optional[str], is_doi: bool = False):
        super().__init__(uri=uri)

        self.is_doi = is_doi
        self._publish: bool = False

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        if "zenodo" in uri.lower():
            return True

        return False

    @staticmethod
    def get_record_id(uri):
        """Extract record id from URI."""
        return urlparse(uri).path.split("/")[-1]

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [ProviderParameter("publish", help="Publish the exported dataset.", is_flag=True)]

    def get_importer(self, **kwargs) -> "ZenodoImporter":
        """Get importer for a record from Zenodo."""
        from renku.core.dataset.providers.doi import DOIProvider

        uri = self.uri
        if self.is_doi:
            # NOTE: Resolve the DOI and make a record for the retrieved record id
            doi = DOIProvider(uri=uri).get_importer()
            uri = doi.uri

        response = _make_request(uri)
        return ZenodoImporter(uri=uri, original_uri=self.uri, json=response.json())

    def get_exporter(
        self, dataset: "Dataset", *, tag: Optional["DatasetTag"], publish: bool = False, **kwargs
    ) -> "ZenodoExporter":
        """Create export manager for given dataset."""
        self._publish = publish
        return ZenodoExporter(dataset=dataset, publish=self._publish, tag=tag)


class ZenodoImporter(RepositoryImporter):
    """Zenodo importer."""

    def __init__(self, *, uri: str, original_uri, json: Dict[str, Any]):
        super().__init__(uri=uri, original_uri=original_uri)

        self._jsonld: Optional[dict] = None
        self._json = json

        metadata = self._json.pop("metadata", {})
        self._json["metadata"] = ZenodoMetadataSerializer.from_metadata(metadata) if metadata is not None else None
        record_id = self._json.pop("record_id", None)
        self._json["record_id"] = str(record_id) if record_id is not None else None

        # NOTE: Make sure that these properties have a default value
        self._json["links"] = self._json.pop("links", {})
        self._json["files"] = self._json.pop("files", [])

    @property
    def version(self):
        """Get record version."""
        return self._json["metadata"].version

    @property
    def latest_uri(self):
        """Get URI of latest version."""
        return self._json["links"].get("latest_html")

    def is_latest_version(self):
        """Check if this record is the latest version."""
        return ZenodoProvider.get_record_id(self._json["links"].get("latest_html")) == self._json["record_id"]

    def get_jsonld(self):
        """Get record metadata as jsonld."""
        response = _make_request(self._uri, accept="application/ld+json")
        self._jsonld = response.json()

        if self._jsonld is not None and "image" in self._jsonld and isinstance(self._jsonld["image"], str):
            self._jsonld["image"] = {
                "@id": self._jsonld["image"],
                "@type": "ImageObject",
                "position": 1,
                "contentUrl": self._jsonld["image"],
            }

        return self._jsonld

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if not self._json["files"]:
            raise LookupError("no files have been found - deposit is empty or protected")

        return [ZenodoFileSerializer(**file) for file in self._json["files"]]

    def fetch_provider_dataset(self) -> "ProviderDataset":
        """Deserialize a `Dataset`."""
        from marshmallow import pre_load

        from renku.command.schema.agent import PersonSchema
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile, ProviderDatasetSchema
        from renku.domain_model.dataset import Url, generate_default_name

        class ZenodoDatasetSchema(ProviderDatasetSchema):
            """Schema for Dataverse datasets."""

            @pre_load
            def fix_data(self, data, **kwargs):
                """Fix data that is received from Dataverse."""
                # Fix context
                context = data.get("@context")
                if context and isinstance(context, str):
                    if context == "https://schema.org/":
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

                # Delete existing isPartOf
                data.pop("isPartOf", None)
                data.pop("sameAs", None)

                return data

        files = self.get_files()
        metadata = self.get_jsonld()
        dataset = ProviderDataset.from_jsonld(metadata, schema_class=ZenodoDatasetSchema)
        dataset.name = generate_default_name(title=dataset.title or "", version=dataset.version)
        dataset.same_as = Url(url_id=remove_credentials(self.original_uri))
        if is_doi(dataset.identifier):
            dataset.same_as = Url(url_str=urllib.parse.urljoin("https://doi.org", dataset.identifier))

        self._provider_dataset_files = [
            ProviderDatasetFile(
                source=file.remote_url.geturl(),
                filename=Path(file.filename).name,
                checksum=file.checksum,
                filesize=file.filesize,
                filetype=file.type,
                path="",
            )
            for file in files
        ]

        self._provider_dataset = dataset
        return self._provider_dataset


class ZenodoFileSerializer:
    """Zenodo record file."""

    def __init__(self, *, id=None, checksum=None, links=None, filename=None, filesize=None):
        self.id = id
        self.checksum = checksum
        self.links = links
        self.filename = filename
        self.filesize = filesize

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        return urllib.parse.urlparse(self.links["download"])

    @property
    def type(self):
        """Get file type."""
        return self.filename.split(".")[-1]


class ZenodoMetadataSerializer:
    """Zenodo metadata."""

    def __init__(
        self,
        *,
        access_right=None,
        communities=None,
        contributors=None,
        creators=None,
        description=None,
        doi=None,
        extras=None,
        grants=None,
        image_type=None,
        journal_issue=None,
        journal_pages=None,
        journal_title=None,
        journal_volume=None,
        keywords=None,
        language=None,
        license=None,
        notes=None,
        prereserve_doi=None,
        publication_date=None,
        publication_type=None,
        references=None,
        related_identifiers=None,
        title=None,
        upload_type=None,
        version=None,
    ):
        self.access_right = access_right
        self.communities = communities
        self.contributors = contributors
        self.creators = creators
        self.description = description
        self.doi = doi
        self.extras = extras
        self.grants = grants
        self.image_type = image_type
        self.journal_issue = journal_issue
        self.journal_pages = journal_pages
        self.journal_title = journal_title
        self.journal_volume = journal_volume
        self.keywords = keywords
        self.language = language
        self.license = license
        self.notes = notes
        self.prereserve_doi = prereserve_doi
        self.publication_date = publication_date
        self.publication_type = publication_type
        self.references = references
        self.related_identifiers = related_identifiers
        self.title = title
        self.upload_type = upload_type
        self.version = version

    @classmethod
    def from_metadata(cls, metadata: Dict[str, Any]) -> "ZenodoMetadataSerializer":
        """Create an instance from a metadata dict.

        Args:
            metadata: The dict data to convert.

        Returns:
            ZenodoMetadataSerializer: Serializer containing data in deserialized form.
        """
        all_keys = set(vars(ZenodoMetadataSerializer()).keys())

        data = {key: metadata.get(key) for key in all_keys}
        data["extras"] = {key: metadata.get(key) for key in (metadata.keys() - all_keys)}

        return ZenodoMetadataSerializer(**data)


class ZenodoExporter(ExporterApi):
    """Zenodo export manager."""

    HEADERS = {"Content-Type": "application/json"}

    def __init__(self, dataset, publish, tag):
        super().__init__(dataset)
        self._access_token = None
        self._publish = publish
        self._tag = tag

    @property
    def zenodo_url(self):
        """Returns correct Zenodo URL based on environment."""
        if "ZENODO_USE_SANDBOX" in os.environ:
            return ZENODO_SANDBOX_URL

        return ZENODO_BASE_URL

    def set_access_token(self, access_token):
        """Set access token."""
        self._access_token = access_token

    def get_access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urlparse("https://zenodo.org/account/settings/applications/tokens/new/").geturl()

    @property
    def default_params(self):
        """Create request default parameters."""
        return {"access_token": self._access_token}

    def dataset_to_request(self):
        """Prepare dataset metadata for request."""
        from renku.command.schema.dataset import dump_dataset_as_jsonld

        jsonld = dump_dataset_as_jsonld(self.dataset)
        jsonld["upload_type"] = "dataset"
        return jsonld

    def export(self, **kwargs):
        """Execute entire export process."""
        # Step 1. Create new deposition
        deposition = ZenodoDeposition(exporter=self)

        # Step 2. Attach metadata to deposition
        deposition.attach_metadata(self.dataset, self._tag)

        # Step 3. Upload all files to created deposition
        with communication.progress("Uploading files ...", total=len(self.dataset.files)) as progressbar:
            for file in self.dataset.files:
                filepath = project_context.repository.copy_content_to_file(
                    path=file.entity.path, checksum=file.entity.checksum
                )
                deposition.upload_file(filepath, path_in_repo=file.entity.path)
                progressbar.update()

        # Step 4. Publish newly created deposition
        if self._publish:
            deposition.publish_deposition()
            return deposition.published_at

        return deposition.deposit_at


class ZenodoDeposition:
    """Zenodo record for a deposit."""

    def __init__(self, exporter, id=None):
        self.exporter = exporter
        self.id = id

        response = self.new_deposition()
        self.id = response.json()["id"]

    @property
    def publish_url(self):
        """Returns publish URL."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_PUBLISH_ACTION_PATH.format(self.id)),
        )

        return url

    @property
    def attach_metadata_url(self):
        """Return URL for attaching metadata."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_METADATA_URL.format(self.id)),
        )
        return url

    @property
    def upload_file_url(self):
        """Return URL for uploading file."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_FILES_URL.format(self.id)),
        )
        return url

    @property
    def new_deposit_url(self):
        """Return URL for creating new deposit."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_NEW_DEPOSIT_URL),
        )
        return url

    @property
    def published_at(self):
        """Return published at URL."""
        url = urllib.parse.urljoin(self.exporter.zenodo_url, posixpath.join(ZENODO_PUBLISH_PATH, str(self.id)))
        return url

    @property
    def deposit_at(self):
        """Return deposit at URL."""
        url = urllib.parse.urljoin(self.exporter.zenodo_url, posixpath.join(ZENODO_DEPOSIT_PATH, str(self.id)))
        return url

    def new_deposition(self):
        """Create new deposition on Zenodo."""
        from renku.core.util import requests

        response = requests.post(
            url=self.new_deposit_url, params=self.exporter.default_params, json={}, headers=self.exporter.HEADERS
        )
        self._check_response(response)

        return response

    def upload_file(self, filepath, path_in_repo):
        """Upload and attach a file to existing deposition on Zenodo."""
        from renku.core.util import requests

        request_payload = {"filename": Path(path_in_repo).name}
        file = {"file": (Path(path_in_repo).name, open(str(filepath), "rb"))}
        response = requests.post(
            url=self.upload_file_url, params=self.exporter.default_params, data=request_payload, files=file
        )
        self._check_response(response)

        return response

    def attach_metadata(self, dataset, tag):
        """Attach metadata to deposition on Zenodo."""
        from renku.core.util import requests

        request_payload = {
            "metadata": {
                "title": dataset.title,
                "upload_type": "dataset",
                "description": dataset.description if dataset.description else None,
                "creators": [
                    {"name": creator.name, "affiliation": creator.affiliation if creator.affiliation else None}
                    for creator in dataset.creators
                ],
            }
        }

        version = tag.name if tag else dataset.version

        if version:
            request_payload["metadata"]["version"] = version

        response = requests.put(
            url=self.attach_metadata_url,
            params=self.exporter.default_params,
            data=json.dumps(request_payload),
            headers=self.exporter.HEADERS,
        )
        self._check_response(response)

        return response

    def publish_deposition(self):
        """Publish existing deposition."""
        from renku.core.util import requests

        response = requests.post(url=self.publish_url, params=self.exporter.default_params)
        self._check_response(response)

        return response

    @staticmethod
    def _check_response(response):
        from renku.core.util import requests

        try:
            requests.check_response(response=response)
        except errors.RequestError:
            if response.status_code == 400:
                err_response = response.json()
                messages = [
                    '"{0}" failed with "{1}"'.format(err["field"], err["message"]) for err in err_response["errors"]
                ]

                raise errors.ExportError(
                    "\n" + "\n".join(messages) + "\nSee `renku dataset edit -h` for details on how to edit" " metadata"
                )
            else:
                raise errors.ExportError(response.content)


def _make_request(uri, accept: str = "application/json"):
    """Execute network request."""
    record_id = ZenodoProvider.get_record_id(uri)
    url = make_records_url(record_id)

    return make_request(url=url, accept=accept)


def make_records_url(record_id):
    """Create URL to access record by ID.

    Args:
        record_id:  The id of the record.

    Returns:
        str: Full URL for the record.
    """
    return urllib.parse.urljoin(ZENODO_BASE_URL, posixpath.join(ZENODO_API_PATH, "records", record_id))
