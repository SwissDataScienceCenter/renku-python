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
"""Zenodo API integration."""

import json
import os
import pathlib
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urlparse

from tqdm import tqdm

from renku.core import errors
from renku.core.dataset.providers.api import ExporterApi, ProviderApi, ProviderRecordSerializerApi
from renku.core.util.file_size import bytes_to_unit

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderDataset


ZENODO_BASE_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org/"

ZENODO_API_PATH = "api"

ZENODO_DEPOSIT_PATH = "deposit"
ZENODO_PUBLISH_PATH = "record"

ZENODO_PUBLISH_ACTION_PATH = "depositions/{0}/actions/publish"
ZENODO_METADATA_URL = "depositions/{0}"
ZENODO_FILES_URL = "depositions/{0}/files"
ZENODO_NEW_DEPOSIT_URL = "depositions"


def make_records_url(record_id):
    """Create URL to access record by ID.

    Args:
        record_id:  The id of the record.

    Returns:
        str: Full URL for the record.
    """
    return urllib.parse.urljoin(ZENODO_BASE_URL, pathlib.posixpath.join(ZENODO_API_PATH, "records", record_id))


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


def _metadata_converter(data: Dict[str, Any]) -> ZenodoMetadataSerializer:
    """Convert dict to ZenodoMetadata instance.

    Args:
        data: The dict data to convert.

    Returns:
        ZenodoMetadataSerializer: Serializer containing data in deserialized form.
    """
    all_keys = set(vars(ZenodoMetadataSerializer()).keys())

    _data = {key: data.get(key) for key in all_keys}

    _data["extras"] = {key: data.get(key) for key in (data.keys()) - all_keys}

    serialized = ZenodoMetadataSerializer(**_data)
    return serialized


class ZenodoRecordSerializer(ProviderRecordSerializerApi):
    """Zenodo record."""

    def __init__(
        self,
        uri: str,
        *,
        conceptdoi=None,
        conceptrecid=None,
        created=None,
        doi=None,
        doi_url=None,
        files=None,
        id=None,
        links=None,
        metadata=None,
        modified=None,
        owner=None,
        record_id=None,
        state=None,
        submitted=None,
        title=None,
    ):
        super().__init__(uri=uri)

        metadata = _metadata_converter(metadata) if metadata is not None else None

        self.conceptdoi = conceptdoi
        self.conceptrecid = conceptrecid
        self.created = created
        self.doi = doi
        self.doi_url = doi_url
        self.files = files
        self.id = id
        self.links = links
        self.metadata: Optional[ZenodoMetadataSerializer] = metadata
        self.modified = modified
        self.owner = owner
        self.record_id: Optional[str] = str(record_id) if record_id is not None else None
        self.state = state
        self.submitted = submitted
        self.title = title

        self._jsonld = None

    @property
    def version(self):
        """Get record version."""
        return self.metadata.version

    @property
    def latest_uri(self):
        """Get URI of latest version."""
        return self.links.get("latest_html")

    def is_last_version(self, uri):
        """Check if this record is the latest version."""
        return ZenodoProvider.record_id(self.links.get("latest_html")) == self.record_id

    def get_jsonld(self):
        """Get record metadata as jsonld."""
        response = _make_request(self._uri, accept="application/ld+json")
        self._jsonld = response.json()

        if "image" in self._jsonld and isinstance(self._jsonld["image"], str):
            self._jsonld["image"] = {
                "@id": self._jsonld["image"],
                "@type": "ImageObject",
                "position": 1,
                "contentUrl": self._jsonld["image"],
            }

        return self._jsonld

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if not self.files:
            raise LookupError("no files have been found - deposit is empty or protected")

        return [ZenodoFileSerializer(**file) for file in self.files]

    def as_dataset(self, client) -> "ProviderDataset":
        """Deserialize `ZenodoRecordSerializer` to `Dataset`."""
        from marshmallow import pre_load

        from renku.command.schema.agent import PersonSchema
        from renku.core.dataset.providers.models import ProviderDataset, ProviderDatasetFile, ProviderDatasetSchema

        class _ZenodoDatasetSchema(ProviderDatasetSchema):
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
        dataset = ProviderDataset.from_jsonld(metadata, schema_class=_ZenodoDatasetSchema)

        self._files_info = [
            ProviderDatasetFile(
                source=file.remote_url.geturl(),
                filename=Path(file.filename).name,
                checksum=file.checksum,
                size_in_mb=bytes_to_unit(file.filesize, "mi"),
                filetype=file.type,
                path="",
            )
            for file in files
        ]

        return dataset


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
            pathlib.posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_PUBLISH_ACTION_PATH.format(self.id)),
        )

        return url

    @property
    def attach_metadata_url(self):
        """Return URL for attaching metadata."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            pathlib.posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_METADATA_URL.format(self.id)),
        )
        return url

    @property
    def upload_file_url(self):
        """Return URL for uploading file."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            pathlib.posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_FILES_URL.format(self.id)),
        )
        return url

    @property
    def new_deposit_url(self):
        """Return URL for creating new deposit."""
        url = urllib.parse.urljoin(
            self.exporter.zenodo_url,
            pathlib.posixpath.join(ZENODO_API_PATH, ZENODO_DEPOSIT_PATH, ZENODO_NEW_DEPOSIT_URL),
        )
        return url

    @property
    def published_at(self):
        """Return published at URL."""
        url = urllib.parse.urljoin(self.exporter.zenodo_url, pathlib.posixpath.join(ZENODO_PUBLISH_PATH, str(self.id)))
        return url

    @property
    def deposit_at(self):
        """Return deposit at URL."""
        url = urllib.parse.urljoin(self.exporter.zenodo_url, pathlib.posixpath.join(ZENODO_DEPOSIT_PATH, str(self.id)))
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

    def publish_deposition(self, secret):
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


class ZenodoExporter(ExporterApi):
    """Zenodo export manager."""

    HEADERS = {"Content-Type": "application/json"}

    def __init__(self, dataset, access_token):
        self.dataset = dataset
        self.access_token = access_token

    @property
    def zenodo_url(self):
        """Returns correct Zenodo URL based on environment."""
        if "ZENODO_USE_SANDBOX" in os.environ:
            return ZENODO_SANDBOX_URL

        return ZENODO_BASE_URL

    def set_access_token(self, access_token):
        """Set access token."""
        self.access_token = access_token

    def access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urlparse("https://zenodo.org/account/settings/applications/tokens/new/").geturl()

    @property
    def default_params(self):
        """Create request default parameters."""
        return {"access_token": self.access_token}

    def dataset_to_request(self):
        """Prepare dataset metadata for request."""
        from renku.command.schema.dataset import dump_dataset_as_jsonld

        jsonld = dump_dataset_as_jsonld(self.dataset)
        jsonld["upload_type"] = "dataset"
        return jsonld

    def export(self, publish, tag=None, client=None, **kwargs):
        """Execute entire export process."""
        # Step 1. Create new deposition
        deposition = ZenodoDeposition(exporter=self)

        # Step 2. Attach metadata to deposition
        deposition.attach_metadata(self.dataset, tag)

        # Step 3. Upload all files to created deposition
        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file in self.dataset.files:
                filepath = client.repository.copy_content_to_file(path=file.entity.path, checksum=file.entity.checksum)
                deposition.upload_file(filepath, path_in_repo=file.entity.path)
                progressbar.update(1)

        # Step 4. Publish newly created deposition
        if publish:
            deposition.publish_deposition(self.access_token)
            return deposition.published_at

        return deposition.deposit_at


class ZenodoProvider(ProviderApi):
    """Zenodo registry API provider."""

    def __init__(self, is_doi: bool = False):
        self.is_doi = is_doi

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given URI."""
        if "zenodo" in uri.lower():
            return True

        return False

    @staticmethod
    def supports_export():
        """Whether this provider supports dataset export."""
        return True

    @staticmethod
    def supports_import():
        """Whether this provider supports dataset import."""
        return True

    @staticmethod
    def record_id(uri):
        """Extract record id from URI."""
        return urlparse(uri).path.split("/")[-1]

    def find_record(self, uri, client=None, **kwargs) -> ZenodoRecordSerializer:
        """Retrieves a record from Zenodo.

        Args:
            uri: DOI or URL.
            client: The ``LocalClient`` (Default value = None).

        Returns:
            ZenodoRecordSerializer: Record found.

        """
        if self.is_doi:
            return self.find_record_by_doi(uri)

        return self._get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve the DOI and make a record for the retrieved record id."""
        from renku.core.dataset.providers.doi import DOIProvider

        doi = DOIProvider().find_record(doi)
        return self._get_record(ZenodoProvider.record_id(doi.url))

    @staticmethod
    def _get_record(uri):
        """Retrieve record metadata and return ``ZenodoRecordSerializer``."""
        response = _make_request(uri)

        return ZenodoRecordSerializer(**response.json(), uri=uri)

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return ZenodoExporter(dataset=dataset, access_token=access_token)


def _make_request(uri, accept: str = "application/json"):
    """Execute network request."""
    from renku.core.util import requests

    record_id = ZenodoProvider.record_id(uri)

    response = requests.get(make_records_url(record_id), headers={"Accept": accept})
    if response.status_code != 200:
        raise LookupError("record not found. Status: {}".format(response.status_code))
    return response
