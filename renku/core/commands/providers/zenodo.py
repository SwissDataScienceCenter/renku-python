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
from urllib.parse import urlparse

import attr
import requests
from marshmallow import pre_load
from tqdm import tqdm

from renku.core import errors
from renku.core.commands.providers.api import ExporterApi, ProviderApi
from renku.core.commands.providers.doi import DOIProvider
from renku.core.models.datasets import Dataset, DatasetFile, DatasetSchema
from renku.core.models.provenance.agents import PersonSchema
from renku.core.utils.requests import retry

ZENODO_BASE_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org/"

ZENODO_API_PATH = "api"

ZENODO_DEPOSIT_PATH = "deposit"
ZENODO_PUBLISH_PATH = "record"

ZENODO_PUBLISH_ACTION_PATH = "depositions/{0}/actions/publish"
ZENODO_METADATA_URL = "depositions/{0}"
ZENODO_FILES_URL = "depositions/{0}/files"
ZENODO_NEW_DEPOSIT_URL = "depositions"


class _ZenodoDatasetSchema(DatasetSchema):
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

        return data


def make_records_url(record_id):
    """Create URL to access record by ID."""
    return urllib.parse.urljoin(ZENODO_BASE_URL, pathlib.posixpath.join(ZENODO_API_PATH, "records", record_id))


def check_or_raise(response):
    """Check for expected response status code."""
    if response.status_code not in [200, 201, 202]:
        if response.status_code == 401:
            raise errors.AuthenticationError("Access unauthorized - update access token.")

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


@attr.s
class ZenodoFileSerializer:
    """Zenodo record file."""

    id = attr.ib(default=None, kw_only=True)

    checksum = attr.ib(default=None, kw_only=True)

    links = attr.ib(default=None, kw_only=True)

    filename = attr.ib(default=None, kw_only=True)

    filesize = attr.ib(default=None, kw_only=True)

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        return urllib.parse.urlparse(self.links["download"])

    @property
    def type(self):
        """Get file type."""
        return self.filename.split(".")[-1]


@attr.s
class ZenodoMetadataSerializer:
    """Zenodo metadata."""

    access_right = attr.ib(default=None, kw_only=True)

    communities = attr.ib(default=None, kw_only=True)

    contributors = attr.ib(default=None, kw_only=True)

    creators = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    doi = attr.ib(default=None, kw_only=True)

    extras = attr.ib(default=None, kw_only=True)

    grants = attr.ib(default=None, kw_only=True)

    image_type = attr.ib(default=None, kw_only=True)

    journal_issue = attr.ib(default=None, kw_only=True)

    journal_pages = attr.ib(default=None, kw_only=True)

    journal_title = attr.ib(default=None, kw_only=True)

    journal_volume = attr.ib(default=None, kw_only=True)

    keywords = attr.ib(default=None, kw_only=True)

    language = attr.ib(default=None, kw_only=True)

    license = attr.ib(default=None, kw_only=True)

    notes = attr.ib(default=None, kw_only=True)

    prereserve_doi = attr.ib(default=None, kw_only=True)

    publication_date = attr.ib(default=None, kw_only=True)

    publication_type = attr.ib(default=None, kw_only=True)

    references = attr.ib(default=None, kw_only=True)

    related_identifiers = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, kw_only=True)

    upload_type = attr.ib(default=None, kw_only=True)

    version = attr.ib(default=None, kw_only=True)


def _metadata_converter(data):
    """Convert dict to ZenodoMetadata instance."""
    all_keys = set(vars(ZenodoMetadataSerializer()).keys())

    _data = {key: data.get(key) for key in all_keys}

    _data["extras"] = {key: data.get(key) for key in (data.keys()) - all_keys}

    serialized = ZenodoMetadataSerializer(**_data)
    return serialized


@attr.s
class ZenodoRecordSerializer:
    """Zenodo record."""

    _jsonld = attr.ib(default=None, init=False)

    id = attr.ib(default=None, kw_only=True)

    doi = attr.ib(default=None, kw_only=True)

    doi_url = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, kw_only=True)

    files = attr.ib(default=None, kw_only=True)

    links = attr.ib(default=None, kw_only=True)

    metadata = attr.ib(default=None, kw_only=True, converter=_metadata_converter)

    modified = attr.ib(default=None, kw_only=True)

    owner = attr.ib(default=None, kw_only=True)

    record_id = attr.ib(default=None, kw_only=True, converter=str)

    state = attr.ib(default=None, kw_only=True)

    submitted = attr.ib(default=None, kw_only=True)

    created = attr.ib(default=None, kw_only=True)

    conceptrecid = attr.ib(default=None, kw_only=True)

    conceptdoi = attr.ib(default=None, kw_only=True)

    _zenodo = attr.ib(default=None, kw_only=True)

    _uri = attr.ib(default=None, kw_only=True)

    @property
    def version(self):
        """Get record version."""
        return self.metadata.version

    @property
    def latest_uri(self):
        """Get uri of latest version."""
        return self.links.get("latest_html")

    def is_last_version(self, uri):
        """Check if this record is the latest version."""
        return ZenodoProvider.record_id(self.links.get("latest_html")) == self.record_id

    def get_jsonld(self):
        """Get record metadata as jsonld."""
        response = self._zenodo.accept_jsonld().make_request(self._uri)
        self._jsonld = response.json()
        return self._jsonld

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if not self.files:
            raise LookupError("no files have been found - deposit is empty or protected")

        return [ZenodoFileSerializer(**file_) for file_ in self.files]

    def as_dataset(self, client):
        """Deserialize `ZenodoRecordSerializer` to `Dataset`."""
        files = self.get_files()
        metadata = self.get_jsonld()
        dataset = Dataset.from_jsonld(metadata, client=client, schema_class=_ZenodoDatasetSchema)

        serialized_files = []
        for file_ in files:
            remote_ = file_.remote_url
            dataset_file = DatasetFile(
                source=remote_.geturl(),
                id=file_.id,
                checksum=file_.checksum,
                filename=file_.filename,
                filesize=file_.filesize,
                filetype=file_.type,
                path="",
            )
            serialized_files.append(dataset_file)

        dataset.files = serialized_files

        if isinstance(dataset.url, dict) and "_id" in dataset.url:
            dataset.url = urllib.parse.urlparse(dataset.url.pop("_id"))
            dataset.url = dataset.url.geturl()

        return dataset


@attr.s
class ZenodoDeposition:
    """Zenodo record for a deposit."""

    exporter = attr.ib()
    id = attr.ib(default=None)

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
        response = requests.post(
            url=self.new_deposit_url, params=self.exporter.default_params, json={}, headers=self.exporter.HEADERS
        )
        check_or_raise(response)

        return response

    def upload_file(self, filepath):
        """Upload and attach a file to existing deposition on Zenodo."""
        request_payload = {"filename": Path(filepath).name}
        file = {"file": open(str(filepath), "rb")}

        response = requests.post(
            url=self.upload_file_url, params=self.exporter.default_params, data=request_payload, files=file,
        )
        check_or_raise(response)

        return response

    def attach_metadata(self, dataset, tag):
        """Attach metadata to deposition on Zenodo."""
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
        check_or_raise(response)

        return response

    def publish_deposition(self, secret):
        """Publish existing deposition."""
        response = requests.post(url=self.publish_url, params=self.exporter.default_params)
        check_or_raise(response)

        return response

    def __attrs_post_init__(self):
        """Post-Init hook to set _id field."""
        response = self.new_deposition()
        self.id = response.json()["id"]


@attr.s
class ZenodoExporter(ExporterApi):
    """Zenodo export manager."""

    HEADERS = {"Content-Type": "application/json"}

    dataset = attr.ib()
    access_token = attr.ib()

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
        """Return endpoint for creation of access token."""
        return urllib.parse.urlparse("https://zenodo.org/account/settings/applications/tokens/new/").geturl()

    @property
    def default_params(self):
        """Create request default params."""
        return {"access_token": self.access_token}

    def dataset_to_request(self):
        """Prepare dataset metadata for request."""
        jsonld = self.dataset.as_jsonld()
        jsonld["upload_type"] = "dataset"
        return jsonld

    def export(self, publish, tag=None, **kwargs):
        """Execute entire export process."""
        # Step 1. Create new deposition
        deposition = ZenodoDeposition(exporter=self)

        # Step 2. Attach metadata to deposition
        deposition.attach_metadata(self.dataset, tag)

        # Step 3. Upload all files to created deposition
        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file_ in self.dataset.files:
                deposition.upload_file(file_.full_path,)
                progressbar.update(1)

        # Step 4. Publish newly created deposition
        if publish:
            deposition.publish_deposition(self.access_token)
            return deposition.published_at

        return deposition.deposit_at


@attr.s
class ZenodoProvider(ProviderApi):
    """zenodo.org registry API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default="application/json")

    @staticmethod
    def supports(uri):
        """Whether or not this provider supports a given uri."""
        if "zenodo" in uri.lower():
            return True

        return False

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        return urlparse(uri).path.split("/")[-1]

    def accept_json(self):
        """Receive response as json."""
        self._accept = "application/json"
        return self

    def accept_jsonld(self):
        """Receive response as jsonld."""
        self._accept = "application/ld+json"
        return self

    def make_request(self, uri):
        """Execute network request."""
        record_id = ZenodoProvider.record_id(uri)

        with retry() as session:
            response = session.get(make_records_url(record_id), headers={"Accept": self._accept})
            if response.status_code != 200:
                raise LookupError("record not found. Status: {}".format(response.status_code))
            return response

    def find_record(self, uri, client=None):
        """Retrieves a record from Zenodo.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :return: ``ZenodoRecord``
        """
        if self.is_doi:
            return self.find_record_by_doi(uri)

        return self.get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve the DOI and make a record for the retrieved record id."""
        doi = DOIProvider().find_record(doi)
        return self.get_record(ZenodoProvider.record_id(doi.url))

    def get_record(self, uri):
        """Retrieve record metadata and return ``ZenodoRecord``."""
        response = self.make_request(uri)

        return ZenodoRecordSerializer(**response.json(), zenodo=self, uri=uri)

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return ZenodoExporter(dataset=dataset, access_token=access_token)
