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
"""OLOS API integration."""

import datetime
import os
import urllib
from pathlib import Path
from urllib import parse as urlparse
from uuid import UUID, uuid4

import attr
import requests
from tqdm import tqdm

from renku.core import errors
from renku.core.commands.providers.api import ExporterApi, ProviderApi
from renku.core.utils import communication
from renku.core.utils.requests import retry

OLOS_SANDBOX_URL = "https://sandbox.dlcm.ch/"


@attr.s
class OLOSProvider(ProviderApi):
    """Provider for OLOS integration."""

    _server_url = attr.ib(default=None)

    @staticmethod
    def supports(uri):
        """Check if provider supports a given uri for importing."""
        return False

    @staticmethod
    def supports_export():
        """Whether this provider supports dataset export."""
        return True

    @staticmethod
    def export_parameters():
        """Returns parameters that can be set for export."""
        return {
            "olos-server": ("OLOS server base url.", str),
        }

    def find_record(self, uri, client=None):
        """Find record by uri."""
        return None

    def get_exporter(self, dataset, access_token):
        """Create export manager for given dataset."""
        return OLOSExporter(dataset=dataset, access_token=access_token, server_url=self._server_url)

    def set_parameters(self, client, *, olos_server=None, **kwargs):
        """Set and validate required parameters for a provider."""
        CONFIG_BASE_URL = "server_url"

        if "OLOS_USE_SANDBOX" in os.environ:
            self._server_url = OLOS_SANDBOX_URL
            return

        if not olos_server:
            olos_server = client.get_value("olos", CONFIG_BASE_URL)
        else:
            client.set_value("olos", CONFIG_BASE_URL, olos_server, global_only=True)

        if not olos_server:
            raise errors.ParameterError("OLOS server URL is required.")

        self._server_url = olos_server


@attr.s
class OLOSExporter(ExporterApi):
    """OLOS export manager."""

    dataset = attr.ib(kw_only=True)

    access_token = attr.ib(kw_only=True)

    _server_url = attr.ib(kw_only=True, default=None)

    def set_access_token(self, access_token):
        """Set access token."""
        self.access_token = access_token

    def access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urljoin(self._server_url, "/portal by clicking on the top-right menu and selecting 'token'")

    def export(self, publish, **kwargs):
        """Execute export process."""
        deposition = _OLOSDeposition(server_url=self._server_url, access_token=self.access_token)

        metadata = self._get_dataset_metadata()
        metadata["organizationalUnitId"] = deposition.get_org_unit()
        deposition.create_dataset(metadata=metadata)

        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file_ in self.dataset.files:
                try:
                    path = Path(file_.path).relative_to(self.dataset.data_dir)
                except ValueError:
                    path = Path(file_.path)
                deposition.upload_file(full_path=file_.full_path, path_in_dataset=path)
                progressbar.update(1)

        return deposition.deposited_at

    def _get_dataset_metadata(self):
        try:
            identifier = UUID(self.dataset.identifier, version=4)
        except ValueError:
            identifier = uuid4()
        metadata = {
            "publicationDate": datetime.date.today().isoformat(),
            "description": self.dataset.description,
            "identifier": str(identifier),
            "keywords": self.dataset.keywords,
            "title": self.dataset.title,
            "access": "CLOSED",
            "dataSensitivity": "CRIMSON",
            "year": datetime.datetime.today().year,
        }
        return metadata


@attr.s
class _OLOSDeposition:
    """OLOS record for deposit."""

    access_token = attr.ib(kw_only=True)
    server_url = attr.ib(kw_only=True)
    dataset_pid = attr.ib(kw_only=True, default=None)
    deposited_at = attr.ib(kw_only=True, default=None)

    ORGANIZATIONAL_UNIT_PATH = "administration/admin/authorized-organizational-units"
    DATASET_CREATE_PATH = "ingestion/preingest/deposits"
    FILE_UPLOAD_PATH = "ingestion/preingest/deposits/{deposit_id}/upload"

    def get_org_unit(self):
        """Get the org units of the user."""
        url = self._make_url(api_path=self.ORGANIZATIONAL_UNIT_PATH)

        response = self._get(url=url)
        self._check_response(response)

        response_data = response.json()
        options = [(o["resId"], o["name"]) for o in response_data["_data"]]

        if not options:
            raise errors.ExportError("Couldn't export dataset, you aren't in any Organizational Unit.")

        if len(options) == 1:
            return options[0][0]
        ids, names = zip(*options)
        msg = "Organizational Unit to export to:\n"
        msg += "\n".join(f"{n} [{i}]" for i, n in enumerate(names, start=1))
        msg += "\n\n Organizational Unit"
        selection = communication.prompt(msg, type=int, default=1)

        return ids[selection - 1]

    def create_dataset(self, metadata):
        """Create a dataset in OLOS."""
        url = self._make_url(api_path=self.DATASET_CREATE_PATH)

        response = self._post(url=url, json=metadata)
        self._check_response(response)

        response_data = response.json()

        self.dataset_pid = response_data["resId"]
        self.deposited_at = response_data["_links"]["self"]["href"]

        return response

    def upload_file(self, full_path, path_in_dataset):
        """Upload a file to a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError("Dataset not created.")

        url = self._make_url(self.FILE_UPLOAD_PATH.format(deposit_id=self.dataset_pid))

        location = str(path_in_dataset.parent)

        if location == ".":
            location = ""

        params = {"folder": location, "category": "Primary", "type": "Derived"}

        files = {"file": (path_in_dataset.name, open(full_path, "rb"))}

        response = self._post(url=url, data=params, files=files)
        self._check_response(response)

        return response

    def _make_url(self, api_path, **query_params):
        """Create URL for creating a dataset."""
        url_parts = urlparse.urlparse(self.server_url)

        query_params = urllib.parse.urlencode(query_params)
        url_parts = url_parts._replace(path=api_path, query=query_params)
        return urllib.parse.urlunparse(url_parts)

    def _get(self, url, json=None, data=None, files=None):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            with retry() as session:
                return session.get(url=url, json=json, data=data, files=files, headers=headers)
        except requests.exceptions.RequestException as e:
            raise errors.ExportError("Cannot GET to remote server.") from e

    def _post(self, url, json=None, data=None, files=None):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            with retry() as session:
                return session.post(url=url, json=json, data=data, files=files, headers=headers)
        except requests.exceptions.RequestException as e:
            raise errors.ExportError("Cannot POST to remote server.") from e

    @staticmethod
    def _check_response(response):
        if len(response.history) > 0:
            raise errors.ExportError(
                f"Couldn't execute request to {response.request.url}, got redirected to {response.url}."
                "Maybe you mixed up http and https in the server url?"
            )

        if response.status_code not in [200, 201, 202]:
            if response.status_code == 401:
                raise errors.AuthenticationError("Access unauthorized - update access token.")
            json_res = response.json()
            raise errors.ExportError(
                "HTTP {} - Cannot export dataset: {}".format(
                    response.status_code, json_res["message"] if "message" in json_res else json_res["status"]
                )
            )
