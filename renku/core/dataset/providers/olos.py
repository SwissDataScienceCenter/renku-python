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
"""OLOS API integration."""

import datetime
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional
from urllib import parse as urlparse
from uuid import UUID, uuid4

from renku.core import errors
from renku.core.config import get_value, set_value
from renku.core.dataset.providers.api import ExporterApi, ExportProviderInterface, ProviderApi, ProviderPriority
from renku.core.util import communication
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter
    from renku.domain_model.dataset import Dataset, DatasetTag


class OLOSProvider(ProviderApi, ExportProviderInterface):
    """Provider for OLOS integration."""

    priority = ProviderPriority.HIGH
    name = "OLOS"

    def __init__(self, uri: Optional[str], is_doi: bool = False):
        super().__init__(uri=uri)

        self.is_doi = is_doi
        self._server_url = None

    @staticmethod
    def supports(uri):
        """Check if provider supports a given URI for importing."""
        return False

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [ProviderParameter("dlcm-server", help="DLCM server base url.", type=str)]

    def get_exporter(
        self, dataset: "Dataset", *, tag: Optional["DatasetTag"], dlcm_server: str = None, **kwargs
    ) -> "OLOSExporter":
        """Create export manager for given dataset."""

        def set_export_parameters():
            """Set and validate required parameters for exporting for a provider."""
            server = dlcm_server
            config_base_url = "server_url"
            if not server:
                server = get_value("olos", config_base_url)
            else:
                set_value("olos", config_base_url, server, global_only=True)

            if not server:
                raise errors.ParameterError("OLOS server URL is required.")

            self._server_url = server  # type: ignore

        set_export_parameters()
        return OLOSExporter(dataset=dataset, server_url=self._server_url)


class OLOSExporter(ExporterApi):
    """OLOS export manager."""

    def __init__(self, *, dataset, server_url=None):
        super().__init__(dataset)
        self._access_token = None
        self._server_url = server_url

    def set_access_token(self, access_token):
        """Set access token."""
        self._access_token = access_token

    def get_access_token_url(self):
        """Endpoint for creation of access token."""
        return urllib.parse.urljoin(self._server_url, "portal")

    def export(self, **kwargs):
        """Execute export process."""
        from renku.domain_model.dataset import get_file_path_in_dataset

        deposition = _OLOSDeposition(server_url=self._server_url, access_token=self._access_token)
        repository = project_context.repository

        metadata = self._get_dataset_metadata()
        metadata["organizationalUnitId"] = deposition.get_org_unit()
        deposition.create_dataset(metadata=metadata)

        with communication.progress("Uploading files ...", total=len(self.dataset.files)) as progressbar:
            for file in self.dataset.files:
                filepath = repository.copy_content_to_file(path=file.entity.path, checksum=file.entity.checksum)
                path_in_dataset = get_file_path_in_dataset(dataset=self.dataset, dataset_file=file)
                deposition.upload_file(full_path=filepath, path_in_dataset=path_in_dataset)
                progressbar.update()

        return deposition.deposited_at

    def _get_dataset_metadata(self):
        try:
            identifier = str(UUID(self.dataset.identifier, version=4))
        except ValueError:
            identifier = uuid4().hex

        metadata = {
            "publicationDate": datetime.date.today().isoformat(),
            "description": self.dataset.description,
            "identifier": identifier,
            "keywords": self.dataset.keywords,
            "title": self.dataset.title,
            "access": "CLOSED",
            "dataSensitivity": "CRIMSON",
            "year": datetime.datetime.today().year,
        }
        return metadata


class _OLOSDeposition:
    """OLOS record for deposit."""

    def __init__(
        self,
        *,
        access_token,
        server_url,
        dataset_pid=None,
        deposited_at=None,
        deposition_base_url=None,
        admin_base_url=None,
    ):
        self.access_token = access_token
        self.server_url = server_url
        self.dataset_pid = dataset_pid
        self.deposited_at = deposited_at
        self.deposition_base_url = deposition_base_url
        self.admin_base_url = admin_base_url

        self._get_base_urls()

    ORGANIZATIONAL_UNIT_PATH = "/authorized-organizational-units"
    DATASET_CREATE_PATH = "/deposits"
    FILE_UPLOAD_PATH = "/deposits/{deposit_id}/upload"
    MODULES_PATH = "administration/preservation-planning/modules"

    def _get_base_urls(self):
        """Get base urls for different endpoints."""
        url = self._make_url(self.server_url, api_path=self.MODULES_PATH)
        response = self._get(url=url)
        self._check_response(response)

        response_data = response.json()
        self.deposition_base_url = response_data["preingest"]
        self.admin_base_url = response_data["admin"]

    def get_org_unit(self):
        """Get the org units of the user."""
        url = self.admin_base_url + self.ORGANIZATIONAL_UNIT_PATH

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
        url = self.deposition_base_url + self.DATASET_CREATE_PATH

        response = self._post(url=url, json=metadata)
        self._check_response(response)

        response_data = response.json()

        self.dataset_pid = response_data["resId"]
        self.deposited_at = response_data["_links"]["self"]["href"]

        return response

    def upload_file(self, full_path, path_in_dataset: Path):
        """Upload a file to a previously-created dataset."""
        if self.dataset_pid is None:
            raise errors.ExportError("Dataset not created.")

        url = self.deposition_base_url + self.FILE_UPLOAD_PATH.format(deposit_id=self.dataset_pid)

        location = str(path_in_dataset.parent)

        if location == ".":
            location = ""

        params = {"folder": location, "category": "Primary", "type": "Derived"}

        files = {"file": (path_in_dataset.name, open(full_path, "rb"))}

        response = self._post(url=url, data=params, files=files)
        self._check_response(response)

        return response

    @staticmethod
    def _make_url(server_url, api_path, **query_params):
        """Create URL for creating a dataset."""
        url_parts = urlparse.urlparse(server_url)

        query_params_encoded = urllib.parse.urlencode(query_params)
        url_parts = url_parts._replace(path=api_path, query=query_params_encoded)
        return urllib.parse.urlunparse(url_parts)

    def _get(self, url):
        from renku.core.util import requests

        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            return requests.get(url=url, headers=headers)
        except errors.RequestError as e:
            raise errors.ExportError("Cannot GET from remote server.") from e

    def _post(self, url, json=None, data=None, files=None):
        from renku.core.util import requests

        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            return requests.post(url=url, json=json, data=data, files=files, headers=headers)
        except errors.RequestError as e:
            raise errors.ExportError("Cannot POST to remote server.") from e

    @staticmethod
    def _check_response(response):
        from renku.core.util import requests

        if len(response.history) > 0:
            raise errors.ExportError(
                f"Couldn't execute request to {response.request.url}, got redirected to {response.url}."
                "Maybe you mixed up http and https in the server url?"
            )

        try:
            requests.check_response(response=response)
        except errors.RequestError:
            json_res = response.json()
            raise errors.ExportError(
                "HTTP {} - Cannot export dataset: {}".format(
                    response.status_code, json_res["message"] if "message" in json_res else json_res["status"]
                )
            )
