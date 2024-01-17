# Copyright Swiss Data Science Center (SDSC)
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
"""Gateway for storage service."""
from dataclasses import asdict
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional

import requests

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.interface.git_api_provider import IGitAPIProvider
from renku.core.interface.storage_service_gateway import IStorageService
from renku.core.login import read_renku_token
from renku.core.session.utils import get_renku_url
from renku.domain_model.cloud_storage import CloudStorage, CloudStorageWithSensitiveFields
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context

TIMEOUT = 5


class StorageService(IStorageService):
    """Storage service gateway."""

    base_url: str
    _gl: IGitAPIProvider = inject.attr(IGitAPIProvider)

    def __init__(self):
        """Create an instance."""
        renku_url = get_renku_url()
        if not renku_url:
            raise errors.RenkulabSessionGetUrlError()
        self.base_url = f"{renku_url}api/data"

    @cached_property
    def project_id(self) -> Optional[str]:
        """Get the current gitlab project id.

        Note: This is mostly a workaround since storage service is already done to only accept
        project ids, but the CLI knows nothing about those.
        This could should be removed once we move to proper renku projects.
        """
        namespace, name = Project.get_namespace_and_name(
            remote=project_context.remote, name=project_context.project.name, repository=project_context.repository
        )

        if namespace is None or name is None:
            raise errors.ParameterError("Couldn't get namespace or name for current project")
        if namespace.startswith("repos/"):
            namespace = namespace[6:]
        gitlab_url = f"https://{project_context.remote.host}/repos/"

        return self._gl.get_project_id(gitlab_url, namespace, name)

    def _auth_headers(self) -> Dict[str, Any]:
        """Send a request with authentication headers."""
        token = read_renku_token(None, get_endpoint_from_remote=True)
        if not token:
            raise errors.NotLoggedIn("Must be logged in to get access storage for a project.")

        return {"Authorization": f"Bearer {token}"}

    def _send_request(
        self,
        path: str,
        parameters: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        method="GET",
        auth=False,
        expected_response=[200],
    ):
        """Send an unauthenticated request."""
        request_method: Callable[..., Any]
        if method == "GET":
            request_method = requests.get
        elif method == "POST":
            request_method = requests.post
        elif method == "PUT":
            request_method = requests.put
        elif method == "DELETE":
            request_method = requests.delete
        else:
            raise NotImplementedError()

        url = f"{self.base_url}{path}"
        headers = None

        if auth:
            headers = self._auth_headers()

        resp = request_method(url, headers=headers, params=parameters, data=body, timeout=TIMEOUT)  # type: ignore

        if resp.status_code not in expected_response:
            raise errors.RequestError(f"Request to storage service failed ({resp.status_code}): {resp.text}")

        return resp.json()

    def list(self, project_id: str) -> List[CloudStorageWithSensitiveFields]:
        """List storage configured for the current project."""
        response = self._send_request("/storage", parameters={"project_id": project_id}, auth=True)
        results = []
        for res in response:
            results.append(
                CloudStorageWithSensitiveFields(CloudStorage.from_dict(res["storage"]), res["sensitive_fields"])
            )

        return results

    def create(self, storage: CloudStorage) -> CloudStorageWithSensitiveFields:
        """Create a new cloud storage."""
        if storage.storage_id is not None:
            raise ValueError("Cannot create storage with 'storage_id' already set.")
        if storage.project_id is None:
            raise ValueError("'project_id' must be set when creating CloudStorage.")
        response = self._send_request(
            "/storage", body=asdict(storage), method="POST", auth=True, expected_response=[201]
        )
        return CloudStorageWithSensitiveFields(
            CloudStorage.from_dict(response["storage"]), response["sensitive_fields"]
        )

    def edit(self, storage_id: str, new_storage: CloudStorage) -> CloudStorageWithSensitiveFields:
        """Edit a cloud storage."""
        response = self._send_request(f"/storage/{storage_id}", body=asdict(new_storage), method="PUT", auth=True)
        return CloudStorageWithSensitiveFields(
            CloudStorage.from_dict(response["storage"]), response["sensitive_fields"]
        )

    def delete(self, storage_id: str) -> None:
        """Delete a cloud storage."""
        self._send_request(f"/storage{storage_id}", method="DELETE", auth=True, expected_response=[204])

    def validate(self, storage: CloudStorage) -> None:
        """Validate a cloud storage.

        Raises an exception for invalid storage.
        """
        self._send_request(
            "/storage_schema/validate", body=storage.configuration, method="POST", expected_response=[204]
        )
