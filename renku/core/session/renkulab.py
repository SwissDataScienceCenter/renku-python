# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Docker based interactive session provider."""

import urllib
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Dict, List, Optional, Tuple, Union

from renku.core import errors
from renku.core.login import read_renku_token
from renku.core.plugin import hookimpl
from renku.core.session.utils import get_renku_project_name, get_renku_url
from renku.core.util import communication, requests
from renku.core.util.git import get_remote
from renku.core.util.jwt import is_token_expired
from renku.domain_model.project_context import project_context
from renku.domain_model.session import ISessionProvider, Session


class RenkulabSessionProvider(ISessionProvider):
    """A session provider that uses the notebook service API to launch sessions."""

    DEFAULT_TIMEOUT_SECONDS = 300

    def __init__(self):
        self.__renku_url = None
        self.__notebooks_url = None

    def _renku_url(self) -> str:
        """Get the URL of the renku instance."""
        if not self.__renku_url:
            renku_url = get_renku_url()
            if not renku_url:
                raise errors.RenkulabSessionGetUrlError()
            self.__renku_url = renku_url
        return self.__renku_url

    def _notebooks_url(self) -> str:
        """Get the url of the notebooks API."""
        if not self.__notebooks_url:
            url = urllib.parse.urljoin(self._renku_url(), "api/notebooks")
            self.__notebooks_url = url
        return self.__notebooks_url

    def _get_token(self) -> str:
        """Get the JWT token used to authenticate against Renku."""
        token = read_renku_token(endpoint=self._renku_url())
        if token is None:
            raise errors.AuthenticationError("Please run the renku login command to authenticate with Renku.")
        elif is_token_expired(token):
            raise errors.AuthenticationError(
                "Authentication token is expired: Please run the renku login command to authenticate with Renku."
            )
        return token

    def _auth_header(self) -> Dict[str, str]:
        """Get the authentication header with the JWT token or cookie needed to authenticate with Renku."""
        return {"Authorization": f"Bearer {self._get_token()}"}

    @staticmethod
    def _get_renku_project_name_parts():
        repository = project_context.repository
        if project_context.remote.name and project_context.remote.owner:
            if get_remote(repository, name="renku-backup-origin") and project_context.remote.owner.startswith("repos/"):
                owner = project_context.remote.owner.replace("repos/", "", 1)
            else:
                owner = project_context.remote.owner
            return {
                "namespace": owner,
                "project": project_context.remote.name,
            }
        else:
            # INFO: In this case the owner/name split is not available. The project name is then
            # derived from the combined name of the remote and has to be split up in the two parts.
            parts = get_renku_project_name().split("/")
            return {
                "namespace": "/".join(parts[:-1]),
                "project": parts[:-1],
            }

    def _wait_for_session_status(
        self,
        name: Optional[str],
        status: str,
    ):
        if not name:
            return
        start = monotonic()
        while monotonic() - start < self.DEFAULT_TIMEOUT_SECONDS:
            res = self._send_renku_request(
                "get", f"{self._notebooks_url()}/servers/{name}", headers=self._auth_header()
            )
            if res.status_code == 404 and status == "stopping":
                return
            if res.status_code == 200 and status != "stopping":
                if res.json().get("status", {}).get("state") == status:
                    return
            sleep(5)
        raise errors.RenkulabSessionError(f"Waiting for the session {name} to reach status {status} timed out.")

    def _wait_for_image(
        self,
        image_name: str,
        config: Optional[Dict[str, Any]],
    ):
        """Check if an image exists, and if it does not wait for it to appear.

        Timeout after a specific period of time.
        """
        start = monotonic()
        while monotonic() - start < self.DEFAULT_TIMEOUT_SECONDS:
            if self.find_image(image_name, config):
                return
            sleep(5)
        raise errors.RenkulabSessionError(
            f"Waiting for the image {image_name} to be built timed out."
            "Are you sure that the image was successfully built? This could be the result "
            "of problems with your Dockerfile."
        )

    def pre_start_checks(self):
        """Check if the state of the repository is as expected before starting a session."""
        repository = project_context.repository

        if repository.is_dirty(untracked_files=True):
            communication.confirm(
                "You have new uncommitted or untracked changes to your repository. "
                "Renku can automatically commit these changes so that it builds "
                "the correct environment for your session. Do you wish to proceed?",
                abort=True,
            )
            repository.add(all=True)
            repository.commit("Automated commit by Renku CLI.")

    @staticmethod
    def _remote_head_hexsha():
        remote = get_remote(repository=project_context.repository)

        if remote is None:
            raise errors.GitRemoteNotFoundError()

        return remote.head

    def _send_renku_request(self, req_type: str, *args, **kwargs):
        res = getattr(requests, req_type)(*args, **kwargs)
        if res.status_code == 401:
            # NOTE: Check if logged in to KC but not the Renku UI
            token = read_renku_token(endpoint=self._renku_url())
            if token and not is_token_expired(token):
                raise errors.AuthenticationError(
                    f"Please log in the Renku UI at {self._renku_url()} to complete authentication with Renku"
                )
            raise errors.AuthenticationError(
                "Please run the renku login command to authenticate with Renku or to refresh your expired credentials."
            )
        return res

    def get_name(self) -> str:
        """Return session provider's name."""
        return "renkulab"

    def is_remote_provider(self) -> bool:
        """Return True for remote providers (i.e. not local Docker)."""
        return True

    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]):
        """Builds the container image."""
        if self.find_image(image_name, config=config):
            return
        repository = project_context.repository
        if repository.head.commit.hexsha != self._remote_head_hexsha():
            repository.push()
        self._wait_for_image(image_name=image_name, config=config)

    def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
        """Find the given container image."""
        return (
            self._send_renku_request(
                "get",
                f"{self._notebooks_url()}/images",
                headers=self._auth_header(),
                params={"image_url": image_name},
            ).status_code
            == 200
        )

    @hookimpl
    def session_provider(self) -> ISessionProvider:
        """Supported session provider.

        Returns:
            a reference to ``self``.
        """
        return self

    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        Returns:
            list: a list of sessions.
        """
        sessions_res = self._send_renku_request(
            "get",
            f"{self._notebooks_url()}/servers",
            headers=self._auth_header(),
            params=self._get_renku_project_name_parts(),
        )
        if sessions_res.status_code == 200:
            return [
                Session(
                    session["name"],
                    session.get("status", {}).get("state", "unknown"),
                    self.session_url(session["name"]),
                )
                for session in sessions_res.json().get("servers", {}).values()
            ]
        return []

    def session_start(
        self,
        image_name: str,
        project_name: str,
        config: Optional[Dict[str, Any]],
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Creates an interactive session.

        Returns:
            Tuple[str, str]: Provider message and a possible warning message.
        """
        repository = project_context.repository

        session_commit = repository.head.commit.hexsha
        if repository.head.commit.hexsha != self._remote_head_hexsha():
            # INFO: The user is registered, the image is pinned or already available
            # but the local repository is not fully in sync with the remote
            communication.confirm(
                "You have unpushed commits that will not be present in your session. "
                "Renku can automatically push these commits so that they are present "
                "in the session you are launching. Do you wish to proceed?",
                abort=True,
            )
            repository.push()

        server_options: Dict[str, Union[str, float]] = {}
        if cpu_request:
            server_options["cpu_request"] = cpu_request
        if mem_request:
            server_options["mem_request"] = mem_request
        if gpu_request:
            server_options["gpu_request"] = int(gpu_request)
        if disk_request:
            server_options["disk_request"] = disk_request
        payload = {
            "image": image_name,
            "commit_sha": session_commit,
            "serverOptions": server_options,
            **self._get_renku_project_name_parts(),
        }
        res = self._send_renku_request(
            "post",
            f"{self._notebooks_url()}/servers",
            headers=self._auth_header(),
            json=payload,
        )
        if res.status_code in [200, 201]:
            session_name = res.json()["name"]
            self._wait_for_session_status(session_name, "running")
            return f"Session {session_name} successfully started", ""
        raise errors.RenkulabSessionError("Cannot start session via the notebook service because " + res.text)

    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
        """Stops all sessions (for the given project) or a specific interactive session."""
        responses = []
        if stop_all:
            sessions = self.session_list(project_name=project_name, config=None)
            for session in sessions:
                responses.append(
                    self._send_renku_request(
                        "delete", f"{self._notebooks_url()}/servers/{session.id}", headers=self._auth_header()
                    )
                )
                self._wait_for_session_status(session.id, "stopping")
        else:
            responses.append(
                self._send_renku_request(
                    "delete", f"{self._notebooks_url()}/servers/{session_name}", headers=self._auth_header()
                )
            )
            self._wait_for_session_status(session_name, "stopping")
        return all([response.status_code == 204 for response in responses]) if responses else False

    def session_url(self, session_name: str) -> str:
        """Get the URL of the interactive session."""
        project_name_parts = self._get_renku_project_name_parts()
        session_url_parts = [
            "projects",
            project_name_parts["namespace"],
            project_name_parts["project"],
            "sessions/show",
            session_name,
        ]
        return urllib.parse.urljoin(self._renku_url(), "/".join(session_url_parts))
