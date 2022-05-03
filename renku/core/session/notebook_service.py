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
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from yaspin import yaspin

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.management.client import LocalClient
from renku.core.plugin import hookimpl
from renku.core.session.utils import get_renku_project_name
from renku.core.util import communication, requests
from renku.core.util.git import get_remote
from renku.core.util.urls import parse_authentication_endpoint
from renku.domain_model.session import ISessionProvider, Session


@inject.autoparams("client_dispatcher")
def _get_token(client_dispatcher: IClientDispatcher, renku_url: str) -> Tuple[str, bool]:
    """Get a token for authenticating with renku.

    If the user is logged in then the JWT token from renku login will be used.
    Otherwise the anonymous user token will be used. Returns the token and a flag to
    indicate if the user is registered (true) or anonymous(false).
    """
    client = client_dispatcher.current_client
    registered_token = client.get_value(section="http", key=urllib.parse.urlparse(renku_url).netloc)
    if not registered_token:
        return _get_anonymous_credentials(renku_url=renku_url), False
    return registered_token, True


@inject.autoparams("client_dispatcher")
def _get_anonymous_credentials(client_dispatcher: IClientDispatcher, renku_url: str) -> str:
    def _get_anonymous_token() -> Optional[str]:
        import requests

        with requests.Session() as session:
            url = urllib.parse.urljoin(renku_url, "api/user")
            try:
                session.get(
                    url,
                    headers={
                        "X-Requested-With": "XMLHttpRequest",
                        "X-Forwarded-Uri": "/api/user",
                    },
                )
            except (requests.exceptions.RequestException, requests.exceptions.ConnectionError):
                pass
            return session.cookies.get("anon-id")

    client = client_dispatcher.current_client
    renku_host = urllib.parse.urlparse(renku_url).netloc
    anon_token = client.get_value(section="anonymous_tokens", key=renku_host)
    if not anon_token:
        anon_token = _get_anonymous_token()
        if not anon_token:
            raise errors.AuthenticationError(
                "Could not get anonymous user token from Renku. "
                f"Ensure the Renku deployment at {renku_url} supports anonymous sessions."
            )
        client.set_value(section="anonymous_tokens", key=renku_host, value=anon_token, global_only=True)
    return anon_token


@inject.autoparams()
def _get_client(client_dispatcher: IClientDispatcher) -> LocalClient:
    """Get the local client."""
    client = client_dispatcher.current_client
    return client


def _split_project_name(project_name: str) -> Dict[str, str]:
    """Split a full project name into the namespace and project components required by the notebooks API."""
    parts = project_name.split("/")
    return {
        "namespace": "/".join(parts[:-1]),
        "project": parts[-1],
    }


class NotebookServiceSessionProvider(ISessionProvider):
    """A session provider that uses the notebook service API to launch sessions."""

    DEFAULT_TIMEOUT_SECONDS = 300

    def __init__(self):
        self.__renku_url = None
        self.__notebooks_url = None

    def _renku_url(self) -> str:
        """Get the URL of the renku instance."""
        if not self.__renku_url:
            renku_url = parse_authentication_endpoint(use_remote=True)
            if not renku_url:
                raise errors.UsageError(
                    "Cannot determine the renku URL to launch a session. "
                    "Ensure your current project is a valid Renku project."
                )
            self.__renku_url = urllib.parse.urlunparse(renku_url)
        return self.__renku_url

    def _notebooks_url(self) -> str:
        """Get the url of the notebooks API."""
        if not self.__notebooks_url:
            url = urllib.parse.urljoin(self._renku_url(), "api/notebooks")
            self.__notebooks_url = url
        return self.__notebooks_url

    def _token(self) -> str:
        """Get the JWT token used to authenticate against Renku."""
        token, _ = _get_token(renku_url=self._renku_url())
        if token is None:
            raise errors.AuthenticationError("Please run the renku login command to authenticate with Renku.")
        return token

    def _is_user_registered(self) -> bool:
        _, is_user_registered = _get_token(renku_url=self._renku_url())
        return is_user_registered

    def _auth_header(self) -> Dict[str, str]:
        """Get the authentication header with the JWT token or cookie needed to authenticate with Renku."""
        if self._is_user_registered():
            return {"Authorization": f"Bearer {self._token()}"}
        return {"Cookie": f"anon-id={self._token()}"}

    def _wait_for_session_status(
        self,
        name: str,
        status: str,
    ):
        now = datetime.now()
        timeout_time = now + timedelta(seconds=self.DEFAULT_TIMEOUT_SECONDS)
        while now < timeout_time:
            res = self._send_renku_request(
                "get", f"{self._notebooks_url()}/servers/{name}", headers=self._auth_header()
            )
            if res.status_code == 404 and status == "stopping":
                return
            if res.status_code == 200 and status != "stopping":
                if res.json().get("status", {}).get("state") == status:
                    return
            sleep(5)
        raise errors.NotebookServiceSessionError(f"Waiting for the session {name} to reach status {status} timed out.")

    def _get_in_progress_gitlab_job_id(self, gitlab_project_name: str, commit_sha: str) -> Optional[str]:
        """Get the ID of any job that is in progress and matches the specified arguments."""
        project_name_enc = gitlab_project_name.replace("/", "%2F")
        res = self._send_renku_request(
            "get",
            f"{self.renku_url()}/api/projects/{project_name_enc}/jobs",
            params={"scope": ["running", "pending", "created"]},
            headers=self._auth_header(),
        )
        if res.status_code == 200:
            for job in res.json():
                if job.get("commit", {}).get("id") == commit_sha:
                    return job["id"]

    def _wait_for_ci_jobs_completion(
        self,
        gitlab_project_name: str,
        commit_sha: str,
    ):
        """Wait for all in-progress CI jobs related to the arguments to finish."""
        if not self._is_user_registered():
            return
        now = datetime.now()
        timeout_time = now + timedelta(seconds=self.DEFAULT_TIMEOUT_SECONDS)
        while now < timeout_time:
            if self._get_in_progress_gitlab_job_id(gitlab_project_name, commit_sha) is None:
                return
            sleep(5)
        raise errors.NotebookServiceSessionError(
            f"Waiting for the CI jobs for project {gitlab_project_name} to finish timed out."
        )

    def _commit_and_push_checks(self):
        client = _get_client()
        remote = get_remote(client.repository)
        if self._is_user_registered():
            if client.repository.is_dirty() or len(client.repository.untracked_files) > 0:
                communication.confirm(
                    "You have new uncommitted or untracked changes to your repository. "
                    "Renku can automatically commit and push these changes so that it builds "
                    "the correct image for your session. Do you wish to proceed?",
                    abort=True,
                )
                client.repository.add(all=True)
                client.repository.commit("Automated commit by Renku CLI.")
                client.repository.push()
            if client.repository.head.commit.hexsha != remote.head_commit.hexsha:
                communication.confirm(
                    "You have new changes that are present only in your local repository. "
                    "Renku can automatically push these changes so that it builds "
                    "the correct image for your session. Do you wish to proceed?",
                    abort=True,
                )
                client.repository.push()

    @staticmethod
    def _send_renku_request(req_type: str, *args, **kwargs):
        res = getattr(requests, req_type)(*args, **kwargs)
        if res.status_code == 401:
            raise errors.AuthenticationError(
                "Please run the renku login command to authenticate with Renku or to refresh your expired credentials."
            )
        return res

    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]):
        """Builds the container image."""
        if self.find_image(image_name):
            return
        if not self._is_user_registered():
            raise errors.NotebookSessionImageNotExistError(
                f"Renku cannot find the image {image_name} and use it in an anonymous session."
            )
        self._commit_and_push_checks()
        project_name = get_renku_project_name()
        client = _get_client()
        if self._get_in_progress_gitlab_job_id(project_name, client.repository.head.commit.hexsha) is not None:
            with yaspin(text="Waiting for image to be built..."):
                self._wait_for_ci_jobs_completion(project_name, client.repository.head.commit.hexsha)

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
    def session_provider(self) -> Tuple[ISessionProvider, str]:
        """Supported session provider.

        Returns:
            a tuple of ``self`` and provider name.
        """
        return (self, "notebook_service")

    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        Returns:
            list: a list of sessions.
        """
        sessions_res = self._send_renku_request(
            "get",
            f"{self._notebooks_url()}/servers",
            headers=self._auth_header(),
            params=_split_project_name(project_name),
        )
        if sessions_res.status_code == 200:
            return [
                Session(
                    session["name"],
                    session.get("status", {}).get("state", "unknown"),
                    session["url"],
                )
                for session in sessions_res.json().get("servers", {}).values()
            ]
        return []

    def session_start(
        self,
        image_name: str,
        project_name: str,
        config: Optional[Dict[str, Any]],
        client: LocalClient,
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
    ) -> str:
        """Creates an interactive session.

        Returns:
            str: a unique id for the created interactive sesssion.
        """
        session_commit = client.repository.head.commit.hexsha
        if not self._is_user_registered():
            communication.warn(
                "You are starting a session as an anonymous user. "
                "None of the local changes in this project will be reflected in your session. "
                "In addition, any changes you make in the new session will be lost when "
                "the session is shut down."
            )
        self._commit_and_push_checks()
        project_name_parts = _split_project_name(project_name)
        server_options = {}
        if cpu_request:
            server_options["cpu_request"] = cpu_request
        if mem_request:
            server_options["mem_request"] = mem_request
        if gpu_request:
            server_options["gpu_request"] = gpu_request
        if disk_request:
            server_options["disk_request"] = disk_request
        payload = {
            "image": image_name,
            "project": project_name_parts["project"],
            "namespace": project_name_parts["namespace"],
            "commit_sha": session_commit,
            "serverOptions": server_options,
        }
        res = self._send_renku_request(
            "post",
            f"{self._notebooks_url()}/servers",
            headers=self._auth_header(),
            json=payload,
        )
        if res.status_code in [200, 201]:
            session_name = res.json()["name"]
            with yaspin(text="Waiting for session to start..."):
                self._wait_for_session_status(session_name, "running")
            return session_name
        raise errors.NotebookServiceSessionError("Cannot start session via the notebook service because " + res.text)

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
                with yaspin(text="Waiting for session to stop..."):
                    self._wait_for_session_status(session.id, "stopping")
        else:
            responses.append(
                self._send_renku_request(
                    "delete", f"{self._notebooks_url()}/servers/{session_name}", headers=self._auth_header()
                )
            )
            with yaspin(text="Waiting for session to stop..."):
                self._wait_for_session_status(session_name, "stopping")
        return all([response.status_code == 204 for response in responses])

    def session_url(self, session_name: str) -> Optional[str]:
        """Get the URL of the interactive session."""
        res = self._send_renku_request(
            "get", f"{self._notebooks_url()}/servers/{session_name}", headers=self._auth_header()
        )
        if res.status_code == 200:
            if res.json().get("status", {}).get("state") != "running":
                raise errors.NotebookSessionNotReadyError(
                    f"The session {session_name} cannot be accessed now because it is not ready."
                )
            return res.json().get("url")
