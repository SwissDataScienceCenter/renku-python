#  Copyright Swiss Data Science Center (SDSC). A partnership between
#  École Polytechnique Fédérale de Lausanne (EPFL) and
#  Eidgenössische Technische Hochschule Zürich (ETHZ).
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

import pty
import urllib
import webbrowser
from datetime import datetime
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.config import get_value
from renku.core.constant import ProviderPriority
from renku.core.interface.storage_service_gateway import IStorageService
from renku.core.login import read_renku_token
from renku.core.plugin import hookimpl
from renku.core.session.utils import get_renku_project_name, get_renku_url
from renku.core.util import communication, requests
from renku.core.util.git import get_remote
from renku.core.util.jwt import is_token_expired
from renku.core.util.ssh import SystemSSHConfig
from renku.domain_model.project_context import project_context
from renku.domain_model.session import IHibernatingSessionProvider, Session, SessionStopStatus

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter


class RenkulabSessionProvider(IHibernatingSessionProvider):
    """A session provider that uses the notebook service API to launch sessions."""

    DEFAULT_TIMEOUT_SECONDS = 300
    # NOTE: Give the renkulab provider the lowest priority so that it's checked last
    priority: ProviderPriority = ProviderPriority.LOWEST

    def __init__(self):
        self.__renku_url: Optional[str] = None
        self.__notebooks_url: Optional[str] = None
        self._force_build: bool = False

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
            if res.status_code in [200, 204] and status != "stopping":
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

    def pre_start_checks(self, ssh: bool = False, **kwargs):
        """Check if the state of the repository is as expected before starting a session."""
        from renku.core.session.session import ssh_setup

        repository = project_context.repository

        if repository.is_dirty():
            communication.confirm(
                "You have new uncommitted or untracked changes to your repository. "
                "Renku can automatically commit these changes so that it builds "
                "the correct environment for your session. Do you wish to proceed?",
                abort=True,
            )
            repository.add(all=True)
            repository.commit("Automated commit by Renku CLI.")

        if ssh:
            system_config = SystemSSHConfig()

            if not system_config.is_configured:
                if communication.confirm(
                    "Your system is not set up for SSH connections to Renkulab. Would you like to set it up?"
                ):
                    ssh_setup()
                    self._force_build = True
                else:
                    raise errors.RenkulabSessionError(
                        "Can't run ssh session without setting up Renku SSH support. Run without '--ssh' or "
                        "run 'renku session ssh-setup'."
                    )

            if system_config.setup_session_keys():
                self._force_build = True

    def _cleanup_ssh_connection_configs(
        self, project_name: str, running_sessions: Optional[List[Session]] = None
    ) -> None:
        """Cleanup leftover SSH connections that aren't valid anymore.

        Args:
            project_name(str): Name of the project.
            running_sessions(List[Session], optional): List of running sessions to check against, otherwise will be
                gotten from the server.
        """
        if not running_sessions:
            running_sessions = self.session_list(project_name="", ssh_garbage_collection=False)

        system_config = SystemSSHConfig()

        name = self._project_name_from_full_project_name(project_name)

        session_config_paths = [system_config.session_config_path(name, s.id) for s in running_sessions]

        for path in system_config.renku_ssh_root.glob(f"00-{name}*.conf"):
            if path not in session_config_paths:
                path.unlink()

    @staticmethod
    def _remote_head_hexsha():
        remote = get_remote(repository=project_context.repository)

        if remote is None:
            raise errors.GitRemoteNotFoundError()

        return remote.head

    def _send_renku_request(self, verb: str, *args, **kwargs):
        response = getattr(requests, verb)(*args, **kwargs)
        if response.status_code == 401:
            # NOTE: Check if logged in to KC but not the Renku UI
            token = read_renku_token(endpoint=self._renku_url())
            if token and not is_token_expired(token):
                raise errors.AuthenticationError(
                    f"Please log in the Renku UI at {self._renku_url()} to complete authentication with Renku"
                )
            raise errors.AuthenticationError(
                "Please run the renku login command to authenticate with Renku or to refresh your expired credentials."
            )
        return response

    @staticmethod
    def _project_name_from_full_project_name(project_name: str) -> str:
        """Get just project name of project name if in owner/name form."""
        if "/" not in project_name:
            return project_name
        return project_name.rsplit("/", 1)[1]

    @property
    def name(self) -> str:
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

    def get_cloudstorage(self):
        """Get cloudstorage configured for the project."""
        storage_service = cast(IStorageService, inject.instance(IStorageService))
        project_id = storage_service.project_id
        if project_id is None:
            communication.warn("Couldn't get project ID from Gitlab, skipping mounting cloudstorage")
            return

        storages = storage_service.list(project_id)

        if not storages:
            return []

        storages_to_mount = []
        for storage, private_fields in storages:
            if not communication.confirm(f"Do you want to mount storage '{storage.name}'({storage.storage_type})?"):
                continue
            if storage.private:
                # check for credentials for user
                private_field_names = [f["name"] for f in private_fields]
                for name, value in storage.configuration.items():
                    if name not in private_field_names:
                        continue
                    field = next(f for f in private_fields if f["name"] == name)

                    secret = communication.prompt(f"{field['help']}\nPlease provide a value for secret '{name}'")
                    storage.configuration[name] = secret

            storages_to_mount.append({"storage_id": storage.storage_id, "configuration": storage.configuration})

        return storages_to_mount

    @hookimpl
    def session_provider(self) -> IHibernatingSessionProvider:
        """Supported session provider.

        Returns:
            a reference to ``self``.
        """
        return self

    def get_start_parameters(self) -> List["ProviderParameter"]:
        """Returns parameters that can be set for session start."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter("ssh", help="Enable ssh connections to the session.", is_flag=True),
        ]

    def get_open_parameters(self) -> List["ProviderParameter"]:
        """Returns parameters that can be set for session open."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter("ssh", help="Open a remote terminal through SSH.", is_flag=True),
        ]

    def session_list(self, project_name: str, ssh_garbage_collection: bool = True) -> List[Session]:
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
            sessions = [
                Session(
                    id=session["name"],
                    status=session.get("status", {}).get("state", "unknown"),
                    url=self.session_url(session["name"]),
                    start_time=datetime.fromisoformat(session.get("started")),
                    commit=session.get("annotations", {}).get("renku.io/commit-sha"),
                    branch=session.get("annotations", {}).get("renku.io/branch"),
                    provider="renkulab",
                    ssh_enabled=get_value("renku", "ssh_supported") == "true"
                    or project_context.project.template_metadata.ssh_supported,
                )
                for session in sessions_res.json().get("servers", {}).values()
            ]
            if ssh_garbage_collection:
                self._cleanup_ssh_connection_configs(project_name, running_sessions=sessions)
            return sessions
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
        ssh: bool = False,
        **kwargs,
    ) -> Tuple[str, str]:
        """Creates an interactive session.

        Returns:
            Tuple[str, str]: Provider message and a possible warning message.
        """
        ssh_supported = (
            get_value("renku", "ssh_supported") == "true" or project_context.project.template_metadata.ssh_supported
        )
        if ssh and not ssh_supported:
            raise errors.RenkulabSessionError(
                "Cannot start session with SSH support because this project doesn't support SSH."
            )

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
        if ssh:
            self._cleanup_ssh_connection_configs(project_name)

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
            "branch": repository.active_branch.name if repository.active_branch else "master",
            "cloudstorage": self.get_cloudstorage(),
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
            if ssh:
                name = self._project_name_from_full_project_name(project_name)
                connection = SystemSSHConfig().setup_session_config(name, session_name)
                return (
                    f"Session {session_name} successfully started, use 'renku session open --ssh {session_name}'"
                    f" or 'ssh {connection}' to connect to it",
                    "",
                )
            return (
                f"Session {session_name} successfully started, use 'renku session open {session_name}'"
                " to connect to it",
                "",
            )
        raise errors.RenkulabSessionError("Cannot start session via the notebook service because " + res.text)

    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> SessionStopStatus:
        """Stops all sessions (for the given project) or a specific interactive session."""
        responses = []
        sessions = self.session_list(project_name=project_name)
        n_sessions = len(sessions)

        if n_sessions == 0:
            return SessionStopStatus.NO_ACTIVE_SESSION

        if stop_all:
            for session in sessions:
                responses.append(
                    self._send_renku_request(
                        "delete", f"{self._notebooks_url()}/servers/{session.id}", headers=self._auth_header()
                    )
                )
                self._wait_for_session_status(session.id, "stopping")
        elif session_name:
            responses.append(
                self._send_renku_request(
                    "delete", f"{self._notebooks_url()}/servers/{session_name}", headers=self._auth_header()
                )
            )
            self._wait_for_session_status(session_name, "stopping")
        elif n_sessions == 1:
            responses.append(
                self._send_renku_request(
                    "delete", f"{self._notebooks_url()}/servers/{sessions[0].id}", headers=self._auth_header()
                )
            )
            self._wait_for_session_status(sessions[0].id, "stopping")
        else:
            return SessionStopStatus.NAME_NEEDED

        self._cleanup_ssh_connection_configs(project_name)

        n_successfully_stopped = len([r for r in responses if r.status_code == 204])

        return SessionStopStatus.SUCCESSFUL if n_successfully_stopped == n_sessions else SessionStopStatus.FAILED

    def session_open(self, project_name: str, session_name: Optional[str], ssh: bool = False, **kwargs) -> bool:
        """Open a given interactive session.

        Args:
            project_name(str): Renku project name.
            session_name(Optional[str]): The unique id of the interactive session.
            ssh(bool): Whether to open an SSH connection or a normal browser interface.
        """
        sessions = self.session_list(project_name="")
        system_config = SystemSSHConfig()
        name = self._project_name_from_full_project_name(project_name)
        ssh_prefix = f"{system_config.renku_host}-{name}-"

        if not session_name:
            if len(sessions) == 1:
                session_name = sessions[0].id
            else:
                return False

        if session_name.startswith(ssh_prefix):
            # NOTE: User passed in ssh connection name instead of session id by accident
            session_name = session_name.replace(ssh_prefix, "", 1)

        if not any(s.id == session_name for s in sessions):
            return False

        if ssh:
            ssh_setup = True
            if not system_config.is_configured:
                raise errors.RenkulabSessionError(
                    "SSH not set up for session. Run without '--ssh' or "
                    "run 'renku session ssh-setup' and start the session again."
                )
            elif not system_config.session_config_path(name, session_name).exists():
                # NOTE: Session wasn't launched from CLI
                if system_config.is_session_configured(session_name):
                    raise errors.RenkulabSessionError(
                        "Session wasn't started using 'renku session start --ssh ...' "
                        "and is not configured for SSH access by you."
                    )
                communication.info(f"Setting up SSH connection config for session {session_name}")
                system_config.setup_session_config(name, session_name)
                ssh_setup = False

            exit_code = pty.spawn(["ssh", session_name])

            if exit_code > 0 and not ssh_setup:
                # NOTE: We tried to connect to SSH even though it wasn't started from CLI
                # This failed, so we'll remove the temporary connection information.
                if system_config.session_config_path(project_name, session_name).exists():
                    system_config.session_config_path(project_name, session_name).unlink()
        else:
            url = self.session_url(session_name)

            webbrowser.open(url)
        return True

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

    def force_build_image(self, **kwargs) -> bool:
        """Whether we should force build the image directly or check for an existing image first."""
        return self._force_build

    def session_pause(self, project_name: str, session_name: Optional[str], **_) -> SessionStopStatus:
        """Pause all sessions (for the given project) or a specific interactive session."""

        def pause(session_name: str):
            result = self._send_renku_request(
                "patch",
                f"{self._notebooks_url()}/servers/{session_name}",
                headers=self._auth_header(),
                json={"state": "hibernated"},
            )

            self._wait_for_session_status(session_name, "hibernated")

            return result

        sessions = self.session_list(project_name=project_name)
        n_sessions = len(sessions)

        if n_sessions == 0:
            return SessionStopStatus.NO_ACTIVE_SESSION

        if session_name:
            response = pause(session_name)
        elif n_sessions == 1:
            response = pause(sessions[0].name)
        else:
            return SessionStopStatus.NAME_NEEDED

        return SessionStopStatus.SUCCESSFUL if response.status_code == 204 else SessionStopStatus.FAILED

    def session_resume(self, project_name: str, session_name: Optional[str], **kwargs) -> bool:
        """Resume a paused session.

        Args:
            project_name(str): Renku project name.
            session_name(Optional[str]): The unique id of the interactive session.
        """
        sessions = self.session_list(project_name="")
        system_config = SystemSSHConfig()
        name = self._project_name_from_full_project_name(project_name)
        ssh_prefix = f"{system_config.renku_host}-{name}-"

        if not session_name:
            if len(sessions) == 1:
                session_name = sessions[0].name
            else:
                return False
        else:
            if session_name.startswith(ssh_prefix):
                # NOTE: User passed in ssh connection name instead of session id by accident
                session_name = session_name.replace(ssh_prefix, "", 1)

            if not any(s.name == session_name for s in sessions):
                return False

        self._send_renku_request(
            "patch",
            f"{self._notebooks_url()}/servers/{session_name}",
            headers=self._auth_header(),
            json={"state": "running"},
        )

        self._wait_for_session_status(session_name, "running")

        return True
