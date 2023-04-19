#
# Copyright 2018-2023 - Swiss Data Science Center (SDSC)
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

import os
import platform
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, cast
from uuid import uuid4

import docker
from requests.exceptions import ReadTimeout

from renku.core import errors
from renku.core.config import get_value
from renku.core.constant import ProviderPriority
from renku.core.plugin import hookimpl
from renku.core.util import communication
from renku.domain_model.project_context import project_context
from renku.domain_model.session import ISessionProvider, Session

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter


class DockerSessionProvider(ISessionProvider):
    """A docker based interactive session provider."""

    JUPYTER_PORT = 8888
    # NOTE: Give the docker provider a higher priority so that it's checked first
    priority: ProviderPriority = ProviderPriority.HIGHEST

    def __init__(self):
        self._docker_client = None

    def docker_client(self) -> docker.client.DockerClient:
        """Get the docker client.

        Note:
            This is not a @property, even though it should be, because ``pluggy``
            will call it in that case in unrelated parts of the code that will
        Raises:
            errors.DockerError: Exception when docker is not available.
        Returns:
            The docker client.
        """
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
            except docker.errors.DockerException as e:
                raise errors.DockerError(f"Error while initializing docker client: {str(e)}")
        return self._docker_client

    @staticmethod
    def _get_jupyter_urls(ports: Dict[str, Any], auth_token: str, jupyter_port: int = 8888) -> Iterable[str]:
        port_key = f"{jupyter_port}/tcp"
        if port_key not in ports:
            return list()
        return map(lambda x: f"http://{x['HostIp']}:{x['HostPort']}/?token={auth_token}", ports[port_key])

    def _get_docker_containers(self, project_name: str) -> List[docker.models.containers.Container]:
        return self.docker_client().containers.list(filters={"label": f"renku_project={project_name}"})

    @property
    def name(self) -> str:
        """Return session provider's name."""
        return "docker"

    def is_remote_provider(self) -> bool:
        """Return True for remote providers (i.e. not local Docker)."""
        return False

    def build_image(self, image_descriptor: Path, image_name: str, config: Optional[Dict[str, Any]]):
        """Builds the container image."""
        self.docker_client().images.build(path=str(image_descriptor), tag=image_name)

    def find_image(self, image_name: str, config: Optional[Dict[str, Any]]) -> bool:
        """Find the given container image."""
        with communication.busy(msg=f"Checking for image {image_name}"):
            try:
                self.docker_client().images.get(image_name)
            except docker.errors.ImageNotFound:
                try:
                    self.docker_client().images.get_registry_data(image_name)
                except docker.errors.APIError:
                    return False
            else:
                return True

        try:
            with communication.busy(msg=f"Pulling image from remote {image_name}"):
                self.docker_client().images.pull(image_name)
        except docker.errors.NotFound:
            return False
        else:
            return True

    @hookimpl
    def session_provider(self) -> ISessionProvider:
        """Supported session provider.

        Returns:
            A reference to ``self``.
        """
        return self

    def get_start_parameters(self) -> List["ProviderParameter"]:
        """Returns parameters that can be set for session start."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter("port", help="Local port to use (random if not specified).", type=int),
            ProviderParameter("force-build", help="Always build image and don't check if it exists.", is_flag=True),
        ]

    def get_open_parameters(self) -> List["ProviderParameter"]:
        """Returns parameters that can be set for session open."""
        return []

    def session_list(self, project_name: str, config: Optional[Dict[str, Any]]) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        Returns:
            list: a list of sessions.
        """

        return list(
            map(
                lambda x: Session(
                    id=cast(str, x.short_id),
                    status=x.status,
                    start_time=datetime.fromisoformat(x.labels["start_time"]),
                    provider="docker",
                    commit=x.labels["commit"],
                    branch=x.labels["branch"],
                    url=next(iter(DockerSessionProvider()._get_jupyter_urls(x.ports, x.labels["jupyter_token"]))),
                ),
                self._get_docker_containers(project_name),
            )
        )

    def session_start(
        self,
        image_name: str,
        project_name: str,
        config: Optional[Dict[str, Any]],
        cpu_request: Optional[float] = None,
        mem_request: Optional[str] = None,
        disk_request: Optional[str] = None,
        gpu_request: Optional[str] = None,
        **kwargs,
    ) -> Tuple[str, str]:
        """Creates an interactive session.

        Returns:
            Tuple[str, str]: Provider message and a possible warning message.
        """
        show_non_standard_user_warning = True

        def session_start_helper(consider_disk_request: bool):
            try:
                docker_is_running = self.docker_client().ping()
                if not docker_is_running:
                    raise errors.DockerError(
                        "Could not communicate with the docker instance. Please make sure it is running!"
                    )

                auth_token = uuid4().hex
                default_url = get_value("interactive", "default_url")

                # resource requests
                resource_requests: Dict[str, Any] = dict()
                if cpu_request:
                    # based on the docker go cli: func ParseCPUs
                    resource_requests["nano_cpus"] = int(cpu_request * 10**9)

                if mem_request:
                    resource_requests["mem_limit"] = mem_request

                if consider_disk_request and disk_request:
                    resource_requests["storage_opt"] = {"size": disk_request}

                if gpu_request:
                    if gpu_request == "all":
                        resource_requests["device_requests"] = [
                            docker.types.DeviceRequest(count=-1, capabilities=[["compute", "utility"]])
                        ]
                    else:
                        resource_requests["device_requests"] = [
                            docker.types.DeviceRequest(count=[gpu_request], capabilities=[["compute", "utility"]])
                        ]

                # NOTE: set git user
                image_data = self.docker_client().api.inspect_image(image_name)
                working_dir = image_data.get("Config", {}).get("WorkingDir", None)

                if working_dir is None:
                    working_dir = "/home/jovyan"

                work_dir = Path(working_dir) / "work" / project_name.split("/")[-1]

                volumes = [f"{str(project_context.path.resolve())}:{work_dir}"]

                user = project_context.repository.get_user()
                environment = {
                    "GIT_AUTHOR_NAME": user.name,
                    "GIT_AUTHOR_EMAIL": user.email,
                    "GIT_COMMITTER_EMAIL": user.email,
                    "EMAIL": user.email,
                }

                additional_options: Dict[str, Any] = {}

                if platform.system() == "Linux" and os.getuid() != 1000:
                    # NOTE: Current user id is not 1000 like jovyan, need to run docker under that user.
                    nonlocal show_non_standard_user_warning
                    if show_non_standard_user_warning:
                        communication.confirm(
                            "Your user id is not 1000 and for Jupyter to work the session must be started as root.\n"
                            "Jupyter itself will run as your user.\n"
                            "Starting as root has security implications, make sure you trust this Dockerfile.\n"
                            "Proceed?",
                            abort=True,
                        )
                        show_non_standard_user_warning = False

                    additional_options["user"] = "root"
                    environment["NB_UID"] = str(os.getuid())
                    environment["CHOWN_HOME"] = "yes"
                    environment["CHOWN_HOME_OPTS"] = "-R"

                container = self.docker_client().containers.run(
                    image_name,
                    'jupyter notebook --NotebookApp.ip="0.0.0.0"'
                    f" --NotebookApp.port={DockerSessionProvider.JUPYTER_PORT}"
                    f' --NotebookApp.token="{auth_token}" --NotebookApp.default_url="{default_url}"'
                    f" --NotebookApp.notebook_dir={work_dir}" + (" --allow-root" if os.getuid() != 1000 else ""),
                    detach=True,
                    labels={
                        "renku_project": project_name,
                        "jupyter_token": auth_token,
                        "commit": project_context.repository.head.commit.hexsha,
                        "branch": project_context.repository.active_branch.name
                        if project_context.repository.active_branch
                        else "master",
                        "start_time": datetime.now().isoformat(),
                    },
                    ports={f"{DockerSessionProvider.JUPYTER_PORT}/tcp": kwargs.get("port")},
                    remove=True,
                    environment=environment,
                    volumes=volumes,
                    working_dir=str(work_dir),
                    **resource_requests,
                    **additional_options,
                )

                if not container.ports:
                    container.reload()

                jupyter_urls = DockerSessionProvider._get_jupyter_urls(
                    container.ports, auth_token, jupyter_port=DockerSessionProvider.JUPYTER_PORT
                )

                message = f"The session for '{image_name}' has been successfully started. It is available at:\n\t"
                message += "\n\t".join(jupyter_urls)
                return message
            except docker.errors.BuildError as error:
                raise errors.DockerError("Couldn't build the image. See inner exception for details.") from error
            except docker.errors.APIError as error:
                raise errors.DockerAPIError("Docker API returned an error. See inner exception for details.") from error
            except ReadTimeout as error:
                raise errors.DockerError(
                    "Couldn't reach the Docker API. Is the docker service running and up to date?"
                ) from error

        try:
            result = session_start_helper(consider_disk_request=True)
        except errors.DockerAPIError:
            warning_message = "Cannot start a session with the disk request: Ignoring the disk request"
            return session_start_helper(consider_disk_request=False), warning_message
        else:
            return result, ""

    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> bool:
        """Stops all or a given interactive session."""
        try:
            docker_containers = (
                self._get_docker_containers(project_name)
                if stop_all
                else self.docker_client().containers.list(filters={"id": session_name})
            )

            if len(docker_containers) == 0:
                return False

            [c.stop() for c in docker_containers]
            return True
        except docker.errors.APIError as error:
            raise errors.DockerError(error.msg)

    def session_open(self, project_name: str, session_name: str, **kwargs) -> bool:
        """Open a given interactive session.

        Args:
            project_name(str): Renku project name.
            session_name(str): The unique id of the interactive session.
        """
        url = self.session_url(session_name)

        if not url:
            return False

        webbrowser.open(url)
        return True

    def session_url(self, session_name: str) -> Optional[str]:
        """Get the URL of the interactive session."""
        for c in self.docker_client().containers.list():
            if c.short_id == session_name and f"{DockerSessionProvider.JUPYTER_PORT}/tcp" in c.ports:
                host = c.ports[f"{DockerSessionProvider.JUPYTER_PORT}/tcp"][0]
                return f'http://{host["HostIp"]}:{host["HostPort"]}/?token={c.labels["jupyter_token"]}'
        return None

    def force_build_image(self, force_build: bool = False, **kwargs) -> bool:
        """Whether we should force build the image directly or check for an existing image first."""
        return force_build
