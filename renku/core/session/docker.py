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

import os
import platform
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union, cast
from uuid import uuid4

import docker
from requests.exceptions import ReadTimeout

from renku.core import errors
from renku.core.config import get_value
from renku.core.constant import ProviderPriority
from renku.core.login import read_renku_token
from renku.core.plugin import hookimpl
from renku.core.session.utils import get_renku_url
from renku.core.util import communication
from renku.core.util.jwt import is_token_expired
from renku.domain_model.project_context import project_context
from renku.domain_model.session import ISessionProvider, Session, SessionStopStatus

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter


class DockerSessionProvider(ISessionProvider):
    """A docker based interactive session provider."""

    JUPYTER_PORT = 8888
    # NOTE: Give the docker provider the highest priority so that it's checked first
    priority: ProviderPriority = ProviderPriority.HIGHEST

    def __init__(self):
        self._docker_client = None

    def docker_client(self) -> docker.client.DockerClient:
        """Get the docker client.

        Note:
            This is not a @property, even though it should be, because ``pluggy``
            will call it in that case in unrelated parts of the code.
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
    def _get_jupyter_urls(ports: Dict[str, Any], auth_token: str, jupyter_port: int = 8888) -> Iterator[str]:
        port_key = f"{jupyter_port}/tcp"
        if port_key not in ports:
            return list()  # type: ignore
        default_url = get_value("interactive", "default_url")
        if not default_url:
            default_url = "/lab"
        return map(lambda x: f"http://{x['HostIp']}:{x['HostPort']}{default_url}?token={auth_token}", ports[port_key])

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
            renku_url = get_renku_url()

            # only search remote image if a user is logged in
            find_remote = True
            if renku_url is None:
                find_remote = False
            else:
                token = read_renku_token(endpoint=renku_url)
                if not token or is_token_expired(token):
                    find_remote = False

            try:
                self.docker_client().images.get(image_name)
            except docker.errors.ImageNotFound:
                if not find_remote:
                    return False
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
            ProviderParameter(
                "blkio-weight", help="Block IO (relative weight), between 10 and 1000, or 0 to disable.", type=int
            ),
            ProviderParameter("cap-add", help="Add Linux capabilities.", multiple=True),
            ProviderParameter("cap-drop", help="Drop Linux capabilities.", multiple=True),
            ProviderParameter("cgroup-parent", help="Override the default parent cgroup.", type=str),
            ProviderParameter("cpu-count", help="Number of usable CPUs.", type=int),
            ProviderParameter("cpu-percent", help="Usable percentage of the available CPUs.", type=int),
            ProviderParameter("cpu-period", help="The length of a CPU period in microseconds.", type=int),
            ProviderParameter(
                "cpu-quota", help="Microseconds of CPU time that the container can get in a CPU period.", type=int
            ),
            ProviderParameter("cpu-rt-period", help="Limit CPU real-time period in microseconds.", type=int),
            ProviderParameter("cpu-rt-runtime", help="Limit CPU real-time runtime in microseconds.", type=int),
            ProviderParameter("cpu-shares", help="CPU shares (relative weight).", type=int),
            ProviderParameter("cpuset-cpus", help="CPUs in which to allow execution ('0-3', '0,1').", type=str),
            ProviderParameter(
                "cpuset-mems", help="Memory nodes (MEMs) in which to allow execution ('0-3', '0,1').", type=str
            ),
            ProviderParameter(
                "device-cgroup-rules",
                help="A list of cgroup rules to apply to the container.",
                multiple=True,
                flags=["device-cgroup-rule"],
            ),
            ProviderParameter("devices", help="Expose host devices to the container.", multiple=True, flags=["device"]),
            ProviderParameter("dns", help="Set custom DNS servers.", multiple=True),
            ProviderParameter(
                "dns-opt",
                help="Additional options to be added to the container's ``resolv.conf`` file.",
                type=str,
                flags=["dns-opt", "dns-option"],
            ),
            ProviderParameter("dns-search", help="DNS search domains.", multiple=True),
            ProviderParameter("domainname", help="Container NIS domain name.", type=str),
            ProviderParameter("entrypoint", help="The entrypoint for the container.", type=str),
            ProviderParameter(
                "environment",
                help="Environment variables to set inside the container, in the format 'VAR=VAL'",
                multiple=True,
                flags=["env"],
            ),
            ProviderParameter(
                "group-add",
                help="List of additional group names and/or IDs that the container process will run as.",
                multiple=True,
            ),
            ProviderParameter("hostname", help="Optional hostname for the container.", type=str),
            ProviderParameter(
                "init", help="Run an init inside the container that forwards signals and reaps processes", is_flag=True
            ),
            ProviderParameter("isolation", help="Isolation technology to use.", type=str),
            ProviderParameter("kernel-memory", help="Kernel memory limit (bytes).", type=int, metavar="<bytes>"),
            ProviderParameter("mac-address", help="MAC address to assign to the container.", type=str),
            ProviderParameter("mem-reservation", help="Memory soft limit.", type=int, flags=["memory-reservation"]),
            ProviderParameter(
                "mem-swappiness",
                help="Tune container memory swappiness (0 to 100).",
                type=int,
                flags=["memory-swappiness"],
            ),
            ProviderParameter("memswap-limit", help="Swap limit equal to memory plus swap.", flags=["memory-swap"]),
            ProviderParameter("name", help="The name for this container.", type=str),
            ProviderParameter("network", help="Connect a container to a network.", type=str),
            ProviderParameter("oom-kill-disable", help="Disable OOM Killer.", is_flag=True),
            ProviderParameter("oom-score-adj", help="Tune host's OOM preferences (-1000 to 1000).", type=int),
            ProviderParameter("pids-limit", help="Tune a container's PIDs limit.", type=int),
            ProviderParameter("platform", help="Set platform if server is multi-platform capable.", type=str),
            ProviderParameter("privileged", help="Give extended privileges to this container.", is_flag=True),
            ProviderParameter(
                "publish-all-ports", help="Publish all ports to the host.", is_flag=True, flags=["publish-all"]
            ),
            ProviderParameter("read-only", help="Mount the container's root filesystem as read-only", is_flag=True),
            ProviderParameter("remove", help="Automatically remove the container when it exits.", flags=["rm"]),
            ProviderParameter("runtime", help="Runtime to use with this container.", type=str),
            ProviderParameter("security-opt", help="Security Options.", multiple=True),
            ProviderParameter("shm-size", help="Size of /dev/shm (bytes).", type=int, metavar="<bytes>"),
            ProviderParameter(
                "stdin-open", help="Keep STDIN open even if not attached.", is_flag=True, flags=["interactive"]
            ),
            ProviderParameter("stop-signal", help="Signal to stop the container.", type=str),
            ProviderParameter("tty", help="Allocate a pseudo-TTY.", is_flag=True),
            ProviderParameter("user", help="Username or UID", type=str),
            ProviderParameter("volume-driver", help="The name of a volume driver/plugin.", type=str),
            ProviderParameter(
                "volumes",
                help="A list of volume mounts (e.g. '/host/path/:/mount/path/in/container')",
                multiple=True,
                flags=["volume"],
            ),
            ProviderParameter("volumes-from", help="Mount volumes from the specified container(s)", multiple=True),
        ]

    def get_open_parameters(self) -> List["ProviderParameter"]:
        """Returns parameters that can be set for session open."""
        return []

    def session_list(self, project_name: str) -> List[Session]:
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
        gpu_request: Optional[Union[str, int]] = None,
        **kwargs,
    ) -> Tuple[str, str]:
        """Creates an interactive session.

        Returns:
            Tuple[str, str]: Provider message and a possible warning message.
        """
        show_non_standard_user_warning = True

        def session_start_helper(consider_disk_request: bool):
            nonlocal gpu_request

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
                        if not isinstance(gpu_request, int):
                            try:
                                gpu_request = int(gpu_request)
                            except ValueError:
                                raise errors.ParameterError(
                                    f"Invalid value for 'gpu': '{gpu_request}'. Valid values are integers or 'all'"
                                )
                        resource_requests["device_requests"] = [
                            docker.types.DeviceRequest(count=gpu_request, capabilities=[["compute", "utility"]])
                        ]

                # NOTE: set git user
                image_data = self.docker_client().api.inspect_image(image_name)
                working_dir = image_data.get("Config", {}).get("WorkingDir", None)

                if working_dir is None:
                    working_dir = "/home/jovyan"

                work_dir = Path(working_dir) / "work" / project_name.split("/")[-1]

                volumes = kwargs.pop("volumes", [])
                volumes = list(volumes)
                volumes.append(f"{str(project_context.path.resolve())}:{work_dir}")

                environment = {}
                passed_env_vars = kwargs.pop("environment", [])
                for env_var in passed_env_vars:
                    var, _, value = env_var.partition("=")
                    if not var:
                        raise errors.ParameterError(f"Invalid environment variable: '{env_var}'")
                    environment[var] = value

                user = project_context.repository.get_user()
                environment.update(
                    {
                        "GIT_AUTHOR_NAME": user.name,
                        "GIT_AUTHOR_EMAIL": user.email,
                        "GIT_COMMITTER_EMAIL": user.email,
                        "EMAIL": user.email,
                    }
                )

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

                    additional_options["user"] = kwargs.pop("user", "root")
                    environment["NB_UID"] = str(os.getuid())
                    environment["CHOWN_HOME"] = "yes"
                    environment["CHOWN_HOME_OPTS"] = "-R"

                if "force_build" in kwargs:
                    del kwargs["force_build"]

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
                    **kwargs,
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

    def session_stop(self, project_name: str, session_name: Optional[str], stop_all: bool) -> SessionStopStatus:
        """Stops all or a given interactive session."""
        try:
            docker_containers = (
                self._get_docker_containers(project_name)
                if stop_all
                else self.docker_client().containers.list(filters={"id": session_name})
                if session_name
                else self.docker_client().containers.list()
            )

            n_docker_containers = len(docker_containers)

            if n_docker_containers == 0:
                return SessionStopStatus.FAILED if session_name else SessionStopStatus.NO_ACTIVE_SESSION
            elif not session_name and len(docker_containers) > 1:
                return SessionStopStatus.NAME_NEEDED

            [c.stop() for c in docker_containers]
        except docker.errors.APIError as error:
            raise errors.DockerError(error.msg)
        else:
            return SessionStopStatus.SUCCESSFUL

    def session_open(self, project_name: str, session_name: Optional[str], **kwargs) -> bool:
        """Open a given interactive session.

        Args:
            project_name(str): Renku project name.
            session_name(Optional[str]): The unique id of the interactive session.
        """
        url = self.session_url(session_name)

        if not url:
            return False

        webbrowser.open(url)
        return True

    def session_url(self, session_name: Optional[str]) -> Optional[str]:
        """Get the URL of the interactive session."""
        try:
            sessions = self.docker_client().containers.list()
        except errors.DockerError:
            return None
        default_url = get_value("interactive", "default_url")
        if not default_url:
            default_url = "/lab"

        for c in sessions:
            if (
                c.short_id == session_name or (not session_name and len(sessions) == 1)
            ) and f"{DockerSessionProvider.JUPYTER_PORT}/tcp" in c.ports:
                url = next(
                    DockerSessionProvider._get_jupyter_urls(
                        c.ports, c.labels["jupyter_token"], DockerSessionProvider.JUPYTER_PORT
                    ),
                    None,
                )
                if not url:
                    continue
                return url
        return None

    def force_build_image(self, force_build: bool = False, **kwargs) -> bool:
        """Whether we should force build the image directly or check for an existing image first."""
        return force_build
