# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import docker
from yaspin import yaspin

from renku.core import errors
from renku.core.management.client import LocalClient
from renku.core.models.session import ISessionProvider, Session
from renku.core.plugins import hookimpl
from renku.core.utils import communication


class DockerSessionProvider(ISessionProvider):
    """A docker based interactive session provider."""

    JUPYTER_PORT = 8888

    def __init__(self):
        self._docker_client = None

    def docker_client(self) -> docker.client.DockerClient:
        """Get the docker client."""
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
            except docker.errors.DockerException as e:
                raise errors.DockerError(f"Error while initializing docker client: {str(e)}")
        return self._docker_client

    @staticmethod
    def _docker_image_name(remote: Dict[str, str], commit_sha: str = None) -> str:
        if commit_sha:
            return f"{remote['owner']}/{remote['name']}:{commit_sha}"
        return f"{remote['owner']}/{remote['name']}"

    @staticmethod
    def _get_jupyter_urls(ports: Dict[str, Any], auth_token: str, jupyter_port: int = 8888) -> List[str]:
        port_key = f"{jupyter_port}/tcp"
        if port_key not in ports:
            return None
        return map(lambda x: f'http://{x["HostIp"]}:{x["HostPort"]}/?token={auth_token}', ports[port_key])

    def _get_docker_containers(self, client: LocalClient) -> List[docker.models.containers.Container]:
        return self.docker_client().containers.list(
            filters={"label": f"renku_project={DockerSessionProvider._docker_image_name(client.remote)}"}
        )

    @hookimpl
    def session_provider(self) -> Tuple[ISessionProvider, str]:
        """Supported session provider.

        :returns: a tuple of ``self`` and provider name.
        """
        return (self, "docker")

    @hookimpl
    def session_list(self, config: Optional[Path], client: LocalClient) -> List[Session]:
        """Lists all the sessions currently running by the given session provider.

        :returns: a list of sessions.
        """

        return map(
            lambda x: Session(
                id=x.short_id,
                status=x.status,
                url=next(DockerSessionProvider()._get_jupyter_urls(x.ports, x.labels["jupyter_token"])),
            ),
            self._get_docker_containers(client),
        )

    @hookimpl
    def session_start(self, config: Optional[Path], image_name: Optional[str], client: LocalClient) -> str:
        """Creates an interactive session.

        :returns: a unique id for the created interactive sesssion.
        """

        def _find_docker_image(remote: Dict[str, str], commit_sha: str) -> docker.models.images.Image:
            try:
                return self.docker_client().images.get(DockerSessionProvider._docker_image_name(remote, commit_sha))
            except docker.errors.ImageNotFound:
                # try to pull
                return self.docker_client().images.pull(
                    DockerSessionProvider._docker_image_name(remote), tag=commit_sha
                )

        try:
            docker_is_running = self.docker_client().ping()
            if not docker_is_running:
                raise errors.DockerError(
                    "Could not communicate with the docker instance. Please make sure it is running!"
                )
        except docker.api.APIError as error:
            raise errors.DockerError(error)

        try:
            remote = client.remote
            commit_short_sha = client.repository.head.commit.hexsha[:7]
            if image_name is None:
                image_name = DockerSessionProvider._docker_image_name(remote, commit_short_sha)

                try:
                    _ = _find_docker_image(remote, commit_short_sha)
                except docker.errors.ImageNotFound:
                    communication.confirm(
                        f"The docker image '{image_name}' does not exists. Would you like to build it?",
                        abort=True,
                    )

                    with yaspin(text="Building docker image"):
                        _ = self.docker_client().images.build(path=str(client.docker_path.parent), tag=image_name)

            auth_token = uuid4().hex
            container = self.docker_client().containers.run(
                image_name,
                f'jupyter notebook --NotebookApp.ip="0.0.0.0" --NotebookApp.port={DockerSessionProvider.JUPYTER_PORT}'
                f' --NotebookApp.token="{auth_token}" --NotebookApp.default_url="/lab"'
                " --NotebookApp.notebook_dir=/home/jovyan/work",
                detach=True,
                labels={"renku_project": DockerSessionProvider._docker_image_name(remote), "jupyter_token": auth_token},
                ports={f"{DockerSessionProvider.JUPYTER_PORT}/tcp": None},
                remove=True,
                volumes=[f"{str(client.path.resolve())}:/home/jovyan/work"],
            )

            if not container.ports:
                container.reload()

            jupyter_urls = DockerSessionProvider._get_jupyter_urls(
                container.ports, auth_token, jupyter_port=DockerSessionProvider.JUPYTER_PORT
            )
            message = f"The session for '{image_name}' has been successfully started. It is available at:\n\t"
            message += "\n\t".join(jupyter_urls)
            return message
        except (docker.errors.APIError, docker.errors.BuildError) as error:
            raise errors.DockerError(error.msg)

    @hookimpl
    def session_stop(self, client: LocalClient, session_name: Optional[str], stop_all: bool):
        """Stops all or a given interactive session."""
        try:
            docker_containers = (
                self._get_docker_containers(client)
                if all
                else self.docker_client().containers.list(filters={"id": session_name})
            )

            if len(docker_containers) == 0:
                return False

            [c.stop() for c in docker_containers]
            return True
        except docker.errors.APIError as error:
            raise errors.DockerError(error.msg)

    @hookimpl
    def session_url(self, client: LocalClient, session_name: str) -> Optional[str]:
        """Get the URL of the interactive session."""
        repo_containers = self._get_docker_containers(client)
        for c in repo_containers:
            if c.short_id == session_name and f"{DockerSessionProvider.JUPYTER_PORT}/tcp" in c.ports:
                host = c.ports[f"{DockerSessionProvider.JUPYTER_PORT}/tcp"][0]
                return f'http://{host["HostIp"]}:{host["HostPort"]}/?token={c.labels["jupyter_token"]}'
        return None
