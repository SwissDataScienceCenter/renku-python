# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku session commands."""

from itertools import chain

import docker
from yaspin import yaspin

from renku.core import errors
from renku.core.commands.format.session import SESSION_FORMATS
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication

JUPYTER_PORT = 8888


def _docker_image_name(remote, commit_sha=None):
    if commit_sha:
        return f"{remote['owner']}/{remote['name']}:{commit_sha}"
    return f"{remote['owner']}/{remote['name']}"


def _get_jupyter_urls(ports, jupyter_port=8888):
    port_key = f"{jupyter_port}/tcp"
    if port_key not in ports:
        return None
    return map(lambda x: f'http://{x["HostIp"]}:{x["HostPort"]}/', ports[port_key])


def _get_docker_containers(renku_client, docker_client):
    docker_image_tags = list(
        chain(
            *map(
                lambda x: x.tags,
                docker_client.images.list(filters={"reference": _docker_image_name(renku_client.remote)}),
            )
        )
    )
    return docker_client.containers.list(filters={"ancestor": docker_image_tags})


@inject.autoparams()
def _session_list(client_dispatcher: IClientDispatcher, format="tabular"):
    client = client_dispatcher.current_client
    docker_client = docker.from_env()
    repo_containers = _get_docker_containers(client, docker_client)

    return SESSION_FORMATS[format](repo_containers)


def session_list_command():
    """List all the running local interactive session."""
    return Command().command(_session_list)


@inject.autoparams()
def _session_start(client_dispatcher: IClientDispatcher):
    client = client_dispatcher.current_client
    docker_client = docker.from_env()

    def _find_docker_image(remote, commit_sha):
        try:
            return docker_client.images.get(_docker_image_name(remote, commit_sha))
        except docker.errors.ImageNotFound:
            # try to pull
            return docker_client.images.pull(_docker_image_name(remote), tag=commit_sha)

    try:
        docker_is_running = docker_client.ping()
        if not docker_is_running:
            raise errors.DockerError("Could not communicate with the docker instance. Please make sure it is running!")
    except docker.api.APIError as error:
        raise errors.DockerError(error)

    try:
        remote = client.remote
        commit_short_sha = client.repository.head.commit.hexsha[:7]
        image_name = _docker_image_name(remote, commit_short_sha)

        try:
            image = _find_docker_image(remote, commit_short_sha)
        except docker.errors.ImageNotFound:
            communication.confirm(
                f"The docker image '{image_name}' does not exists. Would you like to build it?",
                abort=True,
            )

            with yaspin(text="Building docker image"):
                image, logs = docker_client.images.build(path=str(client.docker_path.parent), tag=image_name)

        # TODO: no tokens? security concerns ?
        container = docker_client.containers.run(
            image_name,
            f'jupyter notebook --NotebookApp.ip="0.0.0.0" --NotebookApp.port={JUPYTER_PORT} --NotebookApp.token=""'
            ' --NotebookApp.default_url="/lab" --NotebookApp.notebook_dir=/home/jovyan/work',
            detach=True,
            ports={f"{JUPYTER_PORT}/tcp": None},
            remove=True,
            volumes=[f"{str(client.path.resolve())}:/home/jovyan/work"],
        )

        if not container.ports:
            container.reload()

        jupyter_urls = _get_jupyter_urls(container.ports, jupyter_port=JUPYTER_PORT)
        message = f"The local session for '{image_name}' has been successfully started. It is available at:\n\t"
        message += "\n\t".join(jupyter_urls)
        return message
    except (docker.errors.APIError, docker.errors.BuildError) as error:
        raise errors.DockerError(error.msg)


def session_start_command():
    """Start a running local interactive session."""
    return Command().command(_session_start)


@inject.autoparams()
def _session_stop(session_name: str, client_dispatcher: IClientDispatcher, stop_all=False):
    client = client_dispatcher.current_client
    docker_client = docker.from_env()

    try:
        docker_containers = (
            _get_docker_containers(client, docker_client)
            if stop_all
            else docker_client.containers.list(filters={"id": session_name})
        )

        if len(docker_containers) == 0:
            message = (
                "There are no running containers for this project!"
                if stop_all
                else f"Invalid docker container ID '{session_name}'!"
            )
            raise errors.ParameterError(message)

        [c.stop() for c in docker_containers]
    except docker.errors.APIError as error:
        raise errors.DockerError(error.msg)


def session_stop_command():
    """Stop a running local interactive session."""
    return Command().command(_session_stop)
