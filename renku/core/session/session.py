# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Interactive session business logic."""

import webbrowser
from itertools import chain
from typing import Optional

from yaspin import yaspin

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.plugin.session import supported_session_providers
from renku.core.util import communication
from renku.core.util.os import safe_read_yaml
from renku.domain_model.session import ISessionProvider


@inject.autoparams()
def _get_renku_project_name(client_dispatcher: IClientDispatcher) -> str:
    client = client_dispatcher.current_client
    return f"{client.remote['owner']}/{client.remote['name']}" if client.remote["name"] else f"{client.path.name}"


def _safe_get_provider(provider: str) -> ISessionProvider:
    providers = supported_session_providers()
    p = next(filter(lambda x: x[1] == provider, providers), None)

    if p is None:
        raise errors.ParameterError(f"Session provider '{provider}' is not available!")
    return p[0]


def session_list(config_path: str, provider: Optional[str] = None):
    """List interactive sessions."""
    project_name = _get_renku_project_name()
    config = safe_read_yaml(config_path) if config_path else dict()

    providers = supported_session_providers()
    if provider:
        providers = list(filter(lambda x: x[1] == provider, providers))

    if len(providers) == 0:
        raise errors.ParameterError("No session provider is available!")

    return list(chain(*map(lambda x: x[0].session_list(config=config, project_name=project_name), providers)))


@inject.autoparams("client_dispatcher")
def session_start(
    provider: str,
    config_path: str,
    client_dispatcher: IClientDispatcher,
    image_name: str = None,
    cpu_request: Optional[float] = None,
    mem_request: Optional[str] = None,
    disk_request: Optional[str] = None,
    gpu_request: Optional[str] = None,
):
    """Start interactive session."""
    client = client_dispatcher.current_client

    pinned_image = client.get_value("interactive", "image")
    if pinned_image and image_name is None:
        image_name = pinned_image

    provider_api = _safe_get_provider(provider)
    config = safe_read_yaml(config_path) if config_path else dict()

    project_name = _get_renku_project_name()
    if image_name is None:
        tag = client.repository.head.commit.hexsha[:7]
        image_name = f"{project_name}:{tag}"

        if not provider_api.find_image(image_name, config):
            communication.confirm(
                f"The container image '{image_name}' does not exists. Would you like to build it?",
                abort=True,
            )

            with yaspin(text="Building image"):
                _ = provider_api.build_image(client.docker_path.parent, image_name, config)
    else:
        if not provider_api.find_image(image_name, config):
            raise errors.ParameterError(f"Cannot find the provided container image '{image_name}'!")

    # set resource settings
    cpu_limit = cpu_request or client.get_value("interactive", "cpu_request")
    disk_limit = disk_request or client.get_value("interactive", "disk_request")
    mem_limit = mem_request or client.get_value("interactive", "mem_request")
    gpu = gpu_request or client.get_value("interactive", "gpu_request")

    return provider_api.session_start(
        config=config,
        project_name=project_name,
        image_name=image_name,
        client=client,
        cpu_request=cpu_limit,
        mem_request=mem_limit,
        disk_request=disk_limit,
        gpu_request=gpu,
    )


def session_stop(session_name: str, stop_all: bool = False, provider: Optional[str] = None):
    """Stop interactive session."""
    project_name = _get_renku_project_name()
    if provider:
        p = _safe_get_provider(provider)
        is_stopped = p.session_stop(project_name=project_name, session_name=session_name, stop_all=stop_all)
    else:
        providers = supported_session_providers()
        is_stopped = any(
            map(
                lambda x: x[0].session_stop(project_name=project_name, session_name=session_name, stop_all=stop_all),
                providers,
            )
        )

    if not is_stopped:
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")


def session_open(session_name: str, provider: Optional[str] = None):
    """Open interactive session in the browser."""
    if provider:
        p = _safe_get_provider(provider)
        url = p.session_url(session_name=session_name)
    else:
        providers = supported_session_providers()
        url = next(
            filter(
                lambda x: x is not None,
                map(lambda p: p[0].session_url(session_name=session_name), providers),
            ),
            None,
        )

    if url is None:
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    webbrowser.open(url)
