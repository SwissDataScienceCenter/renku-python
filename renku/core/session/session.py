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
from typing import List, Optional

from pydantic import validate_arguments

from renku.core import errors
from renku.core.config import get_value
from renku.core.plugin.session import get_supported_session_providers
from renku.core.session.utils import get_image_repository_host, get_renku_project_name
from renku.core.util import communication
from renku.core.util.os import safe_read_yaml
from renku.domain_model.session import ISessionProvider, Session


def _safe_get_provider(provider: str) -> ISessionProvider:
    try:
        return next(p for p in get_supported_session_providers() if p.get_name() == provider)
    except StopIteration:
        raise errors.ParameterError(f"Session provider '{provider}' is not available!")


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def session_list(config_path: Optional[str], provider: Optional[str] = None):
    """List interactive sessions."""

    def list_sessions(session_provider: ISessionProvider) -> List[Session]:
        try:
            return session_provider.session_list(config=config, project_name=project_name)
        except errors.RenkulabSessionGetUrlError:
            if provider:
                raise
            return []

    project_name = get_renku_project_name()
    config = safe_read_yaml(config_path) if config_path else dict()

    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()

    return list(chain(*map(list_sessions, providers)))


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def session_start(
    provider: str,
    config_path: Optional[str],
    image_name: Optional[str] = None,
    cpu_request: Optional[float] = None,
    mem_request: Optional[str] = None,
    disk_request: Optional[str] = None,
    gpu_request: Optional[str] = None,
):
    """Start interactive session."""
    from renku.domain_model.project_context import project_context

    pinned_image = get_value("interactive", "image")
    if pinned_image and image_name is None:
        image_name = pinned_image

    provider_api = _safe_get_provider(provider)
    config = safe_read_yaml(config_path) if config_path else dict()

    provider_api.pre_start_checks()

    project_name = get_renku_project_name()
    if image_name is None:
        tag = project_context.repository.head.commit.hexsha[:7]
        repo_host = get_image_repository_host()
        image_name = f"{project_name}:{tag}"
        if repo_host:
            image_name = f"{repo_host}/{image_name}"

        if not provider_api.find_image(image_name, config):
            communication.confirm(
                f"The container image '{image_name}' does not exists. Would you like to build it?",
                abort=True,
            )
            with communication.busy(msg=f"Building image {image_name}"):
                _ = provider_api.build_image(project_context.docker_path.parent, image_name, config)
            communication.echo(f"Image {image_name} built successfully.")
    else:
        if not provider_api.find_image(image_name, config):
            raise errors.ParameterError(f"Cannot find the provided container image '{image_name}'!")

    # set resource settings
    cpu_limit = cpu_request or get_value("interactive", "cpu_request")

    if cpu_limit is not None:
        try:
            cpu_limit = float(cpu_limit)
        except ValueError:
            raise errors.SessionStartError(f"Invalid value for cpu_request (must be float): {cpu_limit}")

    disk_limit = disk_request or get_value("interactive", "disk_request")
    mem_limit = mem_request or get_value("interactive", "mem_request")
    gpu = gpu_request or get_value("interactive", "gpu_request")

    with communication.busy(msg="Waiting for session to start..."):
        session_name = provider_api.session_start(
            config=config,
            project_name=project_name,
            image_name=image_name,
            cpu_request=cpu_limit,
            mem_request=mem_limit,
            disk_request=disk_limit,
            gpu_request=gpu,
        )
    communication.echo(msg=f"Session {session_name} successfully started")
    return session_name


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def session_stop(session_name: Optional[str], stop_all: bool = False, provider: Optional[str] = None):
    """Stop interactive session."""

    def stop_sessions(session_provider: ISessionProvider) -> bool:
        try:
            return session_provider.session_stop(
                project_name=project_name, session_name=session_name, stop_all=stop_all
            )
        except errors.RenkulabSessionGetUrlError:
            if provider:
                raise
            return False

    session_detail = "all sessions" if stop_all else f"session {session_name}"
    project_name = get_renku_project_name()

    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()

    with communication.busy(msg=f"Waiting for {session_detail} to stop..."):
        is_stopped = any(map(stop_sessions, providers))

    if not is_stopped:
        if not session_name:
            raise errors.ParameterError("There are no running sessions.")
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def session_open(session_name: str, provider: Optional[str] = None):
    """Open interactive session in the browser."""

    def open_sessions(session_provider: ISessionProvider) -> Optional[str]:
        try:
            return session_provider.session_url(session_name=session_name)
        except errors.RenkulabSessionGetUrlError:
            if provider:
                raise
            return None

    providers = [_safe_get_provider(provider)] if provider else get_supported_session_providers()

    url = next(filter(lambda u: u is not None, map(open_sessions, providers)), None)

    if url is None:
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    webbrowser.open(url)
