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

from typing import Optional

import webbrowser

from renku.core import errors
from renku.core.commands.format.session import SESSION_FORMATS
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils.os import safe_read_yaml


def _get_provider_command(provider: str, cmd: str):
    """Returns the session command implementation for a given session provider."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    providers = pm.hook.session_provider()
    provider_impl = next(filter(lambda x: provider == x[1], providers), None)
    if not provider_impl:
        raise errors.ParameterError(f"The specified session provider '{provider}' is not available.")

    providers.remove(provider_impl)
    return pm.subset_hook_caller(cmd, list(map(lambda x: x[0], providers)))


@inject.autoparams()
def _session_list(provider: str, config: str, client_dispatcher: IClientDispatcher, format="tabular"):
    client = client_dispatcher.current_client
    lister = _get_provider_command(provider, "session_list")
    if config:
        config = safe_read_yaml(config)
    return SESSION_FORMATS[format](lister(config=config, client=client)[0])


def session_list_command():
    """List all the running interactive sessions."""
    return Command().command(_session_list)


@inject.autoparams()
def _session_start(provider: str, config: str, client_dispatcher: IClientDispatcher, image_name: str = None):
    client = client_dispatcher.current_client
    session_starter = _get_provider_command(provider, "session_start")
    if config:
        config = safe_read_yaml(config)
    return session_starter(config=config, client=client, image_name=image_name)


def session_start_command():
    """Start an interactive session."""
    return Command().command(_session_start)


@inject.autoparams()
def _session_stop(
    session_name: str, client_dispatcher: IClientDispatcher, stop_all: bool = False, provider: Optional[str] = None
):
    from renku.core.plugins.pluginmanager import get_plugin_manager

    client = client_dispatcher.current_client
    if provider:
        session_stopper = _get_provider_command(provider, "session_stop")
        results = session_stopper(client=client, session_name=session_name, stop_all=stop_all)
    else:
        pm = get_plugin_manager()
        results = pm.hook.session_stop(client=client, session_name=session_name, stop_all=stop_all)

    if not any(results):
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")


def session_stop_command():
    """Stop a running an interactive session."""
    return Command().command(_session_stop)


@inject.autoparams()
def _session_open(session_name: str, client_dispatcher: IClientDispatcher, provider: Optional[str] = None):
    from renku.core.plugins.pluginmanager import get_plugin_manager

    client = client_dispatcher.current_client
    if provider:
        get_session_url = _get_provider_command(provider, "session_url")
        urls = get_session_url(client=client, session_name=session_name)
    else:
        pm = get_plugin_manager()
        urls = pm.hook.session_url(client=client, session_name=session_name)

    if len(urls) == 0:
        raise errors.ParameterError(f"Could not find '{session_name}' among the running sessions.")
    webbrowser.open(urls[0])


def session_open_command():
    """Open a running interactive session."""
    return Command().command(_session_open)
