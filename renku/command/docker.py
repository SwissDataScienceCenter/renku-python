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
"""Wrap Docker API."""

import subprocess
from configparser import NoSectionError

import attr

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.domain_model.git import GitURL


@inject.autoparams()
def detect_registry_url(client_dispatcher: IClientDispatcher, auto_login=True):
    """Return a URL of the Docker registry.

    Args:
        client_dispatcher(IClientDispatcher):  Injected client dispatcher.
        auto_login: Whether to log in automatically (Default value = True).

    Returns:
        The remote URL.
    """
    repository = client_dispatcher.current_client.repository
    config = repository.get_configuration()

    # Find registry URL in .git/config
    remote_url = None
    try:
        registry_url = config.get_value("renku", "registry", None)
    except NoSectionError:
        registry_url = None

    remote_branch = repository.active_branch.remote_branch

    if remote_branch is not None:
        remote_name = remote_branch.remote.name
        config_section = f'renku "{remote_name}"'
        try:
            registry_url = config.get_value(config_section, "registry", registry_url)
        except NoSectionError:
            pass
        remote_url = remote_branch.remote.url

    if registry_url:
        # Look in [renku] and [renku "{remote_name}"] for registry_url key.
        url = GitURL.parse(registry_url)
    elif remote_url:
        # Use URL based on remote configuration.
        url = GitURL.parse(remote_url)

        # Replace gitlab. with registry. unless running on gitlab.com.
        hostname_parts = url.hostname.split(".")
        if len(hostname_parts) > 2 and hostname_parts[0] == "gitlab":
            hostname_parts = hostname_parts[1:]
        hostname = ".".join(["registry"] + hostname_parts)
        url = attr.evolve(url, hostname=hostname)
    else:
        raise errors.GitConfigurationError("Configure renku.repository_url or Git remote.")

    if auto_login and url.username and url.password:
        try:
            subprocess.run(
                ["docker", "login", url.hostname, "-u", url.username, "--password-stdin"],
                check=True,
                input=url.password.encode("utf-8"),
            )
        except subprocess.CalledProcessError:
            raise errors.AuthenticationError("Check configuration of password or token in the registry URL")

    return url
