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
"""Logging in to a Renku deployment."""

import os
import sys
import time
import urllib
import uuid
import webbrowser
from pathlib import posixpath

import git
import requests

from renku.core import errors
from renku.core.commands.config import read_config
from renku.core.incubation.command import Command
from renku.core.models.enums import ConfigFilter
from renku.core.utils import communication

CONFIG_SECTION = "http"


def login_command():
    """Return a command for logging in to the platform."""
    return Command().command(_login)


def _login(client, endpoint):
    parsed_endpoint = _parse_endpoint(endpoint)
    query = urllib.parse.urlencode({"cli_token": str(uuid.uuid4())})

    communication.echo(
        f"Please log in at {parsed_endpoint.geturl()} on your browser.\n"
        "Once completed, you may close the browser window."
    )

    login_url = _get_url(parsed_endpoint, "/api/auth/login", query)
    webbrowser.open_new_tab(login_url)

    info_url = _get_url(parsed_endpoint, "/api/auth/info", query)
    access_token = None
    wait_interval = 2
    timeout = wait_interval * 30  # 60 seconds
    while timeout > 0:
        timeout -= wait_interval
        time.sleep(wait_interval)

        try:
            response = requests.get(info_url)
        except requests.RequestException:
            pass
        else:
            if response.status_code == 200:
                access_token = response.json().get("access_token")
                break

    if access_token is None:
        communication.error(f"Cannot get access token from remote host: {parsed_endpoint.geturl()}")
        sys.exit(1)

    _store_token(client, parsed_endpoint, access_token)


def _parse_endpoint(endpoint):
    if not endpoint:
        try:
            endpoint = read_config().build().execute("endpoint").output
        except errors.ParameterError:
            raise errors.ParameterError("Parameter `endpoint` is missing.")

    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"

    parsed_endpoint = urllib.parse.urlparse(endpoint)
    if not parsed_endpoint.netloc:
        raise errors.ParameterError(f"Invalid endpoint: `{endpoint}`.")

    path = parsed_endpoint.path or "/"
    return parsed_endpoint._replace(scheme="https", path=path, params="", query="", fragment="")


def _get_url(parsed_endpoint, path, query):
    path = posixpath.join(parsed_endpoint.path, path.lstrip("/"))
    return parsed_endpoint._replace(path=path, query=query).geturl()


def _store_token(client, parsed_endpoint, token):
    client.set_value(section=CONFIG_SECTION, key=parsed_endpoint.netloc, value=token, global_only=True)
    os.chmod(client.global_config_path, 0o600)

    if not client.repo:
        communication.warn("Not inside a git repository: Cannot store credentials.")
        return

    value = f"Renku-Token: {token}"
    client.repo.git.config("http.extraheader", value, local=True)


def read_renku_token(client, endpoint):
    """Read renku token from renku config file."""
    parsed_endpoint = _parse_endpoint(endpoint)
    return client.get_value(section=CONFIG_SECTION, key=parsed_endpoint.netloc, config_filter=ConfigFilter.GLOBAL_ONLY)


def logout_command():
    """Return a command for logging out from the platform."""
    return Command().command(_logout)


def _logout(client):
    client.remove_value(section=CONFIG_SECTION, key="*", global_only=True)

    if not client.repo:
        communication.warn("Not inside a git repository: Cannot remove credentials.")
        return

    try:
        client.repo.git.config("http.extraheader", local=True, unset=True)
    except git.exc.GitCommandError:  # NOTE: If already logged out, git config --unset raises an exception
        pass
