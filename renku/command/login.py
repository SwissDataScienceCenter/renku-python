# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
import time
import urllib
import webbrowser
from typing import TYPE_CHECKING

from renku.command.command_builder import Command, inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.util import communication
from renku.core.util.git import RENKU_BACKUP_PREFIX, create_backup_remote, get_remote, get_renku_repo_url
from renku.core.util.urls import parse_authentication_endpoint
from renku.domain_model.enums import ConfigFilter

if TYPE_CHECKING:
    from renku.infrastructure.repository import Repository


CONFIG_SECTION = "http"
KEYCLOAK_REALM = "Renku"
CLIENT_ID = "renku-cli"


def login_command():
    """Return a command for logging in to the platform."""
    return Command().command(_login)


@inject.autoparams()
def _login(endpoint, git_login, yes, client_dispatcher: IClientDispatcher):
    from renku.core.util import requests

    client = client_dispatcher.current_client

    parsed_endpoint = _parse_endpoint(endpoint)

    remote_name, remote_url = None, None
    if git_login:
        if not client.repository:
            raise errors.ParameterError("Cannot use '--git' flag outside a project.")

        remote = get_remote(client.repository)
        if remote:
            remote_name, remote_url = remote.name, remote.url

        if remote_name and remote_url:
            if not yes:
                communication.confirm("Remote URL will be changed. Do you want to continue?", abort=True, warning=True)
        else:
            raise errors.ParameterError("Cannot find a unique remote URL for project.")

    auth_server_url = _get_url(
        parsed_endpoint, path=f"auth/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth/device"
    )

    try:
        response = requests.post(auth_server_url, data={"client_id": CLIENT_ID})
    except errors.RequestError as e:
        raise errors.RequestError(f"Cannot connect to authorization server at {auth_server_url}.") from e

    requests.check_response(response=response)
    data = response.json()

    verification_uri = data.get("verification_uri")
    user_code = data.get("user_code")
    verification_uri_complete = f"{verification_uri}?user_code={user_code}"

    communication.echo(
        f"Please grant access to '{CLIENT_ID}' in your browser.\n"
        f"If a browser window does not open automatically, go to {verification_uri_complete}"
    )

    webbrowser.open_new_tab(verification_uri_complete)

    polling_interval = min(data.get("interval", 5), 5)
    token_url = _get_url(parsed_endpoint, path=f"auth/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token")
    device_code = data.get("device_code")

    while True:
        time.sleep(polling_interval)

        response = requests.post(
            token_url,
            data={
                "device_code": device_code,
                "client_id": CLIENT_ID,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
        )
        status_code = response.status_code
        if status_code == 200:
            break
        elif status_code == 400:
            error = response.json().get("error")

            if error == "authorization_pending":
                continue
            elif error == "slow_down":
                polling_interval += 1
            elif error == "access_denied":
                raise errors.AuthenticationError("Access denied")
            elif error == "expired_token":
                raise errors.AuthenticationError("Session expired, try again")
            else:
                raise errors.AuthenticationError(f"Invalid error message from server: {response.json()}")
        else:
            raise errors.AuthenticationError(f"Invalid status code from server: {status_code} - {response.content}")

    access_token = response.json().get("access_token")
    _store_token(parsed_endpoint.netloc, access_token)

    if git_login:
        _set_git_credential_helper(repository=client.repository, hostname=parsed_endpoint.netloc)
        backup_remote_name, backup_exists, remote = create_backup_remote(
            repository=client.repository, remote_name=remote_name, url=remote_url  # type:ignore
        )
        if backup_exists:
            communication.echo(f"Backup remote '{backup_remote_name}' already exists. Ignoring '--git' flag.")
        elif not remote:
            communication.error(f"Cannot create backup remote '{backup_remote_name}' for '{remote_url}'")
        else:
            _set_renku_url_for_remote(
                repository=client.repository,
                remote_name=remote_name,  # type:ignore
                remote_url=remote_url,  # type:ignore
                hostname=parsed_endpoint.netloc,
            )


def _parse_endpoint(endpoint, use_remote=False):
    parsed_endpoint = parse_authentication_endpoint(endpoint=endpoint, use_remote=use_remote)
    if not parsed_endpoint:
        raise errors.ParameterError("Parameter 'endpoint' is missing.")

    return parsed_endpoint


def _get_url(parsed_endpoint, path, **query_args) -> str:
    query = urllib.parse.urlencode(query_args)
    return parsed_endpoint._replace(path=path, query=query).geturl()


@inject.autoparams()
def _store_token(netloc, access_token, client_dispatcher: IClientDispatcher):
    client = client_dispatcher.current_client

    client.set_value(section=CONFIG_SECTION, key=netloc, value=access_token, global_only=True)
    os.chmod(client.global_config_path, 0o600)


def _set_git_credential_helper(repository: "Repository", hostname):
    with repository.get_configuration(writable=True) as config:
        config.set_value("credential", "helper", f"!renku credentials --hostname {hostname}")


def _set_renku_url_for_remote(repository: "Repository", remote_name: str, remote_url: str, hostname: str):
    """Set renku repository URL for ``remote_name``.

    Args:
        repository("Repository"): Current ``Repository``.
        remote_name(str): Name of the remote.
        remote_url(str): Url of the remote.
        hostname(str): Hostname.

    Raises:
        errors.GitCommandError: If remote doesn't exist.
    """
    new_remote_url = get_renku_repo_url(remote_url, deployment_hostname=hostname)

    try:
        repository.remotes[remote_name].set_url(url=new_remote_url)
    except errors.GitCommandError as e:
        raise errors.GitError(f"Cannot change remote url for '{remote_name}' to '{new_remote_url}'") from e


@inject.autoparams()
def read_renku_token(endpoint: str, client_dispatcher: IClientDispatcher, get_endpoint_from_remote=False) -> str:
    """Read renku token from renku config file.

    Args:
        endpoint(str):  Endpoint to get token for.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
    Keywords:
        get_endpoint_from_remote: if no endpoint is specified, use the repository remote to infer one

    Returns:
        Token for endpoint.
    """
    try:
        parsed_endpoint = _parse_endpoint(endpoint, use_remote=get_endpoint_from_remote)
    except errors.ParameterError:
        return ""
    if not parsed_endpoint:
        return ""

    return _read_renku_token_for_hostname(client_dispatcher.current_client, parsed_endpoint.netloc)


def _read_renku_token_for_hostname(client, hostname):
    return client.get_value(section=CONFIG_SECTION, key=hostname, config_filter=ConfigFilter.GLOBAL_ONLY)


def logout_command():
    """Return a command for logging out from the platform."""
    return Command().command(_logout)


@inject.autoparams()
def _logout(endpoint, client_dispatcher: IClientDispatcher):
    if endpoint:
        parsed_endpoint = parse_authentication_endpoint(endpoint=endpoint)
        key = parsed_endpoint.netloc
    else:
        key = "*"

    client = client_dispatcher.current_client
    client.remove_value(section=CONFIG_SECTION, key=key, global_only=True)
    _remove_git_credential_helper(client=client)
    _restore_git_remote(client=client)


def _remove_git_credential_helper(client):
    if not client.repository:  # Outside a renku project
        return

    with client.repository.get_configuration(writable=True) as config:
        try:
            config.remove_value("credential", "helper")
        except errors.GitError:  # NOTE: If already logged out, an exception is raised
            pass


def _restore_git_remote(client):
    if not client.repository:  # Outside a renku project
        return

    backup_remotes = [r.name for r in client.repository.remotes if r.name.startswith(RENKU_BACKUP_PREFIX)]
    for backup_remote in backup_remotes:
        remote_name = backup_remote.replace(f"{RENKU_BACKUP_PREFIX}-", "")
        remote_url = client.repository.remotes[backup_remote].url

        try:
            client.repository.remotes[remote_name].set_url(remote_url)
        except errors.GitCommandError:
            raise errors.GitError(f"Cannot restore remote url for '{remote_name}' to {remote_url}")

        try:
            client.repository.remotes.remove(backup_remote)
        except errors.GitCommandError:
            communication.error(f"Cannot delete backup remote '{backup_remote}'")


def credentials_command():
    """Return a command as git credential helper."""
    return Command().command(_credentials)


@inject.autoparams()
def _credentials(command, hostname, client_dispatcher: IClientDispatcher):
    if command != "get":
        return

    # NOTE: hostname comes from the credential helper we set up and has proper format
    hostname = hostname or ""
    token = _read_renku_token_for_hostname(client_dispatcher.current_client, hostname) or ""

    communication.echo("username=renku")
    communication.echo(f"password={token}")
