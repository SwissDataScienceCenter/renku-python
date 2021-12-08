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
import urllib
import uuid
import webbrowser
from typing import TYPE_CHECKING

from renku.core import errors
from renku.core.management.command_builder import Command, inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.models.enums import ConfigFilter
from renku.core.utils import communication
from renku.core.utils.git import RENKU_BACKUP_PREFIX, create_backup_remote, get_remote, get_renku_repo_url
from renku.core.utils.urls import parse_authentication_endpoint

if TYPE_CHECKING:
    from renku.core.metadata.repository import Repository


CONFIG_SECTION = "http"


def login_command():
    """Return a command for logging in to the platform."""
    return Command().command(_login)


@inject.autoparams()
def _login(endpoint, git_login, yes, client_dispatcher: IClientDispatcher):
    from renku.core.utils import requests

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

    cli_nonce = str(uuid.uuid4())

    communication.echo(f"Please log in at {parsed_endpoint.geturl()} on your browser.")

    login_url = _get_url(parsed_endpoint, "/api/auth/login", cli_nonce=cli_nonce)
    webbrowser.open_new_tab(login_url)

    server_nonce = communication.prompt("Once completed, enter the security code that you receive at the end")
    cli_token_url = _get_url(parsed_endpoint, "/api/auth/cli-token", cli_nonce=cli_nonce, server_nonce=server_nonce)

    try:
        response = requests.get(cli_token_url)
    except errors.RequestError as e:
        raise errors.OperationError("Cannot get access token from remote host.") from e

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        _store_token(parsed_endpoint.netloc, access_token)

        if git_login:
            _set_git_credential_helper(repository=client.repository, hostname=parsed_endpoint.netloc)
            backup_remote_name, backup_exists, remote = create_backup_remote(
                repository=client.repository, remote_name=remote_name, url=remote_url
            )
            if backup_exists:
                communication.echo(f"Backup remote '{backup_remote_name}' already exists. Ignoring '--git' flag.")
            elif not remote:
                communication.error(f"Cannot create backup remote '{backup_remote_name}' for '{remote_url}'")
            else:
                _set_renku_url_for_remote(
                    repository=client.repository,
                    remote_name=remote_name,
                    remote_url=remote_url,
                    hostname=parsed_endpoint.netloc,
                )

    else:
        communication.error(
            f"Remote host did not return an access token: {parsed_endpoint.geturl()}, "
            f"status code: {response.status_code}"
        )
        sys.exit(1)


def _parse_endpoint(endpoint):
    parsed_endpoint = parse_authentication_endpoint(endpoint=endpoint)
    if not parsed_endpoint:
        raise errors.ParameterError("Parameter 'endpoint' is missing.")

    return parsed_endpoint


def _get_url(parsed_endpoint, path, **query_args):
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
    """Set renku repository URL for ``remote_name``."""
    new_remote_url = get_renku_repo_url(remote_url, deployment_hostname=hostname)

    try:
        repository.remotes[remote_name].set_url(url=new_remote_url)
    except errors.GitCommandError as e:
        raise errors.GitError(f"Cannot change remote url for '{remote_name}' to '{new_remote_url}'") from e


@inject.autoparams()
def read_renku_token(endpoint, client_dispatcher: IClientDispatcher):
    """Read renku token from renku config file."""
    try:
        parsed_endpoint = _parse_endpoint(endpoint)
    except errors.ParameterError:
        return
    if not parsed_endpoint:
        return

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
