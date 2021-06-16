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

import git
import requests

from renku.core import errors
from renku.core.incubation.command import Command
from renku.core.models.enums import ConfigFilter
from renku.core.utils import communication
from renku.core.utils.git import get_renku_repo_url
from renku.core.utils.urls import get_remote, parse_authentication_endpoint

CONFIG_SECTION = "http"
RENKU_BACKUP_PREFIX = "renku-backup"


def login_command():
    """Return a command for logging in to the platform."""
    return Command().command(_login)


def _login(client, endpoint, git_login, yes):
    parsed_endpoint = _parse_endpoint(client, endpoint)

    remote_name, remote_url = None, None
    if git_login:
        if not client.repo:
            raise errors.ParameterError("Cannot use '--git' flag outside a project.")

        remote_name, remote_url = get_remote(client.repo)
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
    except requests.RequestException as e:
        raise errors.OperationError("Cannot get access token from remote host.") from e

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        _store_token(client, parsed_endpoint.netloc, access_token)

        if git_login:
            _store_git_credential_helper(client, parsed_endpoint.netloc)
            _swap_git_remote(client, parsed_endpoint, remote_name, remote_url)
    else:
        communication.error(
            f"Remote host did not return an access token: {parsed_endpoint.geturl()}, "
            f"status code: {response.status_code}"
        )
        sys.exit(1)


def _parse_endpoint(client, endpoint):
    parsed_endpoint = parse_authentication_endpoint(client=client, endpoint=endpoint)
    if not parsed_endpoint:
        raise errors.ParameterError("Parameter 'endpoint' is missing.")

    return parsed_endpoint


def _get_url(parsed_endpoint, path, **query_args):
    query = urllib.parse.urlencode(query_args)
    return parsed_endpoint._replace(path=path, query=query).geturl()


def _store_token(client, netloc, access_token):
    client.set_value(section=CONFIG_SECTION, key=netloc, value=access_token, global_only=True)
    os.chmod(client.global_config_path, 0o600)


def _store_git_credential_helper(client, netloc):
    client.repo.git.config("credential.helper", f"!renku token --hostname {netloc}", local=True)


def _swap_git_remote(client, parsed_endpoint, remote_name, remote_url):
    backup_remote_name = f"{RENKU_BACKUP_PREFIX}-{remote_name}"

    if backup_remote_name in [r.name for r in client.repo.remotes]:
        communication.echo(f"Backup remove '{backup_remote_name}' already exists. Ignoring '--git' flag.")
        return

    new_remote_url = get_renku_repo_url(remote_url, deployment_hostname=parsed_endpoint.netloc)

    try:
        client.repo.create_remote(backup_remote_name, url=remote_url)
    except git.GitCommandError:
        communication.error(f"Cannot create backup remote '{backup_remote_name}' for '{remote_url}'")
    else:
        try:
            client.repo.git.remote("set-url", remote_name, new_remote_url)
        except git.GitCommandError:
            client.repo.delete_remote(backup_remote_name)
            raise errors.GitError(f"Cannot change remote url for '{remote_name}' to {new_remote_url}")


def read_renku_token(client, endpoint):
    """Read renku token from renku config file."""
    try:
        parsed_endpoint = _parse_endpoint(client, endpoint)
    except errors.ParameterError:
        return
    if not parsed_endpoint:
        return

    return _read_renku_token_for_hostname(client, parsed_endpoint.netloc)


def _read_renku_token_for_hostname(client, hostname):
    return client.get_value(section=CONFIG_SECTION, key=hostname, config_filter=ConfigFilter.GLOBAL_ONLY)


def logout_command():
    """Return a command for logging out from the platform."""
    return Command().command(_logout)


def _logout(client, endpoint):
    if endpoint:
        parsed_endpoint = parse_authentication_endpoint(client=client, endpoint=endpoint)
        key = parsed_endpoint.netloc
    else:
        key = "*"

    client.remove_value(section=CONFIG_SECTION, key=key, global_only=True)
    _remove_git_credential_helper(client)
    _restore_git_remote(client)


def _remove_git_credential_helper(client):
    try:
        client.repo.git.config("credential.helper", local=True, unset=True)
    except git.exc.GitCommandError:  # NOTE: If already logged out, ``git config --unset`` raises an exception
        pass


def _restore_git_remote(client):
    if not client.repo:  # Outside a renku project
        return

    backup_remotes = [r.name for r in client.repo.remotes if r.name.startswith(RENKU_BACKUP_PREFIX)]
    for backup_remote in backup_remotes:
        remote_name = backup_remote.replace(f"{RENKU_BACKUP_PREFIX}-", "")
        remote_url = client.repo.remotes[backup_remote].url

        try:
            client.repo.git.remote("set-url", remote_name, remote_url)
        except git.GitCommandError:
            raise errors.GitError(f"Cannot restore remote url for '{remote_name}' to {remote_url}")

        try:
            client.repo.delete_remote(backup_remote)
        except git.GitCommandError:
            communication.error(f"Cannot delete backup remote '{backup_remote}'")


def token_command():
    """Return a command as git credential helper."""
    return Command().command(_token)


def _token(client, command, hostname):
    if command != "get":
        return

    # NOTE: hostname comes from the credential helper we set up and has proper format
    hostname = hostname or ""
    token = _read_renku_token_for_hostname(client, hostname) or ""

    communication.echo("username=renku")
    communication.echo(f"password={token}")
