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
"""Test ``login`` command."""

import pytest

from renku.command.login import read_renku_token
from renku.core import errors
from renku.core.util.contexts import chdir
from renku.ui.cli import cli
from tests.cli.fixtures.cli_gateway import ACCESS_TOKEN, ENDPOINT
from tests.utils import format_result_exception


def test_login(runner, client_with_remote, mock_login, client_database_injection_manager):
    """Test login command."""
    remote_url = client_with_remote.repository.remotes[0].url

    result = runner.invoke(cli, ["login", "--git", ENDPOINT], input="y")

    assert 0 == result.exit_code, format_result_exception(result)

    with client_database_injection_manager(client_with_remote):
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        credential = client_with_remote.repository.get_configuration().get_value("credential", "helper")
        assert f"!renku credentials --hostname {ENDPOINT}" == credential
        assert {"origin", "renku-backup-origin"} == {r.name for r in client_with_remote.repository.remotes}
        assert remote_url == client_with_remote.repository.remotes["renku-backup-origin"].url
        assert client_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")


@pytest.mark.parametrize("args", [[], ["--git"]])
def test_login_no_endpoint(runner, client, mock_login, args):
    """Test login command with no endpoint."""
    result = runner.invoke(cli, ["login"] + args)

    assert 2 == result.exit_code
    assert "Parameter 'endpoint' is missing." in result.output


@pytest.mark.parametrize("args", [[], ["--git"]])
def test_login_no_endpoint_and_remote(runner, client_with_remote, mock_login, args):
    """Test login command with no endpoint and with project remote."""
    result = runner.invoke(cli, ["login"] + args)

    assert 2 == result.exit_code, result.output
    assert "Parameter 'endpoint' is missing." in result.output


@pytest.mark.parametrize("args", [[], ["--git"]])
def test_login_invalid_endpoint(runner, client, mock_login, args):
    """Test login with and invalid endpoint."""
    result = runner.invoke(cli, ["login", "http: //example.com"] + args)

    assert 2 == result.exit_code
    assert "Invalid endpoint: `http: //example.com`." in result.output


def test_login_with_config_endpoint(runner, client, mock_login):
    """Test login command with endpoint in config file."""
    assert 0 == runner.invoke(cli, ["config", "set", "endpoint", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully logged in." in result.output


def test_logout(runner, client, mock_login, client_database_injection_manager):
    """Test logout removes all credentials."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code, format_result_exception(result)

    with client_database_injection_manager(client):
        assert read_renku_token(ENDPOINT) is None
    assert "Successfully logged out." in result.output


def test_repeated_login(runner, client, mock_login, client_database_injection_manager):
    """Test multiple logins."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    with client_database_injection_manager(client):
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)


def test_repeated_logout(runner, client, mock_login, client_database_injection_manager):
    """Test multiple logouts."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    with client_database_injection_manager(client):
        assert read_renku_token(ENDPOINT) is None


def test_login_to_multiple_endpoints(runner, client, mock_login, client_database_injection_manager):
    """Test login to multiple endpoints."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["login", second_endpoint]).exit_code

    with client_database_injection_manager(client):
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        assert second_token == read_renku_token(second_endpoint)


def test_logout_all(runner, client, mock_login, client_database_injection_manager):
    """Test logout with no endpoint removes multiple credentials."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code
    assert 0 == runner.invoke(cli, ["login", second_endpoint]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    with client_database_injection_manager(client):
        assert read_renku_token(ENDPOINT) is None
        assert read_renku_token(second_endpoint) is None


def test_logout_one_endpoint(runner, client, mock_login, client_database_injection_manager):
    """Test logout from an endpoint removes credentials for that endpoint only."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code
    assert 0 == runner.invoke(cli, ["login", second_endpoint]).exit_code

    assert 0 == runner.invoke(cli, ["logout", ENDPOINT]).exit_code

    with client_database_injection_manager(client):
        assert read_renku_token(ENDPOINT) is None
        assert second_token == read_renku_token(second_endpoint)


def test_logout_non_existing_endpoint(runner, client, mock_login, client_database_injection_manager):
    """Test logout from a non-existing endpoint does nothing."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["logout", "non.existing"]).exit_code

    with client_database_injection_manager(client):
        assert read_renku_token(ENDPOINT) is not None


def test_login_git_abort(runner, client_with_remote):
    """Test login command."""
    result = runner.invoke(cli, ["login", "--git", ENDPOINT], input="n")

    assert 1 == result.exit_code
    assert "Remote URL will be changed. Do you want to continue?" in result.output
    assert "Aborted!" in result.output


def test_login_non_git(runner, client, directory_tree):
    """Test login inside a non-git directory."""
    with chdir(directory_tree):
        result = runner.invoke(cli, ["login", "--git", ENDPOINT])

    assert 2 == result.exit_code
    assert "Cannot use '--git' flag outside a project" in result.output


def test_logout_non_git(runner, client, directory_tree):
    """Test logout inside a non-git directory."""
    with chdir(directory_tree):
        result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code
    assert "Successfully logged out." in result.output


def test_login_git_no_unique_remote(runner, client_with_remote):
    """Test login from a git directory with no clear remote."""
    client_with_remote.repository.remotes.add("second-remote", "second-remote.net")
    client_with_remote.repository.branches.add("branch-with-no-remote")
    client_with_remote.repository.checkout("branch-with-no-remote")

    result = runner.invoke(cli, ["login", "--git", ENDPOINT])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "Cannot find a unique remote URL for project." in result.output


def test_repeated_git_login(runner, client_with_remote, mock_login):
    """Test multiple logins to git repo fails to change remote URL after first time."""
    remote_url = client_with_remote.repository.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Backup remote 'renku-backup-origin' already exists. Ignoring '--git' flag." in result.output
    assert "Error: Cannot create backup remote 'renku-backup-origin' for" not in result.output
    assert {"origin", "renku-backup-origin"} == {r.name for r in client_with_remote.repository.remotes}
    assert remote_url == client_with_remote.repository.remotes["renku-backup-origin"].url
    assert client_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")
    assert not client_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo/repo")


def test_logout_git(runner, client_with_remote, mock_login):
    """Test logout removes backup remotes and restores original remote url."""
    remote_url = client_with_remote.repository.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert {"origin"} == {r.name for r in client_with_remote.repository.remotes}
    assert remote_url == client_with_remote.repository.remotes["origin"].url
    try:
        credential = client_with_remote.repository.get_configuration(scope="local").remove_value("credential", "helper")
    except errors.GitConfigurationError:  # NOTE: If already logged out, ``git config --unset`` raises an exception
        credential = None
    assert credential is None


def test_token(runner, client_with_remote, mock_login):
    """Test get credential when valid credential exist."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert f"password={ACCESS_TOKEN}\n" in result.output


def test_token_non_existing_hostname(runner, client_with_remote, mock_login):
    """Test get credential for a different hostname."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", "non-existing", "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_no_credential(runner, client_with_remote, mock_login):
    """Test get credential when valid credential doesn't exist."""
    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_invalid_command(runner, client_with_remote, mock_login, client_database_injection_manager):
    """Test call credential helper with a command other than 'get'."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "non-get-command"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "" == result.output

    with client_database_injection_manager(client_with_remote):
        assert read_renku_token(ENDPOINT) is not None
