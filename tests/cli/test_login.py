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
"""Test ``login`` command."""

import git
import pytest

from renku.cli import cli
from renku.core.commands.login import read_renku_token
from renku.core.utils.contexts import chdir
from tests.cli.fixtures.cli_gateway import ACCESS_TOKEN, ENDPOINT, USER_CODE


def test_login(runner, client_with_remote, mock_login):
    """Test login command."""
    remote_url = client_with_remote.repo.remotes[0].url

    result = runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT], input=USER_CODE)

    assert 0 == result.exit_code
    assert ACCESS_TOKEN == read_renku_token(client_with_remote, ENDPOINT)
    credential = client_with_remote.repo.config_reader().get_value("credential", "helper")
    assert f"!renku token --hostname {ENDPOINT}" == credential
    assert {"origin", "renku-backup-origin"} == {r.name for r in client_with_remote.repo.remotes}
    assert remote_url == client_with_remote.repo.remotes["renku-backup-origin"].url
    assert client_with_remote.repo.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")


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


def test_login_invalid_user_code(runner, client, mock_login):
    """Test login fails if user code is not valid."""
    result = runner.invoke(cli, ["login", ENDPOINT], input="invalid_user_code")

    assert 1 == result.exit_code
    assert "Remote host did not return an access token:" in result.output
    assert "status code: 404" in result.output


def test_login_with_config_endpoint(runner, client, mock_login):
    """Test login command with endpoint in config file."""
    assert 0 == runner.invoke(cli, ["config", "set", "endpoint", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login"], input=USER_CODE)

    assert 0 == result.exit_code
    assert "Successfully logged in." in result.output


def test_logout(runner, client, mock_login):
    """Test logout removes all credentials."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code
    assert read_renku_token(client, ENDPOINT) is None
    assert "Successfully logged out." in result.output


def test_repeated_login(runner, client, mock_login):
    """Test multiple logins."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code
    assert ACCESS_TOKEN == read_renku_token(client, ENDPOINT)


def test_repeated_logout(runner, client, mock_login):
    """Test multiple logouts."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code
    assert read_renku_token(client, ENDPOINT) is None


def test_login_to_multiple_endpoints(runner, client, mock_login):
    """Test login to multiple endpoints."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_endpoint_token(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["login", second_endpoint], input=USER_CODE).exit_code

    assert ACCESS_TOKEN == read_renku_token(client, ENDPOINT)
    assert second_token == read_renku_token(client, second_endpoint)


def test_logout_all(runner, client, mock_login):
    """Test logout with no endpoint removes multiple credentials."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_endpoint_token(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code
    assert 0 == runner.invoke(cli, ["login", second_endpoint], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert read_renku_token(client, ENDPOINT) is None
    assert read_renku_token(client, second_endpoint) is None


def test_logout_one_endpoint(runner, client, mock_login):
    """Test logout from an endpoint removes credentials for that endpoint only."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_endpoint_token(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code
    assert 0 == runner.invoke(cli, ["login", second_endpoint], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout", ENDPOINT]).exit_code

    assert read_renku_token(client, ENDPOINT) is None
    assert second_token == read_renku_token(client, second_endpoint)


def test_logout_non_existing_endpoint(runner, client, mock_login):
    """Test logout from a non-existing endpoint does nothing."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout", "non.existing"]).exit_code

    assert read_renku_token(client, ENDPOINT) is not None


def test_login_git_abort(runner, client_with_remote):
    """Test login command."""
    result = runner.invoke(cli, ["login", "--git", ENDPOINT], input="n")

    assert 1 == result.exit_code
    assert "Remote URL will be changed. Do you want to continue?" in result.output
    assert "Aborted!" in result.output


def test_login_non_git(runner, client, directory_tree):
    """Test login from a non-git directory."""
    with chdir(directory_tree):
        result = runner.invoke(cli, ["login", "--git", ENDPOINT])

    assert 2 == result.exit_code
    assert "Cannot use '--git' flag outside a project" in result.output


def test_login_git_no_unique_remote(runner, client_with_remote):
    """Test login from a git directory with no clear remote."""
    client_with_remote.repo.create_remote("second-remote", "second-remote.net")
    client_with_remote.repo.create_head("branch-with-no-remote")
    client_with_remote.repo.heads["branch-with-no-remote"].checkout()

    result = runner.invoke(cli, ["login", "--git", ENDPOINT])

    assert 2 == result.exit_code
    assert "Cannot find a unique remote URL for project." in result.output


def test_repeated_git_login(runner, client_with_remote, mock_login):
    """Test multiple logins to git repo fails to change remote URL after first time."""
    remote_url = client_with_remote.repo.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT], input=USER_CODE)

    assert 0 == result.exit_code
    assert "Backup remove 'renku-backup-origin' already exists. Ignoring '--git' flag." in result.output
    assert "Error: Cannot create backup remote 'renku-backup-origin' for" not in result.output
    assert {"origin", "renku-backup-origin"} == {r.name for r in client_with_remote.repo.remotes}
    assert remote_url == client_with_remote.repo.remotes["renku-backup-origin"].url
    assert client_with_remote.repo.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")
    assert not client_with_remote.repo.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo/repo")


def test_logout_git(runner, client_with_remote, mock_login):
    """Test logout removes backup remotes and restores original remote url."""
    remote_url = client_with_remote.repo.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--git", "--yes", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code
    assert {"origin"} == {r.name for r in client_with_remote.repo.remotes}
    assert remote_url == client_with_remote.repo.remotes["origin"].url
    try:
        credential = client_with_remote.repo.git.config("credential.helper", local=True)
    except git.exc.GitCommandError:  # NOTE: If already logged out, ``git config --unset`` raises an exception
        credential = None
    assert credential is None


def test_token(runner, client_with_remote, mock_login):
    """Test get credential when valid credential exist."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["token", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code
    assert "username=renku\n" in result.output
    assert f"password={ACCESS_TOKEN}\n" in result.output


def test_token_non_existing_hostname(runner, client_with_remote, mock_login):
    """Test get credential for a different hostname."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["token", "--hostname", "non-existing", "get"])

    assert 0 == result.exit_code
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_no_credential(runner, client_with_remote, mock_login):
    """Test get credential when valid credential doesn't exist."""
    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    result = runner.invoke(cli, ["token", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_invalid_command(runner, client_with_remote, mock_login):
    """Test call credential helper with a command other than 'get'."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USER_CODE).exit_code

    result = runner.invoke(cli, ["token", "--hostname", ENDPOINT, "non-get-command"])

    assert 0 == result.exit_code
    assert "" == result.output
