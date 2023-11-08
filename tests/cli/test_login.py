# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

from renku.core import errors
from renku.core.login import read_renku_token
from renku.core.util.contexts import chdir
from renku.ui.cli import cli
from tests.cli.fixtures.cli_gateway import ACCESS_TOKEN, ENDPOINT
from tests.utils import format_result_exception


def test_login(runner, project_with_remote, mock_login, with_injection):
    """Test login command."""
    remote_url = f"https://{ENDPOINT}/gitlab/namespace/project"
    project_with_remote.repository.remotes[0].set_url(remote_url)

    result = runner.invoke(cli, ["login", ENDPOINT], input="y")

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        assert ACCESS_TOKEN == read_renku_token("", get_endpoint_from_remote=True)
        credential = project_with_remote.repository.get_configuration().get_value("credential", "helper")
        assert f"!renku credentials --hostname {ENDPOINT}" == credential
        assert {"origin", "renku-backup-origin"} == {r.name for r in project_with_remote.repository.remotes}
        assert remote_url == project_with_remote.repository.remotes["renku-backup-origin"].url
        assert project_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")


def test_login_with_no_warn_config(runner, project_with_remote, mock_login, with_injection):
    """Test login command with ``show_login_warning`` configured in the ``renku.ini``."""
    assert 0 == runner.invoke(cli, ["config", "set", "show_login_warning", "false"]).exit_code

    result = runner.invoke(cli, ["login", ENDPOINT])  # No ``--yes`` and no input

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        assert project_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")


def test_login_with_no_git_does_not_change_remote(runner, project_with_remote, mock_login, with_injection):
    """Test login command with ``--no-git`` doesn't change project's remote."""
    result = runner.invoke(cli, ["login", "--yes", "--no-git", ENDPOINT])

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        with pytest.raises(errors.GitConfigurationError, match="Cannot get value 'credential helper'"):
            project_with_remote.repository.get_configuration().get_value("credential", "helper")
        assert {"origin"} == {r.name for r in project_with_remote.repository.remotes}
        assert not project_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")


def test_login_no_endpoint(runner, project, mock_login):
    """Test login command with no endpoint."""
    result = runner.invoke(cli, ["login"])

    assert 2 == result.exit_code
    assert "Parameter 'endpoint' is missing." in result.output


def test_login_no_endpoint_and_remote(runner, project_with_remote, mock_login):
    """Test login command with no endpoint and with project remote."""
    result = runner.invoke(cli, ["login"])

    assert 2 == result.exit_code, result.output
    assert "Parameter 'endpoint' is missing." in result.output


def test_login_invalid_endpoint(runner, project, mock_login):
    """Test login with and invalid endpoint."""
    result = runner.invoke(cli, ["login", "http: //example.com"])

    assert 2 == result.exit_code
    assert "Invalid endpoint: `http: //example.com`." in result.output


def test_login_with_config_endpoint(runner, project, mock_login):
    """Test login command with endpoint in config file."""
    assert 0 == runner.invoke(cli, ["config", "set", "endpoint", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login", "--no-git"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully logged in." in result.output


def test_logout(runner, project, mock_login, with_injection):
    """Test logout removes all credentials."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code, format_result_exception(result)

    with with_injection():
        assert read_renku_token(ENDPOINT) is None
    assert "Successfully logged out." in result.output


def test_repeated_login(runner, project, mock_login, with_injection):
    """Test multiple logins."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    with with_injection():
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)


def test_repeated_logout(runner, project, mock_login, with_injection):
    """Test multiple logouts."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    with with_injection():
        assert read_renku_token(ENDPOINT) is None


def test_login_to_multiple_endpoints(runner, project_with_remote, mock_login, with_injection):
    """Test login to multiple endpoints changes project's remote to the first endpoint."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", "--yes", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login", "--yes", second_endpoint])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Backup remote 'renku-backup-origin' already exists." in result.output
    assert "Error: Cannot create backup remote 'renku-backup-origin' for" not in result.output
    assert {"origin", "renku-backup-origin"} == {r.name for r in project_with_remote.repository.remotes}

    with with_injection():
        assert ACCESS_TOKEN == read_renku_token(ENDPOINT)
        assert second_token == read_renku_token(second_endpoint)
        assert project_with_remote.repository.remotes["origin"].url.startswith(f"https://{second_endpoint}/repo")


def test_logout_all(runner, project, mock_login, with_injection):
    """Test logout with no endpoint removes multiple credentials."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code
    assert 0 == runner.invoke(cli, ["login", "--no-git", second_endpoint]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    with with_injection():
        assert read_renku_token(ENDPOINT) is None
        assert read_renku_token(second_endpoint) is None


def test_logout_one_endpoint(runner, project, mock_login, with_injection):
    """Test logout from an endpoint removes credentials for that endpoint only."""
    second_endpoint, second_token = "second.endpoint", "second-token"
    mock_login.add_device_auth(second_endpoint, second_token)
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code
    assert 0 == runner.invoke(cli, ["login", "--no-git", second_endpoint]).exit_code

    assert 0 == runner.invoke(cli, ["logout", ENDPOINT]).exit_code

    with with_injection():
        assert read_renku_token(ENDPOINT) is None
        assert second_token == read_renku_token(second_endpoint)


def test_logout_non_existing_endpoint(runner, project, mock_login, with_injection):
    """Test logout from a non-existing endpoint does nothing."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    assert 0 == runner.invoke(cli, ["logout", "non.existing"]).exit_code

    with with_injection():
        assert read_renku_token(ENDPOINT) is not None


def test_login_git_abort(runner, project_with_remote):
    """Test login command."""
    result = runner.invoke(cli, ["login", ENDPOINT], input="n")

    assert 1 == result.exit_code
    assert "Remote URL will be changed. Do you want to continue" in result.output
    assert "Aborted!" in result.output


def test_login_non_git(runner, project, mock_login, directory_tree):
    """Test login inside a non-git directory."""
    with chdir(directory_tree):
        result = runner.invoke(cli, ["login", ENDPOINT])

    assert 0 == result.exit_code
    assert "Cannot log in to git outside a project." in result.output


def test_logout_non_git(runner, project, directory_tree):
    """Test logout inside a non-git directory."""
    with chdir(directory_tree):
        result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code
    assert "Successfully logged out." in result.output


def test_login_git_no_unique_remote(runner, project_with_remote):
    """Test login from a git directory with no clear remote."""
    project_with_remote.repository.remotes.add("second-remote", "second-remote.net")
    project_with_remote.repository.branches.add("branch-with-no-remote")
    project_with_remote.repository.checkout("branch-with-no-remote")

    result = runner.invoke(cli, ["login", "--yes", ENDPOINT])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "Cannot find a unique remote URL for project." in result.output


def test_repeated_git_login(runner, project_with_remote, mock_login):
    """Test multiple logins to git repo fails to change remote URL after first time."""
    remote_url = project_with_remote.repository.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--yes", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["login", "--yes", ENDPOINT])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Backup remote 'renku-backup-origin' already exists." in result.output
    assert "Error: Cannot create backup remote 'renku-backup-origin' for" not in result.output
    assert {"origin", "renku-backup-origin"} == {r.name for r in project_with_remote.repository.remotes}
    assert remote_url == project_with_remote.repository.remotes["renku-backup-origin"].url
    assert project_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo")
    assert not project_with_remote.repository.remotes["origin"].url.startswith(f"https://{ENDPOINT}/repo/repo")


def test_logout_git(runner, project_with_remote, mock_login):
    """Test logout removes backup remotes and restores original remote url."""
    remote_url = project_with_remote.repository.remotes[0].url

    assert 0 == runner.invoke(cli, ["login", "--yes", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert {"origin"} == {r.name for r in project_with_remote.repository.remotes}
    assert remote_url == project_with_remote.repository.remotes["origin"].url
    try:
        credential = project_with_remote.repository.get_configuration(scope="local").remove_value(
            "credential", "helper"
        )
    except errors.GitConfigurationError:  # NOTE: If already logged out, ``git config --unset`` raises an exception
        credential = None
    assert credential is None


def test_token(runner, project_with_remote, mock_login):
    """Test get credential when valid credential exist."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert f"password={ACCESS_TOKEN}\n" in result.output


def test_token_non_existing_hostname(runner, project_with_remote, mock_login):
    """Test get credential for a different hostname."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", "non-existing", "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_no_credential(runner, project_with_remote, mock_login):
    """Test get credential when valid credential doesn't exist."""
    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "get"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "username=renku\n" in result.output
    assert "password=\n" in result.output


def test_token_invalid_command(runner, project_with_remote, mock_login, with_injection):
    """Test call credential helper with a command other than 'get'."""
    assert 0 == runner.invoke(cli, ["login", "--no-git", ENDPOINT]).exit_code

    result = runner.invoke(cli, ["credentials", "--hostname", ENDPOINT, "non-get-command"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "" == result.output

    with with_injection():
        assert read_renku_token(ENDPOINT) is not None
