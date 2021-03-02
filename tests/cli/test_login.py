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

from renku.cli import cli
from renku.core.commands.login import read_renku_token

ENDPOINT = "renku.deployment.ch"
USE_CODE = "valid_user_code"


def test_login(runner, client, mock_login):
    """Test login command."""
    result = runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE)

    assert 0 == result.exit_code
    assert "jwt-token" == read_renku_token(client, ENDPOINT)


def test_login_no_endpoint(runner, client, mock_login):
    """Test login command with no endpoint."""
    result = runner.invoke(cli, ["login"])

    assert 2 == result.exit_code
    assert "Parameter 'endpoint' is missing." in result.output


def test_login_invalid_endpoint(runner, client, mock_login):
    """Test login with and invalid endpoint."""
    result = runner.invoke(cli, ["login", "http: //example.com"])

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

    result = runner.invoke(cli, ["login"], input=USE_CODE)

    assert 0 == result.exit_code
    assert "Successfully logged in." in result.output


def test_logout(runner, client, mock_login):
    """Test logout removes all credentials."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code

    result = runner.invoke(cli, ["logout"])

    assert 0 == result.exit_code
    assert read_renku_token(client, ENDPOINT) is None
    assert "Successfully logged out." in result.output


def test_repeated_login(runner, client, mock_login):
    """Test multiple logins."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code
    assert "jwt-token" == read_renku_token(client, ENDPOINT)


def test_repeated_logout(runner, client, mock_login):
    """Test multiple logouts."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code
    assert read_renku_token(client, ENDPOINT) is None


def test_login_to_multiple_endpoints(runner, client, mock_login):
    """Test login to multiple endpoints."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["login", "other.deployment"], input=USE_CODE).exit_code

    assert "jwt-token" == read_renku_token(client, ENDPOINT)
    assert "other-token" == read_renku_token(client, "other.deployment")


def test_logout_all(runner, client, mock_login):
    """Test logout with no endpoint removes multiple credentials."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code
    assert 0 == runner.invoke(cli, ["login", "other.deployment"], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout"]).exit_code

    assert read_renku_token(client, ENDPOINT) is None
    assert read_renku_token(client, "other.deployment") is None


def test_logout_one_endpoint(runner, client, mock_login):
    """Test logout from an endpoint removes credentials for that endpoint only."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code
    assert 0 == runner.invoke(cli, ["login", "other.deployment"], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout", ENDPOINT]).exit_code

    assert read_renku_token(client, ENDPOINT) is None
    assert read_renku_token(client, "other.deployment") is not None


def test_logout_non_existing_endpoint(runner, client, mock_login):
    """Test logout from a non-existing endpoint does nothing."""
    assert 0 == runner.invoke(cli, ["login", ENDPOINT], input=USE_CODE).exit_code

    assert 0 == runner.invoke(cli, ["logout", "non.existing"]).exit_code

    assert read_renku_token(client, ENDPOINT) is not None
