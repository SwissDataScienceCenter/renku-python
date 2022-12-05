# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Test ``service`` command."""

from unittest.mock import MagicMock

from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_session_up_down(runner, project, dummy_session_provider, monkeypatch):
    """Test starting a session."""
    import renku.core.session.session

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 2 == len(result.output.splitlines())

    for _ in range(3):
        result = runner.invoke(cli, ["session", "start", "-p", "dummy"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "successfully started" in result.output

    session_id = result.output.splitlines()[-1]

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 5 == len(result.output.splitlines())

    with monkeypatch.context() as monkey:
        browser = MagicMock()
        monkey.setattr(renku.core.session.session, "webbrowser", browser)
        result = runner.invoke(cli, ["session", "open", "-p", "dummy", session_id])
        assert 0 == result.exit_code, format_result_exception(result)
        browser.open.assert_called_once_with("http://localhost/")

    result = runner.invoke(cli, ["session", "stop", "-p", "dummy", session_id])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "has been successfully stopped" in result.output

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 4 == len(result.output.splitlines())

    result = runner.invoke(cli, ["session", "stop", "-p", "dummy", "-a"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 2 == len(result.output.splitlines())


def test_session_start_config_requests(runner, project, dummy_session_provider, monkeypatch):
    """Test session with configuration in the renku config."""
    import docker

    result = runner.invoke(cli, ["config", "set", "interactive.cpu_request", "0.5"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["config", "set", "interactive.disk_request", "100mb"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["config", "set", "interactive.mem_request", "100mb"])
    assert 0 == result.exit_code, format_result_exception(result)

    with monkeypatch.context() as monkey:
        monkey.setattr(docker, "from_env", MagicMock())
        result = runner.invoke(cli, ["session", "start", "-p", "docker"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "successfully started" in result.output
