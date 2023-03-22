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

import re
from unittest.mock import MagicMock, patch

from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_session_up_down(runner, project, dummy_session_provider, monkeypatch):
    """Test starting a session."""
    browser = dummy_session_provider

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 2 == len(result.output.splitlines())

    for _ in range(3):
        result = runner.invoke(cli, ["session", "start", "-p", "dummy"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "session-random-" in result.output

    session_id = re.findall(r".*(session-random-.*-name).*", result.output, re.MULTILINE)[0]

    result = runner.invoke(cli, ["session", "ls", "-p", "dummy"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert 5 == len(result.output.splitlines())

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
        docker_mock = MagicMock()
        docker_mock.api.inspect_image.return_value = {}
        monkey.setattr(docker, "from_env", lambda: docker_mock)
        result = runner.invoke(cli, ["session", "start", "-p", "docker"], input="y\n")
        assert 0 == result.exit_code, format_result_exception(result)
        assert "successfully started" in result.output


def test_session_ssh_setup(runner, project, dummy_session_provider, fake_home):
    """Test starting a session."""
    from renku.core.util.ssh import generate_ssh_keys

    with patch("renku.core.util.ssh.get_renku_url", lambda: "https://renkulab.io/"):
        result = runner.invoke(cli, ["session", "ssh-setup"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Generating keys" in result.output
    assert "Writing SSH config" in result.output

    private_key, public_key = generate_ssh_keys()

    private_path = fake_home / ".ssh" / "existing"
    private_path.write_text(private_key)
    (fake_home / ".ssh" / "existing.pub").write_text(public_key)

    with patch("renku.core.util.ssh.get_renku_url", lambda: "https://renkulab.io/"):
        result = runner.invoke(cli, ["session", "ssh-setup", "-k", str(private_path), "--force"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Linking existing keys" in result.output
    assert "Writing SSH config" in result.output
