# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Test ``session`` commands."""

import re
from unittest.mock import patch

import click
import pytest

from renku.core.errors import ParameterError
from renku.core.plugin.session import get_supported_session_providers
from renku.core.session.docker import DockerSessionProvider
from renku.core.session.renkulab import RenkulabSessionProvider
from renku.core.session.session import session_list, session_start, session_stop, ssh_setup


def fake_start(
    self,
    image_name,
    project_name,
    config,
    cpu_request,
    mem_request,
    disk_request,
    gpu_request,
    **kwargs,
):
    return "0xdeadbeef", ""


def fake_stop(self, project_name, session_name, stop_all):
    if session_name == "missing_session":
        return False
    return True


def fake_find_image(self, image_name, config):
    if image_name == "missing_image":
        return False
    return True


def fake_build_image(self, image_descriptor, image_name, config):
    return


def fake_session_list(self, project_name, config):
    return ["0xdeadbeef"]


def fake_pre_start_checks(self, **kwargs):
    pass


@pytest.mark.parametrize(
    "provider_name,session_provider,provider_patches",
    [
        ("docker", DockerSessionProvider, {}),
        ("renkulab", RenkulabSessionProvider, {}),
    ],
)
@pytest.mark.parametrize(
    "parameters,result",
    [
        ({}, "0xdeadbeef"),
        ({"image_name": "fixed_image"}, "0xdeadbeef"),
        ({"image_name": "missing_image"}, ParameterError),
    ],
)
def test_session_start(
    run_shell,
    project,
    provider_name,
    session_provider,
    provider_patches,
    parameters,
    result,
    with_injection,
    mock_communication,
):
    with patch.multiple(
        session_provider,
        session_start=fake_start,
        find_image=fake_find_image,
        build_image=fake_build_image,
        pre_start_checks=fake_pre_start_checks,
        **provider_patches,
    ):
        provider_implementation = next(
            filter(lambda x: x.name == provider_name, get_supported_session_providers()), None
        )
        assert provider_implementation is not None

        with with_injection():
            if not isinstance(result, str) and issubclass(result, Exception):
                with pytest.raises(result):
                    session_start(provider=provider_name, config_path=None, **parameters)
            else:
                session_start(provider=provider_name, config_path=None, **parameters)
                assert result in mock_communication.stdout_lines


@pytest.mark.parametrize(
    "provider_name,session_provider,provider_patches",
    [
        ("docker", DockerSessionProvider, {}),
        ("renkulab", RenkulabSessionProvider, {}),
    ],
)
@pytest.mark.parametrize(
    "parameters,result",
    [
        ({"session_name": "0xdeadbeef"}, None),
        ({"session_name": "0xdeadbeef", "stop_all": True}, None),
        ({"session_name": "missing_session"}, ParameterError),
    ],
)
def test_session_stop(
    run_shell,
    project,
    session_provider,
    provider_name,
    parameters,
    provider_patches,
    result,
    with_injection,
):
    with patch.multiple(session_provider, session_stop=fake_stop, **provider_patches):
        provider_implementation = next(
            filter(lambda x: x.name == provider_name, get_supported_session_providers()), None
        )
        assert provider_implementation is not None

        with with_injection():
            if result is not None and issubclass(result, Exception):
                with pytest.raises(result):
                    session_stop(provider=provider_name, **parameters)
            else:
                session_stop(provider=provider_name, **parameters)


@pytest.mark.parametrize(
    "provider_name,session_provider,provider_patches",
    [
        ("docker", DockerSessionProvider, {}),
        ("renkulab", RenkulabSessionProvider, {}),
    ],
)
@pytest.mark.parametrize("provider_exists,result", [(True, ["0xdeadbeef"]), (False, ParameterError)])
def test_session_list(
    project,
    provider_name,
    session_provider,
    provider_patches,
    provider_exists,
    result,
    with_injection,
):
    with patch.multiple(session_provider, session_list=fake_session_list, **provider_patches):
        with with_injection():
            provider = provider_name if provider_exists else "no_provider"

            if not isinstance(result, list) and issubclass(result, Exception):
                with pytest.raises(result):
                    session_list(provider=provider, config_path=None)
            else:
                sessions, _, _ = session_list(provider=provider, config_path=None)
                assert sessions == result


def test_session_setup_ssh(project, with_injection, fake_home, mock_communication):
    """Test setting up SSH config for a deployment."""
    with with_injection():
        ssh_setup()

    ssh_home = fake_home / ".ssh"
    renku_ssh_path = ssh_home / "renku"
    assert renku_ssh_path.exists()
    assert re.search(r"Include .*/\.ssh/renku/\*\.conf", (ssh_home / "config").read_text())
    assert (renku_ssh_path / "99-None-jumphost.conf").exists()
    assert (renku_ssh_path / "None-key").exists()
    assert (renku_ssh_path / "None-key.pub").exists()
    assert len(mock_communication.confirm_calls) == 0

    key = (renku_ssh_path / "None-key").read_text()

    with with_injection():
        with pytest.raises(click.Abort):
            ssh_setup()

    assert len(mock_communication.confirm_calls) == 1
    assert key == (renku_ssh_path / "None-key").read_text()

    with with_injection():
        ssh_setup(force=True)

    assert key != (renku_ssh_path / "None-key").read_text()


def test_session_start_ssh(project, with_injection, mock_communication, fake_home):
    def _fake_send_request(self, req_type: str, *args, **kwargs):
        class _FakeResponse:
            status_code = 200

            def json(self):
                return {"name": "0xdeadbeef"}

        return _FakeResponse()

    with patch.multiple(
        RenkulabSessionProvider,
        find_image=fake_find_image,
        build_image=fake_build_image,
        _wait_for_session_status=lambda _, __, ___: None,
        _send_renku_request=_fake_send_request,
        _remote_head_hexsha=lambda _: project.repository.head.commit.hexsha,
        _renku_url=lambda _: "example.com",
        _cleanup_ssh_connection_configs=lambda _, __: None,
        _auth_header=lambda _: None,
    ):
        provider_implementation = next(filter(lambda x: x.name == "renkulab", get_supported_session_providers()), None)
        assert provider_implementation is not None

        with with_injection():
            ssh_setup()
            session_start(provider="renkulab", config_path=None, ssh=True)

        assert any("0xdeadbeef" in line for line in mock_communication.stdout_lines)
        ssh_home = fake_home / ".ssh"
        renku_ssh_path = ssh_home / "renku"
        assert (renku_ssh_path / "99-None-jumphost.conf").exists()
        assert (project.path / ".ssh" / "authorized_keys").exists()
        assert len(list(renku_ssh_path.glob("00-*-0xdeadbeef.conf"))) == 1
