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

from unittest.mock import patch

import pytest

from renku.core.errors import ParameterError
from renku.core.plugin.session import supported_session_providers
from renku.core.session.docker import DockerSessionProvider
from renku.core.session.session import session_list, session_start, session_stop


def fake_start(
    self,
    image_name,
    project_name,
    config,
    client,
    cpu_request,
    mem_request,
    disk_request,
    gpu_request,
):
    return "0xdeadbeef"


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


@pytest.mark.parametrize("provider", ["docker"])
@pytest.mark.parametrize(
    "parameters,result",
    [
        ({}, "0xdeadbeef"),
        ({"image_name": "fixed_image"}, "0xdeadbeef"),
        ({"image_name": "missing_image"}, ParameterError),
    ],
)
@patch.multiple(
    DockerSessionProvider, session_start=fake_start, find_image=fake_find_image, build_image=fake_build_image
)
def test_session_start(run_shell, client, provider, parameters, result, client_database_injection_manager):
    provider_implementation = next(filter(lambda x: x[1] == provider, supported_session_providers()), None)
    assert provider_implementation is not None

    with client_database_injection_manager(client):
        if not isinstance(result, str) and issubclass(result, Exception):
            with pytest.raises(result):
                session_start(provider=provider, config_path=None, **parameters)
        else:
            assert session_start(provider=provider, config_path=None, **parameters) == result


@pytest.mark.parametrize("provider", ["docker"])
@pytest.mark.parametrize(
    "parameters,result",
    [
        ({"session_name": "0xdeadbeef"}, None),
        ({"session_name": "0xdeadbeef", "stop_all": True}, None),
        ({"session_name": "missing_session"}, ParameterError),
    ],
)
@patch.object(DockerSessionProvider, "session_stop", fake_stop)
def test_session_stop(run_shell, client, provider, parameters, result, client_database_injection_manager):
    provider_implementation = next(filter(lambda x: x[1] == provider, supported_session_providers()), None)
    assert provider_implementation is not None

    with client_database_injection_manager(client):
        if result is not None and issubclass(result, Exception):
            with pytest.raises(result):
                session_stop(provider=provider, **parameters)
        else:
            session_stop(provider=provider, **parameters)


@pytest.mark.parametrize("provider,result", [("docker", ["0xdeadbeef"]), ("no_provider", ParameterError)])
@patch.object(DockerSessionProvider, "session_list", fake_session_list)
def test_session_list(run_shell, client, provider, result, client_database_injection_manager):
    with client_database_injection_manager(client):
        if not isinstance(result, list) and issubclass(result, Exception):
            with pytest.raises(result):
                session_list(provider=provider, config_path=None)
        else:
            assert session_list(provider=provider, config_path=None) == result
