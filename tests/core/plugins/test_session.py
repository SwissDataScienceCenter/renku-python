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
from renku.core.plugin.session import get_supported_session_providers
from renku.core.session.docker import DockerSessionProvider
from renku.core.session.renkulab import RenkulabSessionProvider
from renku.core.session.session import session_list, session_start, session_stop


def fake_start(
    self,
    image_name,
    project_name,
    config,
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


def fake_pre_start_checks(self):
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
    client,
    provider_name,
    session_provider,
    provider_patches,
    parameters,
    result,
    client_database_injection_manager,
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
            filter(lambda x: x.get_name() == provider_name, get_supported_session_providers()), None
        )
        assert provider_implementation is not None

        with client_database_injection_manager(client):
            if not isinstance(result, str) and issubclass(result, Exception):
                with pytest.raises(result):
                    session_start(provider=provider_name, config_path=None, **parameters)
            else:
                assert session_start(provider=provider_name, config_path=None, **parameters) == result


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
    client,
    session_provider,
    provider_name,
    parameters,
    provider_patches,
    result,
    client_database_injection_manager,
):
    with patch.multiple(session_provider, session_stop=fake_stop, **provider_patches):
        provider_implementation = next(
            filter(lambda x: x.get_name() == provider_name, get_supported_session_providers()), None
        )
        assert provider_implementation is not None

        with client_database_injection_manager(client):
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
    run_shell,
    client,
    provider_name,
    session_provider,
    provider_patches,
    provider_exists,
    result,
    client_database_injection_manager,
):
    with patch.multiple(session_provider, session_list=fake_session_list, **provider_patches):
        with client_database_injection_manager(client):
            if not isinstance(result, list) and issubclass(result, Exception):
                with pytest.raises(result):
                    session_list(provider=provider_name if provider_exists else "no_provider", config_path=None)
            else:
                assert (
                    session_list(provider=provider_name if provider_exists else "no_provider", config_path=None)
                    == result
                )
