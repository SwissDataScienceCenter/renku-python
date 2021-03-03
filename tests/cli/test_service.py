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
import time

import pytest

from renku.cli import cli
from renku.cli.service import list_renku_processes

SVC_COMPONENTS_EXPECTED_BOOT_TIME = 10


@pytest.mark.serial
def test_service_up_down(runner):
    """Check bringing service components up and down in daemon mode."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)

    assert "Using runtime directory" in result.output
    assert 0 == result.exit_code

    # NOTE: Booting up all processes can take few seconds.
    time.sleep(SVC_COMPONENTS_EXPECTED_BOOT_TIME)

    processes = list_renku_processes(include=["renku", "up"])
    cmdlines = set([p["cmdline"] for p in processes])
    assert 4 == len(cmdlines)

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    # NOTE: Booting down all processes can take up to a second.
    time.sleep(5)

    processes = list_renku_processes(include=["renku", "up"])
    assert 0 == len(processes)


def test_service_up_restart(runner):
    """Check bringing service components up in daemon mode and restarting them."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)

    assert "Using runtime directory" in result.output
    assert 0 == result.exit_code

    # NOTE: Booting up all processes can take few seconds.
    time.sleep(SVC_COMPONENTS_EXPECTED_BOOT_TIME)

    processes = list_renku_processes(include=["renku", "up"])
    cmdlines = set([p["cmdline"] for p in processes])

    assert 4 == len(cmdlines)

    result = runner.invoke(cli, ["service", "restart"], catch_exceptions=False)
    assert 0 == result.exit_code

    # NOTE: Restart all processes can take up to a second.
    time.sleep(5)

    processes_after_restart = list_renku_processes(include=["renku", "up"])
    assert 4 == len(processes_after_restart)
    assert [p["pid"] for p in processes] != [p["pid"] for p in processes_after_restart]

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    # NOTE: Booting down all processes can take up to a second.
    time.sleep(5)

    processes = list_renku_processes(include=["renku", "up"])
    assert 0 == len(processes)
