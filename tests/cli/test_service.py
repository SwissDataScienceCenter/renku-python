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
from flaky import flaky

from renku.cli import cli
from renku.cli.service import list_renku_processes


@pytest.mark.skip(reason="Doesn't work: https://github.com/SwissDataScienceCenter/renku-python/issues/2064")
@pytest.mark.serial
@flaky(max_runs=10, min_passes=1)
def test_service_up_down(runner, svc_client_cache):
    """Check bringing service components up and down in daemon mode."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)

    assert "Using runtime directory" in result.output
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    # NOTE: Instead of booting/waiting/check/terminating, we are bringing the processes immediately
    # down and asserting the output. This would be the same as the (1), but much quicker due to lack of waiting.
    assert "Shutting down" in result.output

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}


@pytest.mark.skip(reason="Doesn't work: https://github.com/SwissDataScienceCenter/renku-python/issues/2064")
@flaky(max_runs=10, min_passes=1)
def test_service_up_restart(runner, svc_client_cache):
    """Check bringing service components up in daemon mode and restarting them."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes(include=["renku", "up"])
    pids = {p["pid"] for p in processes}
    assert pids
    assert 0 == result.exit_code

    time.sleep(1)

    result = runner.invoke(cli, ["service", "restart"], catch_exceptions=False)
    assert 0 == result.exit_code

    time.sleep(1)

    processes_after_restart = list_renku_processes(include=["renku", "up"])

    assert processes_after_restart
    assert pids != {p["pid"] for p in processes_after_restart}
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}


@pytest.mark.skip(reason="Doesn't work: https://github.com/SwissDataScienceCenter/renku-python/issues/2064")
@flaky(max_runs=10, min_passes=1)
def test_service_ps(runner, svc_client_cache):
    """Check bringing service components up and listing them."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes()
    pids = {p["pid"] for p in processes}

    result = runner.invoke(cli, ["service", "ps"], catch_exceptions=False)
    assert 0 == result.exit_code

    for pid in pids:
        assert str(pid) in result.output

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}


@pytest.mark.skip(reason="Doesn't work: https://github.com/SwissDataScienceCenter/renku-python/issues/2064")
@flaky(max_runs=20, min_passes=1)
def test_service_logs(runner, svc_client_cache):
    """Check service component logs."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes()
    pids = {p["pid"] for p in processes}

    result = runner.invoke(cli, ["service", "ps"], catch_exceptions=False)
    assert 0 == result.exit_code

    for pid in pids:
        assert str(pid) in result.output

    time.sleep(0.5)

    result = runner.invoke(cli, ["service", "logs"], catch_exceptions=False)
    assert 0 == result.exit_code
    assert result.output

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}
