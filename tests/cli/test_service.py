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

from pathlib import Path
from time import sleep

import pytest

from renku.ui.cli import cli
from renku.ui.cli.service import list_renku_processes
from tests.utils import format_result_exception, retry_failed


@pytest.mark.serial
@pytest.mark.redis
@pytest.mark.integration
@retry_failed
def test_service_up_down(runner, svc_client_cache, tmpdir):
    """Check bringing service components up and down in daemon mode."""
    result = runner.invoke(cli, ["service", "up", "--daemon", "--runtime-dir", str(tmpdir)], catch_exceptions=False)

    assert "Using runtime directory" in result.output
    assert 0 == result.exit_code, format_result_exception(result)

    # NOTE: Wait for processes to properly start
    sleep(5)

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    assert "Shutting down" in result.output

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}

    assert "ERROR" not in Path(tmpdir.join("renku.err")).read_text(), Path(tmpdir.join("renku.err")).read_text()
    assert "WARN" not in Path(tmpdir.join("renku.err")).read_text()


@pytest.mark.serial
@pytest.mark.redis
@pytest.mark.integration
@retry_failed
def test_service_up_restart(runner, svc_client_cache, tmpdir):
    """Check bringing service components up in daemon mode and restarting them."""
    result = runner.invoke(cli, ["service", "up", "--daemon", "--runtime-dir", str(tmpdir)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    sleep(5)

    processes = list_renku_processes(include=["renku", "up"])
    pids = {p["pid"] for p in processes}
    assert pids
    assert 0 == result.exit_code, format_result_exception(result)

    sleep(1)

    result = runner.invoke(cli, ["service", "restart"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    sleep(5)

    processes_after_restart = list_renku_processes(include=["renku", "up"])

    assert processes_after_restart
    assert pids != {p["pid"] for p in processes_after_restart}
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}

    assert "ERROR" not in Path(tmpdir.join("renku.err")).read_text(), Path(tmpdir.join("renku.err")).read_text()
    assert "WARN" not in Path(tmpdir.join("renku.err")).read_text()


@pytest.mark.serial
@pytest.mark.redis
@pytest.mark.integration
@retry_failed
def test_service_ps(runner, svc_client_cache, tmpdir):
    """Check bringing service components up and listing them."""
    result = runner.invoke(cli, ["service", "up", "--daemon", "--runtime-dir", str(tmpdir)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    sleep(5)

    processes = list_renku_processes()
    pids = {p["pid"] for p in processes}

    result = runner.invoke(cli, ["service", "ps"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    for pid in pids:
        assert str(pid) in result.output

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}

    assert "ERROR" not in Path(tmpdir.join("renku.err")).read_text(), Path(tmpdir.join("renku.err")).read_text()
    assert "WARN" not in Path(tmpdir.join("renku.err")).read_text()


@pytest.mark.serial
@pytest.mark.service
@pytest.mark.redis
@pytest.mark.integration
@retry_failed
def test_service_logs(runner, svc_client_cache, tmpdir):
    """Check service component logs."""
    result = runner.invoke(cli, ["service", "up", "--daemon", "--runtime-dir", str(tmpdir)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    sleep(5)

    processes = list_renku_processes()
    pids = {p["pid"] for p in processes}

    result = runner.invoke(cli, ["service", "ps"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    for pid in pids:
        assert str(pid) in result.output

    sleep(0.5)

    result = runner.invoke(cli, ["service", "logs"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    processes = list_renku_processes(include=["renku", "up"])
    assert not {p["pid"] for p in processes}

    assert "ERROR" not in Path(tmpdir.join("renku.err")).read_text(), Path(tmpdir.join("renku.err")).read_text()
    assert "WARN" not in Path(tmpdir.join("renku.err")).read_text()
