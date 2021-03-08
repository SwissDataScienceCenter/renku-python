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

SVC_COMPONENTS_EXPECTED_BOOT_TIME = 5


@pytest.mark.serial
@flaky(max_runs=10, min_passes=1)
def test_service_up_down(runner):
    """Check bringing service components up and down in daemon mode."""
    result = runner.invoke(cli, ["service", "up", "--daemon"], catch_exceptions=False)

    assert "Using runtime directory" in result.output
    assert 0 == result.exit_code

    # NOTE: Booting up all processes can take few seconds.
    time.sleep(SVC_COMPONENTS_EXPECTED_BOOT_TIME)

    processes = list_renku_processes(include=["renku", "up"])
    cmd_lines = set([p["cmdline"] for p in processes])
    assert cmd_lines

    result = runner.invoke(cli, ["service", "down"], catch_exceptions=False)
    assert 0 == result.exit_code
