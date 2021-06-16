# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku common configurations."""
import subprocess
import time

import pytest


@pytest.fixture()
def run_shell():
    """Create a shell cmd runner."""

    def run_(cmd, return_ps=None, sleep_for=None):
        """Spawn subprocess and execute shell command.

        :param return_ps: Return process object.
        :param sleep_for: After executing command sleep for n seconds.
        :returns: Process object or tuple (stdout, stderr).
        """
        ps = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if return_ps:
            return ps

        output = ps.communicate()

        if sleep_for:
            time.sleep(sleep_for)

        return output

    return run_


@pytest.fixture()
def runner():
    """Create a runner on isolated filesystem."""
    from click.testing import CliRunner

    return CliRunner()


@pytest.fixture()
def run(runner, capsys):
    """Return a callable runner."""
    from renku.cli import cli
    from renku.core.utils.contexts import Isolation

    def generate(args=("update", "--all"), cwd=None, **streams):
        """Generate an output."""
        with capsys.disabled(), Isolation(cwd=cwd, **streams):
            try:
                cli.main(args=args, prog_name=runner.get_default_prog_name(cli))
                return 0
            except SystemExit as e:
                return 0 if e.code is None else e.code
            except Exception:
                raise

    return generate


@pytest.fixture()
def isolated_runner():
    """Create a runner on isolated filesystem."""
    from click.testing import CliRunner

    runner_ = CliRunner()
    with runner_.isolated_filesystem():
        yield runner_
