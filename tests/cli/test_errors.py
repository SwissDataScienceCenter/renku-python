# -*- coding: utf-8 -*-
#
# Copyright 2017-2019- Swiss Data Science Center (SDSC)
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
"""CLI tests."""
import sys
import tempfile

import pytest
from filelock import FileLock

from renku.cli import cli


@pytest.mark.parametrize(
    "cmd",
    [
        ["config"],
        ["dataset"],
        ["doctor"],
        ["githooks"],
        ["log"],
        ["migrate"],
        ["mv"],
        ["rerun"],
        ["rm"],
        ["run"],
        ["show"],
        ["status"],
        ["storage"],
        ["update"],
        ["workflow"],
    ],
)
def test_cli_initialization_err(cmd, runner):
    """Test correct exception raise within non-renku repository."""
    from renku.core.utils.contexts import chdir

    with tempfile.TemporaryDirectory() as tmpdir:
        with chdir(tmpdir):
            result = runner.invoke(cli, cmd)
            assert 2 == result.exit_code

            expected_output = (
                "Error: `.` is not a renku repository.\n"
                "To initialize this as a "
                "renku repository use: `renku init`\n"
            )
            assert expected_output == result.output


@pytest.mark.parametrize(
    "cmd",
    [
        # NOTE: Clone command covered through integration tests.
        ["config", "--help"],
        ["dataset", "--help"],
        ["doctor", "--help"],
        ["githooks", "--help"],
        ["log", "--help"],
        ["migrate", "--help"],
        ["mv", "--help"],
        ["rerun", "--help"],
        ["rm", "--help"],
        ["run", "--help"],
        ["show", "--help"],
        ["status", "--help"],
        ["storage", "--help"],
        ["update", "--help"],
        ["workflow", "--help"],
        ["init", "--help"],
        ["help"],
        ["--help"],
    ],
)
def test_cli_initialization_no_err_help(cmd, runner):
    """Test allowed commands within non-renku repository."""
    from renku.core.utils.contexts import chdir

    sys.argv = cmd
    with tempfile.TemporaryDirectory() as tmpdir:
        with chdir(tmpdir):
            result = runner.invoke(cli, cmd)
            assert 0 == result.exit_code


def test_file_lock_timeout_error(project, runner):
    """Test file lock timeout."""
    with FileLock(".renku.lock"):
        result = runner.invoke(cli, ["dataset", "import", "10.5281/zenodo.3715335"])

        assert "Unable to acquire lock." in result.output
