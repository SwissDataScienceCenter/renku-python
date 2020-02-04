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
import tempfile

from renku.cli import cli


def test_cli_initialization_err(runner):
    """Check correct user warning within non-renku repository."""
    from renku.core.utils.contexts import chdir

    with tempfile.TemporaryDirectory() as tmpdir:
        with chdir(tmpdir):
            result = runner.invoke(cli, ['config', 'key', 'local-value'])
            assert 2 == result.exit_code

            expected_output = (
                'Warning: `.` is not a renku initialized repository.\n'
                'Error: To initialize this as a renku managed repository use: '
                '`renku init`\n'
            )
            assert expected_output == result.output


def test_cli_initialization_no_err_help(runner):
    """Check correct user warning within non-renku repository."""
    from renku.core.utils.contexts import chdir

    with tempfile.TemporaryDirectory() as tmpdir:
        with chdir(tmpdir):
            result = runner.invoke(cli, ['--help'])
            assert 0 == result.exit_code
