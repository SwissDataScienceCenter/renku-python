# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test ``log`` command."""
import os

import pytest

from renku.cli import cli
from tests.utils import format_result_exception


@pytest.mark.serial
@pytest.mark.shelled
@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_run_log_strict(runner, project, run_shell, format):
    """Test log output of run command."""
    # Run a shell command with pipe.
    result = run_shell('renku run echo "my input string" > my_output_file')

    # Assert created output file.
    result = runner.invoke(cli, ["graph", "export", "--strict", "--format={}".format(format)])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "my_output_file" in result.output
    assert "my input string" in result.output


@pytest.mark.parametrize("format", ["json-ld", "nt", "rdf"])
def test_dataset_log_strict(tmpdir, runner, project, client, format, subdirectory):
    """Test output of log for dataset add."""
    result = runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)
    paths = []
    test_paths = []
    for i in range(3):
        new_file = tmpdir.join("file_{0}".format(i))
        new_file.write(str(i))
        paths.append(str(new_file))
        test_paths.append(os.path.relpath(str(new_file), str(project)))

    # add data
    result = runner.invoke(cli, ["dataset", "add", "my-dataset"] + paths)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["graph", "export", "--strict", f"--format={format}"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert all(p in result.output for p in test_paths), result.output
