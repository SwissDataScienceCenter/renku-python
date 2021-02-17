# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Test location of indirect inputs/outputs files list."""

import os
from pathlib import Path

import pytest

from renku.core import errors
from renku.core.models.cwl import command_line_tool


@pytest.mark.serial
@pytest.mark.parametrize("env_var", ["", ".", "some/path/", "./path"])
def test_set_indirect_input_files_paths_via_env_var(tmp_path, env_var, reset_environment):
    """Test setting of RENKU_INDIRECT_PATH env variable."""
    os.environ["RENKU_INDIRECT_PATH"] = env_var

    path = command_line_tool.get_indirect_inputs_path(tmp_path)

    assert path.is_absolute()

    expected_path = Path(".renku") / "tmp" / env_var / "inputs.txt"

    assert expected_path == path.relative_to(tmp_path)


@pytest.mark.serial
@pytest.mark.parametrize("env_var", ["", ".", "some/path/", "./path"])
def test_set_indirect_output_files_paths_via_env_var(tmp_path, env_var, reset_environment):
    """Test setting of RENKU_INDIRECT_PATH env variable."""
    os.environ["RENKU_INDIRECT_PATH"] = env_var

    path = command_line_tool.get_indirect_outputs_path(tmp_path)

    assert path.is_absolute()

    expected_path = Path(".renku") / "tmp" / env_var / "outputs.txt"

    assert expected_path == path.relative_to(tmp_path)


@pytest.mark.serial
@pytest.mark.parametrize("env_var", ["/absolute/path", "..", "../../outside/repo"])
def test_set_invalid_values_for_indirect_env_var(tmp_path, env_var, reset_environment):
    """Test setting invalid values for RENKU_INDIRECT_PATH env variable."""
    os.environ["RENKU_INDIRECT_PATH"] = env_var

    with pytest.raises(errors.InvalidFileOperation):
        command_line_tool.get_indirect_inputs_path(tmp_path)
