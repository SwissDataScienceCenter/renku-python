# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Test ``remove`` command."""

import pytest

from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.domain_model.project_context import project_context
from renku.ui.cli import cli
from tests.utils import format_result_exception


@pytest.mark.parametrize("datadir_option,datadir", [([], f"{DATA_DIR}/testing"), (["--datadir", "mydir"], "mydir")])
def test_remove_dataset_file(isolated_runner, client, tmpdir, subdirectory, datadir_option, datadir):
    """Test remove of a file that belongs to a dataset."""
    runner = isolated_runner

    # create a dataset
    result = runner.invoke(cli, ["dataset", "create", "testing"] + datadir_option)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    source = tmpdir.join("remove_dataset.file")
    source.write(DATA_DIR)

    result = runner.invoke(cli, ["dataset", "add", "--copy", "testing", source.strpath])
    assert 0 == result.exit_code, format_result_exception(result)

    path = project_context.path / datadir / "remove_dataset.file"
    assert path.exists()

    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["rm", str(project_context.path / datadir)])
    assert 0 == result.exit_code, format_result_exception(result)

    assert not path.exists()

    result = runner.invoke(cli, ["doctor"])
    assert 0 == result.exit_code, format_result_exception(result)
