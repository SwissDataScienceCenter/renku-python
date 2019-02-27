# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Test move command."""

import os

from renku import cli


def test_move_dataset_file(tmpdir, runner, client):
    """Test move of a file that belongs to a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'testing'])
    assert 0 == result.exit_code

    source = tmpdir.join('source')
    source.write('Source file')

    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'testing', source.strpath],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    assert (client.path / client.datadir / 'testing' / 'source').exists()

    result = runner.invoke(cli.cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['mv', 'data', 'files'])
    assert 0 == result.exit_code

    assert not (client.path / client.datadir / 'testing' / 'source').exists()
    assert (client.path / 'files' / 'testing' / 'source').exists()

    result = runner.invoke(cli.cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code

    src = os.path.join('files', 'testing', 'source')
    dst = os.path.join('files', 'testing', 'newname')

    result = runner.invoke(cli.cli, ['mv', src, dst], catch_exceptions=False)
    assert 0 == result.exit_code

    assert not (client.path / src).exists()
    assert (client.path / dst).exists()

    result = runner.invoke(cli.cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code
