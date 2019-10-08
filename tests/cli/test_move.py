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
"""Test ``move`` command."""

import os

import pytest

from renku.cli import cli


def test_move_dataset_file(tmpdir, runner, client):
    """Test move of a file that belongs to a dataset."""
    # create a dataset
    result = runner.invoke(cli, ['dataset', 'create', 'testing'])
    assert 0 == result.exit_code

    source = tmpdir.join('source')
    source.write('Source file')

    result = runner.invoke(
        cli,
        ['dataset', 'add', 'testing', source.strpath],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    assert (client.path / client.datadir / 'testing' / 'source').exists()

    result = runner.invoke(cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['mv', 'data', 'files'])
    assert 0 == result.exit_code

    assert not (client.path / client.datadir / 'testing' / 'source').exists()
    assert (client.path / 'files' / 'testing' / 'source').exists()

    result = runner.invoke(cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code

    src = os.path.join('files', 'testing', 'source')
    dst = os.path.join('files', 'testing', 'newname')

    result = runner.invoke(cli, ['mv', src, dst], catch_exceptions=False)
    assert 0 == result.exit_code

    assert not (client.path / src).exists()
    assert (client.path / dst).exists()

    result = runner.invoke(cli, ['doctor'], catch_exceptions=False)
    assert 0 == result.exit_code


@pytest.mark.parametrize(
    'destination',
    (
        'destination',  # root
        os.path.join('dir', 'subdir', 'destination'),
        os.path.join('data', 'destination'),
        os.path.join('data', 'dataset', 'destination'),  # change name
        os.path.join('data', 'dataset', 'subdir', 'destination'),
    )
)
def test_move_symlinks(data_repository, runner, project, client, destination):
    """Test importing data into a dataset."""
    # create a dataset
    dataset_name = 'dataset'
    result = runner.invoke(cli, ['dataset', 'create', dataset_name])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    with client.with_dataset(dataset_name) as dataset:
        assert dataset.name == 'dataset'

    # add data from local git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', 'file',
            data_repository.working_dir
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code

    src = client.path / client.datadir / 'dataset' / 'file'
    assert src.exists()

    result = runner.invoke(
        cli, ['mv', str(src), destination], catch_exceptions=False
    )
    assert 0 == result.exit_code

    dst = client.path / destination
    assert dst.exists()
