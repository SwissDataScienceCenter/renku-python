# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Test dataset command."""

from __future__ import absolute_import, print_function

import json
import os

import git
import pytest

from renku import cli
from renku._compat import Path
from renku.cli._format.dataset_files import FORMATS as DATASET_FILES_FORMATS
from renku.cli._format.datasets import FORMATS as DATASETS_FORMATS


def test_datasets_import(data_file, data_repository, runner, project, client):
    """Test importing data into a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset',
         str(data_file)],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert os.stat(
        os.path.join('data', 'dataset', os.path.basename(str(data_file)))
    )

    # add data from a git repo via http
    result = runner.invoke(
        cli.cli,
        [
            'dataset', 'add', 'dataset', '--target', 'README.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert os.stat('data/dataset/README.rst')

    # add data from local git repo
    result = runner.invoke(
        cli.cli, [
            'dataset', 'add', 'dataset', '-t', 'file2', '-t', 'file3',
            os.path.dirname(data_repository.git_dir)
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code


@pytest.mark.parametrize('output_format', DATASETS_FORMATS.keys())
def test_datasets_list_empty(output_format, runner, project):
    """Test listing without datasets."""
    format_option = '--format={0}'.format(output_format)
    result = runner.invoke(cli.cli, ['dataset', format_option])
    assert 0 == result.exit_code


@pytest.mark.parametrize('output_format', DATASETS_FORMATS.keys())
def test_datasets_list_non_empty(output_format, runner, project):
    """Test listing with datasets."""
    format_option = '--format={0}'.format(output_format)
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code

    result = runner.invoke(cli.cli, ['dataset', format_option])
    assert 0 == result.exit_code
    assert 'dataset' in result.output

    result = runner.invoke(
        cli.cli, ['dataset', '--revision=HEAD~1', format_option]
    )
    assert result.exit_code == 0
    assert 'dataset' not in result.output


def test_multiple_file_to_dataset(
    tmpdir, data_repository, runner, project, client
):
    """Test importing multiple data into a dataset at once."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    paths = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset'] + paths,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code


def test_repository_file_to_dataset(runner, project, client):
    """Test adding a file from the repository into a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code

    with (client.path / 'a').open('w') as fp:
        fp.write('a')

    client.repo.git.add('a')
    client.repo.git.commit(message='Added file a')

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset', 'a'],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'
        assert Path('../../../a') in dataset.files


def test_relative_import_to_dataset(
    tmpdir, data_repository, runner, project, client
):
    """Test importing data from a directory structure."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    zero_data = tmpdir.join('data.txt')
    zero_data.write('zero')

    first_level = tmpdir.mkdir('first')
    second_level = first_level.mkdir('second')

    first_data = first_level.join('data.txt')
    first_data.write('first')

    second_data = second_level.join('data.txt')
    second_data.write('second')

    paths = [str(zero_data), str(first_data), str(second_data)]

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset', '--relative-to',
         str(tmpdir)] + paths,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    assert os.stat(os.path.join('data', 'dataset', 'data.txt'))
    assert os.stat(os.path.join('data', 'dataset', 'first', 'data.txt'))
    assert os.stat(
        os.path.join('data', 'dataset', 'first', 'second', 'data.txt')
    )


def test_relative_git_import_to_dataset(tmpdir, runner, project, client):
    """Test relative import from a git repository."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    data_repo = git.Repo.init(str(tmpdir))

    zero_data = tmpdir.join('data.txt')
    zero_data.write('zero')

    first_level = tmpdir.mkdir('first')
    second_level = first_level.mkdir('second')

    first_data = first_level.join('data.txt')
    first_data.write('first')

    second_data = second_level.join('data.txt')
    second_data.write('second')

    paths = [str(zero_data), str(first_data), str(second_data)]
    data_repo.index.add(paths)
    data_repo.index.commit('Added source files')

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        [
            'dataset', 'add', 'dataset', '--relative-to',
            str(first_level),
            str(tmpdir)
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    assert os.stat(os.path.join('data', 'dataset', 'data.txt'))
    assert os.stat(os.path.join('data', 'dataset', 'second', 'data.txt'))

    # add data in subdirectory
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'relative', '--relative-to', 'first',
         str(tmpdir)],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    assert os.stat(os.path.join('data', 'relative', 'data.txt'))
    assert os.stat(os.path.join('data', 'relative', 'second', 'data.txt'))


def test_dataset_add_with_link(tmpdir, runner, project, client):
    """Test adding data to dataset with --link flag."""
    import stat

    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code

    paths = []
    expected_inodes = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        expected_inodes.append(os.lstat(str(new_file))[stat.ST_INO])
        paths.append(str(new_file))

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset', '--link'] + paths,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    received_inodes = []
    with client.with_dataset('my-dataset') as dataset:
        assert dataset.name == 'my-dataset'
        for relative, file_ in dataset.files.items():
            path_ = (client.renku_datasets_path / 'my-dataset' /
                     file_.path).resolve()
            received_inodes.append(os.lstat(str(path_))[stat.ST_INO])

    # check that original and dataset inodes are the same
    assert sorted(expected_inodes) == sorted(received_inodes)


def test_dataset_add_with_copy(tmpdir, runner, project, client):
    """Test adding data to dataset with copy."""
    import os
    import stat

    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code

    paths = []
    original_inodes = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        original_inodes.append(os.lstat(str(new_file))[stat.ST_INO])
        paths.append(str(new_file))

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
    )
    assert 0 == result.exit_code

    received_inodes = []
    with client.with_dataset('my-dataset') as dataset:
        assert dataset.name == 'my-dataset'

        for relative, file_ in dataset.files.items():
            path_ = (client.renku_datasets_path / 'my-dataset' /
                     file_.path).resolve()
            received_inodes.append(os.lstat(str(path_))[stat.ST_INO])

    # check that original inodes are within created ones
    for inode in received_inodes:
        assert inode not in original_inodes


def test_datasets_ls_files_tabular_empty(runner, project):
    """Test listing of data within empty dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # list all files in dataset
    result = runner.invoke(cli.cli, ['dataset', 'ls-files', 'my-dataset'])
    assert result.exit_code == 0

    # check output
    output = result.output.split('\n')
    assert output.pop(0).split() == ['ADDED', 'AUTHORS', 'DATASET', 'PATH']
    assert set(output.pop(0)) == {' ', '-'}
    assert output.pop(0) == ''
    assert not output


@pytest.mark.parametrize('output_format', DATASET_FILES_FORMATS.keys())
def test_datasets_ls_files_check_exit_code(output_format, runner, project):
    """Test file listing exit codes for different formats."""
    format_option = '--format={0}'.format(output_format)
    result = runner.invoke(cli.cli, ['dataset', 'ls-files', format_option])
    assert result.exit_code == 0


def test_datasets_ls_files_tabular_dataset_filter(tmpdir, runner, project):
    """Test listing of data within dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create some data
    paths = []
    created_files = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))
        created_files.append(new_file.basename)

    # add data to dataset
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # list all files in non empty dataset
    result = runner.invoke(cli.cli, ['dataset', 'ls-files', 'my-dataset'])
    assert result.exit_code == 0

    # check output from ls-files command
    output = result.output.split('\n')
    assert output.pop(0).split() == ['ADDED', 'AUTHORS', 'DATASET', 'PATH']
    assert set(output.pop(0)) == {' ', '-'}

    # check listing
    added_at = []
    for i in range(3):
        row = output.pop(0).split(' ')
        assert Path(row.pop()).name in created_files
        added_at.append(row.pop(0))

    # check if sorted by added_at
    assert added_at == sorted(added_at)


def test_datasets_ls_files_tabular_patterns(tmpdir, runner, project):
    """Test listing of data within dataset with include/exclude filters."""

    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create some data
    subdir = tmpdir.mkdir('sub')
    paths = [str(subdir)]
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

        sub_file = subdir.join('sub_file_{0}'.format(i))
        sub_file.write(str(i))

    # add data to dataset
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # check include / exclude filters
    result = runner.invoke(
        cli.cli,
        ['dataset', 'ls-files', '--include=**/file*', '--exclude=**/file_2']
    )
    assert result.exit_code == 0

    # check output
    assert 'file_0' in result.output
    assert 'file_1' in result.output
    assert 'file_2' not in result.output

    # check directory pattern
    result = runner.invoke(
        cli.cli, ['dataset', 'ls-files', '--include=**/sub/*']
    )
    assert result.exit_code == 0

    # check output
    assert 'sub_file_0' in result.output
    assert 'sub_file_1' in result.output
    assert 'sub_file_2' in result.output


def test_datasets_ls_files_tabular_authors(tmpdir, runner, project, client):
    """Test listing of data within dataset with authors filters."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create some data
    paths = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data to dataset
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    authors = None
    with client.with_dataset(name='my-dataset') as dataset:
        authors = dataset.authors_csv

    # check include / exclude filters
    result = runner.invoke(
        cli.cli, ['dataset', 'ls-files', '--authors={0}'.format(authors)]
    )
    assert result.exit_code == 0

    # check output
    for file_ in paths:
        assert Path(file_).name in result.output


def test_datasets_ls_files_correct_paths(tmpdir, runner, project):
    """Test listing of data within dataset and check that paths are correct."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create some data
    paths = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data to dataset
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # check include / exclude filters
    result = runner.invoke(
        cli.cli, ['dataset', 'ls-files', '--format=json-ld']
    )
    assert result.exit_code == 0

    output = json.loads(result.output)
    for record in output:
        assert Path(record['url']).exists()


def test_dataset_unlink_file_not_found(runner, project):
    """Test unlinking of file from dataset with no files found."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    result = runner.invoke(
        cli.cli,
        ['dataset', 'unlink', 'my-dataset', '--include', 'notthere.csv']
    )
    assert result.exit_code == 0
    assert '' == result.output


def test_dataset_unlink_file_abort_unlinking(tmpdir, runner, project):
    """Test unlinking of file from dataset and aborting."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert result.exit_code == 0

    # unlink file from dataset
    result = runner.invoke(
        cli.cli,
        ['dataset', 'unlink', 'my-dataset', '--include', new_file.basename],
        input='n'
    )
    assert result.exit_code == 1

    # check output
    assert 'Aborted!' in result.output


def test_dataset_unlink_file(tmpdir, runner, client):
    """Test unlinking of file and check removal from dataset"""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert result.exit_code == 0

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert result.exit_code == 0

    with client.with_dataset(name='my-dataset') as dataset:
        assert new_file.basename in [
            file_.path.name for file_ in dataset.files.values()
        ]

    result = runner.invoke(
        cli.cli, [
            'dataset', 'unlink', 'my-dataset', '--include', new_file.basename,
            '-y'
        ]
    )
    assert result.exit_code == 0

    with client.with_dataset(name='my-dataset') as dataset:
        assert new_file.basename not in [
            file_.path.name for file_ in dataset.files.values()
        ]
