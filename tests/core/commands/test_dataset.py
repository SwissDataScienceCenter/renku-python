# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Dataset tests."""
import os
import shutil
import stat
from pathlib import Path

import pytest
from git import Repo
from tests.utils import raises

from renku.core import errors
from renku.core.commands.dataset import add_file, create_dataset, \
    file_unlink, list_datasets, list_files
from renku.core.errors import ParameterError
from renku.core.models.datasets import Dataset, DatasetFile
from renku.core.models.provenance.agents import Person
from renku.core.utils.contexts import chdir


@pytest.mark.parametrize(
    'scheme, path, overwrite, error',
    [('', 'temp', False, None), ('file://', 'temp', True, None),
     ('', 'tempp', False, errors.ParameterError),
     ('http://', 'example.com/file', False, None),
     ('https://', 'example.com/file', True, None),
     ('bla://', 'file', False, errors.UrlSchemeNotSupported)]
)
def test_data_add(
    scheme, path, overwrite, error, client, data_file, directory_tree,
    dataset_responses
):
    """Test data import."""
    with raises(error):
        if path == 'temp':
            path = str(data_file)
        elif path == 'tempdir':
            path = str(directory_tree)

        with client.with_dataset('dataset', create=True) as d:
            d.creator = [{
                'name': 'me',
                'email': 'me@example.com',
                'identifier': 'me_id'
            }]

            client.add_data_to_dataset(
                d, ['{}{}'.format(scheme, path)], overwrite=overwrite
            )

        with open('data/dataset/file') as f:
            assert f.read() == '1234'

        assert d.find_file('data/dataset/file')

        # check that the imported file is read-only
        assert not os.access(
            'data/dataset/file', stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        )

        # check the linking
        if scheme in ('', 'file://'):
            shutil.rmtree('./data/dataset')
            with client.with_dataset('dataset') as d:
                d.creator = [{
                    'name': 'me',
                    'email': 'me@example.com',
                    'identifier': 'me_id'
                }]
                client.add_data_to_dataset(
                    d, ['{}{}'.format(scheme, path)], overwrite=True
                )
            assert os.path.exists('data/dataset/file')


def test_data_add_recursive(directory_tree, client):
    """Test recursive data imports."""
    with client.with_dataset('dataset', create=True) as dataset:
        dataset.creator = [{
            'name': 'me',
            'email': 'me@example.com',
            'identifier': 'me_id'
        }]
        client.add_data_to_dataset(
            dataset, [directory_tree.join('dir2').strpath]
        )

        assert os.path.basename(
            os.path.dirname(dataset.files[0].path)
        ) == 'dir2'


def test_git_repo_import(client, dataset, tmpdir, data_repository):
    """Test an import from a git repository."""
    # add data from local repo
    client.add_data_to_dataset(
        dataset,
        [os.path.join(os.path.dirname(data_repository.git_dir), 'dir2')]
    )
    assert os.stat('data/dataset/dir2/file2')
    assert dataset.files[0].path.endswith('dir2/file2')


@pytest.mark.parametrize(
    'creators', [
        [Person(name='me', email='me@example.com')],
        [{
            'name': 'me',
            'email': 'me@example.com',
        }],
    ]
)
def test_creator_parse(creators, data_file):
    """Test that different options for specifying creators work."""
    f = DatasetFile(path='file', creator=creators)
    creator = Person(name='me', email='me@example.com')
    assert creator in f.creator

    # email check
    with pytest.raises(ValueError):
        Person(name='me', email='meexample.com')

    # creators must be a set or list of dicts or Person
    with pytest.raises(ValueError):
        f = DatasetFile(path='file', creator=['name'])


def test_dataset_serialization(dataset):
    """Test dataset (de)serialization."""
    dataset_metadata = dataset.asjsonld()
    dataset = Dataset.from_jsonld(dataset_metadata)

    # assert that all attributes found in metadata are set in the instance
    assert dataset.created
    assert dataset.creator
    assert dataset.identifier
    assert dataset.name
    assert dataset.path
    assert dataset._project

    # check values
    assert str(dataset.created.isoformat()) == dataset_metadata.get('created')
    assert dataset.creator[0].email == dataset_metadata.get('creator'
                                                            )[0].get('email')
    assert dataset.identifier == dataset_metadata.get('identifier')
    assert dataset.name == dataset_metadata.get('name')
    assert dataset.path == dataset_metadata.get('path')


def test_create_dataset_custom_message(project):
    """Test create dataset custom message."""
    create_dataset(
        'ds1',
        title='',
        description='',
        creators=[],
        commit_message='my awesome dataset'
    )

    last_commit = Repo('.').head.commit
    assert 'my awesome dataset' == last_commit.message


def test_list_datasets_default(project):
    """Test a default dataset listing."""
    create_dataset(
        'ds1',
        title='',
        description='',
        creators=[],
        commit_message='my awesome dataset'
    )

    datasets = list_datasets()

    assert isinstance(datasets, list)
    assert 'ds1' in [ds.name for ds in datasets]


def test_list_files_default(project, tmpdir):
    """Test a default file listing."""
    create_dataset(
        'ds1',
        title='',
        description='',
        creators=[],
        commit_message='my awesome dataset'
    )
    data_file = tmpdir / Path('somefile')
    data_file.write_text('1,2,3', encoding='utf-8')

    add_file([str(data_file)], 'ds1')
    files = list_files(datasets=['ds1'])

    assert isinstance(files, list)
    assert 'somefile' in [ds.name for ds in files]


def test_unlink_default(directory_tree, client):
    """Test unlink default behaviour."""
    with chdir(client.path):
        create_dataset('dataset')
        add_file([directory_tree.join('dir2').strpath], 'dataset')

    with pytest.raises(ParameterError):
        file_unlink('dataset', (), ())
