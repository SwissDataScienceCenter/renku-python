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
"""Dataset tests."""

import os
import shutil
import stat
from contextlib import contextmanager

import git
import pytest

from renku.core.models.datasets import Creator, Dataset, DatasetFile


def _key(client, dataset, filename):
    """Return key in dataset for a given filename."""
    dataset_path = client.renku_datasets_path / dataset.name
    return os.path.relpath(
        str(client.path / client.datadir / dataset.name / filename),
        start=str(dataset_path),
    )


def raises(error):
    """Wrapper around pytest.raises to support None."""
    if error:
        return pytest.raises(error)
    else:

        @contextmanager
        def not_raises():
            try:
                yield
            except Exception as e:
                raise e

        return not_raises()


@pytest.mark.parametrize(
    'scheme, path, error', [('', 'temp', None), ('file://', 'temp', None),
                            ('', 'tempp', git.NoSuchPathError),
                            ('http://', 'example.com/file', None),
                            ('https://', 'example.com/file', None),
                            ('bla://', 'file', NotImplementedError)]
)
def test_data_add(
    scheme, path, error, client, data_file, directory_tree, dataset_responses
):
    """Test data import."""
    with raises(error):
        if path == 'temp':
            path = str(data_file)
        elif path == 'tempdir':
            path = str(directory_tree)

        with client.with_dataset('dataset') as d:
            d.creator = [{
                'name': 'me',
                'email': 'me@example.com',
                'identifier': 'me_id'
            }]
            client.add_data_to_dataset(d, ['{}{}'.format(scheme, path)])

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
                client.add_data_to_dataset(d, ['{}{}'.format(scheme, path)])
            assert os.path.exists('data/dataset/file')


def test_data_add_recursive(directory_tree, client):
    """Test recursive data imports."""
    with client.with_dataset('dataset') as dataset:
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

    # check that the creators are properly parsed from commits
    client.add_data_to_dataset(
        dataset, [os.path.dirname(data_repository.git_dir)], sources=['file']
    )

    assert dataset.files[1].name == 'file'
    assert len(dataset.files[1].creator) == 2
    assert all(x.name in ('me', 'me2') for x in dataset.files[1].creator)


@pytest.mark.parametrize(
    'creators', [
        [Creator(name='me', email='me@example.com')],
        [{
            'name': 'me',
            'email': 'me@example.com',
        }],
    ]
)
def test_creator_parse(creators, data_file):
    """Test that different options for specifying creators work."""
    f = DatasetFile(path='file', creator=creators)
    creator = Creator(name='me', email='me@example.com')
    assert creator in f.creator

    # email check
    with pytest.raises(ValueError):
        Creator(name='me', email='meexample.com')

    # creators must be a set or list of dicts or Creator
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
