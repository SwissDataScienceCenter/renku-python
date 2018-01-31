# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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

import json
import os
import shutil
import stat
from contextlib import contextmanager

import pytest
import responses

from renga.models import dataset


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


def test_dataset_creation(tmpdir):
    """Test dataset directory tree creation."""
    p = tmpdir.mkdir("project")
    os.chdir(p)
    os.mkdir('data')

    # creating a dataset without a author fails
    with pytest.raises(RuntimeError):
        d = dataset.Dataset.create('dataset')

    d = dataset.Dataset.create(
        'dataset', authors={'name': 'me',
                            'email': 'me@example.com'})
    assert os.stat(p.join('data/dataset'))

    # creating another dataset fails by default
    with pytest.raises(FileExistsError):
        d2 = dataset.Dataset.create(
            'dataset', authors={'name': 'me',
                                'email': 'me@example.com'})


@pytest.mark.parametrize('scheme, path, error',
                         [('', 'temp', None), ('file://', 'temp', None),
                          ('', 'tempp', FileNotFoundError),
                          ('http://', 'example.com/sample_file',
                           None), ('https://', 'example.com/sample_file',
                                   None)])
def test_data_add(scheme, path, error, tmpdir, sample_file, dataset_responses):
    """Test data import."""
    p = tmpdir.mkdir("project")
    os.chdir(p)
    os.mkdir('data')

    with raises(error):
        if path == 'temp':
            path = str(sample_file)
        d = dataset.Dataset.create(
            'dataset',
            datadir='./data',
            authors={'name': 'me',
                     'email': 'me@example.com'})
        d.add_data('{}{}'.format(scheme, path))
        with open('data/dataset/sample_file') as f:
            assert f.read() == '1234'

        assert d.files.get('sample_file')

        # check that the imported file is read-only
        assert not os.access('data/dataset/sample_file',
                             stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        assert os.stat('data/dataset/metadata.json')

        # check the linking
        if scheme in ('', 'file://'):
            shutil.rmtree('./data/dataset')
            d = dataset.Dataset.create(
                'dataset',
                datadir='./data',
                authors={'name': 'me',
                         'email': 'me@example.com'})
            d.add_data('{}{}'.format(scheme, path), nocopy=True)
            assert os.path.islink('data/dataset/sample_file')


def test_dataset_serialization(temp_dataset, sample_file):
    """Test deserializing a dataset object."""
    # deserialize from json on disk
    d = dataset.Dataset.from_json(temp_dataset.path.joinpath('metadata.json'))
    assert d.path == temp_dataset.path

    d_dict = d.to_dict()

    assert all([key in d_dict for key in ('name', 'identifier', 'files')])
    assert not len(d_dict['files'].values())
    d.add_data(str(sample_file))
    d_dict = d.to_dict()
    assert len(d_dict['files'].values())


def test_repo_commit(temp_dataset, sample_file):
    """Test that files get commited to the git repository properly."""
    from git import Repo
    r = Repo('.')

    temp_dataset.repo = r
    temp_dataset.add_data(str(sample_file))
    temp_dataset.write_metadata()
    temp_dataset.commit_to_repo()
    assert all([
        f not in r.untracked_files
        for f in ['data/dataset/metadata.json', 'data/dataset/sample_file']
    ])


def test_git_repo_import(temp_dataset, tmpdir):
    """Test an import from a git repository."""
    from git import Repo, Actor
    r = Repo('.')

    temp_dataset.repo = r

    # add data from remote repo
    temp_dataset.add_data(
        'https://github.com/SwissDataScienceCenter/renga-python.git',
        targets='README.rst')
    assert os.stat('data/dataset/README.rst')
    assert os.stat(
        '.renga/vendors/github.com/SwissDataScienceCenter/renga-python')

    # add data from local repo
    p = tmpdir.mkdir('test_repo')
    test_repo = Repo.init(p.strpath)
    p.join('file').write('1234')
    test_repo.index.add([p.join('file').strpath])
    test_repo.index.commit('test commit', author=Actor('me', 'me@example.com'))
    p.join('file').write('5678')
    test_repo.index.add([p.join('file').strpath])
    test_repo.index.commit(
        'test commit', author=Actor('me2', 'me2@example.com'))
    temp_dataset.add_data(p.strpath, targets=['file'])
    assert os.stat('data/dataset/file')
    assert 'file' in temp_dataset.files
    assert os.stat('.renga/vendors/local')

    # check that the authors are properly parsed from commits
    assert len(temp_dataset.files['file'].authors) == 2
    assert all(
        x.name in ('me', 'me2') for x in temp_dataset.files['file'].authors)
