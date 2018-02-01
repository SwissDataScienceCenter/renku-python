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

import git
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


def test_dataset_creation(test_project):
    """Test dataset directory tree creation."""
    # creating a dataset without an author fails
    with pytest.raises(RuntimeError):
        d = dataset.Dataset.create('dataset')

    d = dataset.Dataset.create(
        'dataset', authors={'name': 'me',
                            'email': 'me@example.com'})
    assert os.stat('data/dataset')

    # creating another dataset fails by default
    with pytest.raises(FileExistsError):
        d2 = dataset.Dataset.create(
            'dataset', authors={'name': 'me',
                                'email': 'me@example.com'})


@pytest.mark.parametrize('scheme, path, error',
                         [('', 'temp', None), ('file://', 'temp', None),
                          ('', 'tempp', git.NoSuchPathError),
                          ('http://', 'example.com/test_file',
                           None), ('https://', 'example.com/test_file',
                                   None), ('bla://', 'file',
                                           NotImplementedError)])
def test_data_add(scheme, path, error, test_project, test_file, test_dir,
                  dataset_responses):
    """Test data import."""
    with raises(error):
        if path == 'temp':
            path = str(test_file)
        elif path == 'tempdir':
            path = str(test_dir)
        d = dataset.Dataset.create(
            'dataset',
            datadir='./data',
            authors={'name': 'me',
                     'email': 'me@example.com'})
        d.add_data('{}{}'.format(scheme, path))
        with open('data/dataset/test_file') as f:
            assert f.read() == '1234'

        assert d.files.get('test_file')

        # check that the imported file is read-only
        assert not os.access('data/dataset/test_file',
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
            assert os.path.exists('data/dataset/test_file')


def test_data_add_recursive(test_dir, test_project):
    """Test recursive data imports."""
    d = dataset.Dataset.create(
        'dataset', authors={'name': 'me',
                            'email': 'me@example.com'})
    d.add_data(test_dir.join('dir2').strpath)
    assert 'dir2/file2' in d.files


def test_dataset_serialization(test_dataset, test_file):
    """Test deserializing a dataset object."""
    # deserialize from json on disk
    d = dataset.Dataset.from_json(test_dataset.path.joinpath('metadata.json'))
    assert d.path == test_dataset.path

    d_dict = d.to_dict()

    assert all([key in d_dict for key in ('name', 'identifier', 'files')])
    assert not len(d_dict['files'].values())
    d.add_data(str(test_file))
    d_dict = d.to_dict()
    assert len(d_dict['files'].values())


def test_repo_commit(test_dataset, test_file):
    """Test that files get commited to the git repository properly."""
    from git import Repo
    r = Repo('.')

    test_dataset.repo = r
    test_dataset.add_data(str(test_file))
    test_dataset.write_metadata()
    test_dataset.commit_to_repo()
    assert all([
        f not in r.untracked_files
        for f in ['data/dataset/metadata.json', 'data/dataset/test_file']
    ])


def test_git_repo_import(test_dataset, tmpdir, test_repo):
    """Test an import from a git repository."""
    from git import Repo
    r = Repo('.')

    test_dataset.repo = r

    # add data from remote repo
    test_dataset.add_data(
        'https://github.com/SwissDataScienceCenter/renga-python.git',
        target='README.rst')
    assert os.stat('data/dataset/renga-python/README.rst')
    assert os.stat(
        '.renga/vendors/github.com/SwissDataScienceCenter/renga-python')
    assert 'renga-python/README.rst' in test_dataset.files

    # adding a repo subpath
    test_dataset.add_data(
        'https://github.com/SwissDataScienceCenter/renga-python.git',
        target='renga/cli')
    assert 'renga-python/renga/cli/__init__.py' in test_dataset.files

    # add data from local repo
    test_dataset.add_data(
        os.path.join(os.path.dirname(test_repo.git_dir), 'dir2'))
    assert os.stat('data/dataset/test_dir/dir2/file2')
    assert 'test_dir/dir2/file2' in test_dataset.files
    assert os.stat('.renga/vendors/local')

    # check that the authors are properly parsed from commits
    test_dataset.add_data(os.path.dirname(test_repo.git_dir), target='file')
    assert len(test_dataset.files['test_dir/file'].authors) == 2
    assert all(
        x.name in ('me', 'me2')
        for x in test_dataset.files['test_dir/file'].authors)


@pytest.mark.parametrize('authors', [
    dataset.Author(name='me', email='me@example.com'),
    set([dataset.Author(name='me', email='me@example.com')]),
    [dataset.Author(name='me', email='me@example.com')], {
        'name': 'me',
        'email': 'me@example.com'
    }
])
def test_author_parse(authors, test_file):
    """Test that different options for specifying authors work."""
    f = dataset.DatasetFile(
        'test_file', origin=str(test_file), authors=authors)
    assert dataset.Author(name='me', email='me@example.com') in f.authors

    # email check
    with pytest.raises(ValueError):
        dataset.Author(name='me', email='meexample.com')

    # authors must be a set or list of dicts or Author
    with pytest.raises(ValueError):
        f = dataset.DatasetFile(
            'test_file', origin=str(test_file), authors=['name'])
