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

import os
import shutil
import stat
from contextlib import contextmanager

import git
import pytest
import responses
import yaml

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


def dataset_creation(project):
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
                          ('http://', 'example.com/file',
                           None), ('https://', 'example.com/file',
                                   None), ('bla://', 'file',
                                           NotImplementedError)])
def test_data_add(scheme, path, error, project, data_file, directory_tree,
                  dataset_responses):
    """Test data import."""
    with raises(error):
        if path == 'temp':
            path = str(data_file)
        elif path == 'tempdir':
            path = str(directory_tree)
        d = dataset.Dataset.create(
            'dataset',
            datadir='./data',
            authors={'name': 'me',
                     'email': 'me@example.com'})
        d.add_data('{}{}'.format(scheme, path))
        with open('data/dataset/file') as f:
            assert f.read() == '1234'

        assert d.files.get('file')

        # check that the imported file is read-only
        assert not os.access('data/dataset/file',
                             stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        # assert os.stat('data/dataset/file/metadata.yml')

        # check the linking
        if scheme in ('', 'file://'):
            shutil.rmtree('./data/dataset')
            d = dataset.Dataset.create(
                'dataset',
                datadir='./data',
                authors={'name': 'me',
                         'email': 'me@example.com'})
            d.add_data('{}{}'.format(scheme, path), nocopy=True)
            assert os.path.exists('data/dataset/file')


def test_data_add_recursive(directory_tree, project):
    """Test recursive data imports."""
    d = dataset.Dataset.create(
        'dataset', authors={'name': 'me',
                            'email': 'me@example.com'})
    d.add_data(directory_tree.join('dir2').strpath)
    assert 'dir2/file2' in d.files


def dataset_serialization(dataset, data_file):
    """Test deserializing a dataset object."""
    with open(dataset.path / 'metadata.yml', 'r') as f:
        source = yaml.load(f)

    d = dataset.Dataset.from_jsonld(source)
    assert d.path == dataset.path

    d_dict = d.to_dict()

    assert all([key in d_dict for key in ('name', 'identifier', 'files')])
    assert not len(d_dict['files'].values())
    d.add_data(str(data_file))
    d_dict = d.to_dict()
    assert len(d_dict['files'].values())


def data_repository_commit(dataset, data_file):
    """Test that files get commited to the git repository properly."""
    from git import Repo
    r = Repo('.')

    dataset.repo = r
    dataset.add_data(str(data_file))
    dataset.write_metadata()
    dataset.commit_to_repo()
    assert all([
        f not in r.untracked_files
        for f in ['data/dataset/metadata.yml', 'data/dataset/file']
    ])


def test_git_repo_import(dataset, tmpdir, data_repository):
    """Test an import from a git repository."""
    # add data from local repo
    dataset.add_data(
        os.path.join(os.path.dirname(data_repository.git_dir), 'dir2'))
    assert os.stat('data/dataset/directory_tree/dir2/file2')
    assert 'directory_tree/dir2/file2' in dataset.files
    assert os.stat('.renga/vendors/local')

    # check that the authors are properly parsed from commits
    dataset.add_data(os.path.dirname(data_repository.git_dir), target='file')
    assert len(dataset.files['directory_tree/file'].authors) == 2
    assert all(
        x.name in ('me', 'me2')
        for x in dataset.files['directory_tree/file'].authors)


@pytest.mark.parametrize('authors', [
    dataset.Author(name='me', email='me@example.com'),
    set([dataset.Author(name='me', email='me@example.com')]),
    [dataset.Author(name='me', email='me@example.com')], {
        'name': 'me',
        'email': 'me@example.com'
    }
])
def test_author_parse(authors, data_file):
    """Test that different options for specifying authors work."""
    f = dataset.DatasetFile(
        'file', origin=str(data_file), authors=authors)
    assert dataset.Author(name='me', email='me@example.com') in f.authors

    # email check
    with pytest.raises(ValueError):
        dataset.Author(name='me', email='meexample.com')

    # authors must be a set or list of dicts or Author
    with pytest.raises(ValueError):
        f = dataset.DatasetFile(
            'file', origin=str(data_file), authors=['name'])
