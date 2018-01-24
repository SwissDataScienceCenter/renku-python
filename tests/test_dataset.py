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


@pytest.fixture()
def temp_dataset(tmpdir):
    """Create a dataset fixture."""
    p = tmpdir.mkdir("project")
    cwd = os.getcwd()
    os.chdir(p)
    return dataset.Dataset('dataset')


def test_dataset_creation(tmpdir):
    """Test dataset directory tree creation."""
    p = tmpdir.mkdir("project")
    os.chdir(p)
    d = dataset.Dataset('dataset')
    os.stat(p.join('data/dataset'))

    # creating another dataset fails by default
    with pytest.raises(FileExistsError):
        d2 = dataset.Dataset('dataset')


@pytest.mark.parametrize('scheme, path, error',
                         [('', 'temp', None), ('file://', 'temp', None),
                          ('http://', 'example.com/sample_file',
                           None), ('https://', None, NotImplementedError)])
def test_data_import(scheme, path, error, tmpdir, sample_file,
                     dataset_responses):
    """Test data import."""
    p = tmpdir.mkdir("project")
    os.chdir(p)

    with raises(error):
        if path == 'temp':
            path = str(sample_file)
        d = dataset.Dataset(
            'dataset',
            data_dir='./data',
            import_from='{}{}'.format(scheme, path))
        with open('data/dataset/sample_file') as f:
            assert f.read() == '1234'

        assert os.stat('data/dataset/dataset.meta.json')


def test_dataset_serialization(temp_dataset, sample_file):
    """Test deserializing a dataset object."""
    # deserialize from json on disk
    d = dataset.Dataset.from_json(
        temp_dataset.path.joinpath('dataset.meta.json'))
    assert d.path == temp_dataset.path

    d_dict = d.to_dict()

    assert all([key in d_dict for key in ('name', 'identifier', 'files')])

    d.import_data(str(sample_file))
    d_dict = d.to_dict()
    assert all([key in d_dict for key in ('date_imported', 'imported_from')])
