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
"""Pytest configuration."""

from __future__ import absolute_import, print_function

import json
import os
import shutil
import tempfile
import time
import types

import pytest
import responses
from click.testing import CliRunner


@pytest.fixture()
def instance_path(monkeypatch):
    """Temporary instance path."""
    path = os.path.realpath(tempfile.mkdtemp())
    orig_pwd = os.getcwd()

    with monkeypatch.context() as m:
        m.chdir(path)
        yield path

    shutil.rmtree(path)


@pytest.fixture()
def base_runner(instance_path, monkeypatch):
    """Create a runner on isolated filesystem."""
    from renku.cli._config import RENKU_HOME
    monkeypatch.setenv('RENKU_CONFIG', os.path.join(instance_path, RENKU_HOME))
    cli_runner = CliRunner()
    yield cli_runner


@pytest.fixture()
def data_file(tmpdir):
    """Create a sample data file."""
    p = tmpdir.mkdir('data').join('file')
    p.write('1234')
    return p


@pytest.fixture()
def project(instance_path, base_runner):
    """Create a test project."""
    from renku import cli

    result = base_runner.invoke(cli.cli, ['init', '.'], catch_exceptions=False)
    assert result.exit_code == 0
    yield instance_path


@pytest.fixture()
def runner(base_runner, project):
    """Return runner with a new project."""
    yield base_runner


@pytest.fixture()
def client(project):
    """Return a Renku repository."""
    from renku.api import LocalClient
    return LocalClient(path=project)


@pytest.fixture()
def dataset(client):
    """Create a dataset."""
    with client.with_dataset(name='dataset') as dataset:
        dataset.authors = {
            'name': 'me',
            'email': 'me@example.com',
        }
    return dataset


@pytest.fixture()
def dataset_responses():
    """Authentication responses."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:

        def request_callback(request):
            return (200, {'Content-Type': 'application/text'}, '1234')

        rsps.add_callback(
            responses.GET,
            'http://example.com/file',
            callback=request_callback
        )
        rsps.add_callback(
            responses.GET,
            'https://example.com/file',
            callback=request_callback
        )
        yield rsps


@pytest.fixture()
def directory_tree(tmpdir):
    """Create a test directory tree."""
    # initialize
    p = tmpdir.mkdir('directory_tree')
    p.join('file').write('1234')
    p.join('dir2').mkdir()
    p.join('dir2/file2').write('5678')
    return p


@pytest.fixture()
def data_repository(directory_tree):
    """Create a test repo."""
    from git import Repo, Actor
    # initialize
    repo = Repo.init(directory_tree.strpath)

    # add a file
    repo.index.add([directory_tree.join('file').strpath])
    repo.index.commit('test commit', author=Actor('me', 'me@example.com'))

    # commit changes to the same file with a different user
    directory_tree.join('file').write('5678')
    repo.index.add([directory_tree.join('file').strpath])
    repo.index.commit('test commit', author=Actor('me2', 'me2@example.com'))

    # commit a second file
    repo.index.add([directory_tree.join('dir2/file2').strpath])
    repo.index.commit('test commit', author=Actor('me', 'me@example.com'))

    # return the repo
    return repo


@pytest.fixture(autouse=True)
def add_client(doctest_namespace, client):
    """Add Renku client to doctest namespace."""
    doctest_namespace['client'] = client
