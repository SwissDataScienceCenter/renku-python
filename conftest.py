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

import json
import os
import shutil
import sys
import tempfile
import time
import types

import pytest
import responses
from click.testing import CliRunner


@pytest.fixture(scope='module')
def renku_path(tmpdir_factory):
    """Temporary instance path."""
    path = str(tmpdir_factory.mktemp('renku'))
    yield path
    shutil.rmtree(path)


@pytest.fixture()
def instance_path(renku_path, monkeypatch):
    """Temporary instance path."""
    orig_pwd = os.getcwd()

    with monkeypatch.context() as m:
        m.chdir(renku_path)
        yield renku_path


@pytest.fixture()
def runner(monkeypatch):
    """Create a runner on isolated filesystem."""
    from renku.cli._config import RENKU_HOME
    monkeypatch.setenv('RENKU_CONFIG', RENKU_HOME)
    return CliRunner()


@pytest.fixture()
def run(runner, capsys):
    """Return a callable runner."""
    import contextlib

    from renku import cli
    from renku._contexts import Isolation

    def generate(args=('update', ), cwd=None, **streams):
        """Generate an output."""
        with capsys.disabled(), Isolation(cwd=cwd, **streams):
            try:
                cli.cli.main(
                    args=args,
                    prog_name=runner.get_default_prog_name(cli.cli),
                )
            except SystemExit as e:
                return 0 if e.code is None else e.code
            except Exception:
                raise

    return generate


@pytest.fixture()
def isolated_runner(monkeypatch):
    """Create a runner on isolated filesystem."""
    from renku.cli._config import RENKU_HOME
    monkeypatch.setenv('RENKU_CONFIG', RENKU_HOME)
    runner_ = CliRunner()
    with runner_.isolated_filesystem():
        yield runner_


@pytest.fixture()
def data_file(tmpdir):
    """Create a sample data file."""
    p = tmpdir.mkdir('data').join('file')
    p.write('1234')
    return p


@pytest.fixture(scope='module')
def repository():
    """Yield a Renku repository."""
    from renku import cli
    from renku.api import LocalClient
    runner = CliRunner()

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli.cli, ['init', '.'], catch_exceptions=False)
        assert result.exit_code == 0

        yield project_path


@pytest.fixture
def project(repository):
    """Create a test project."""
    from git import Repo

    repo = Repo(repository)
    commit = repo.head.commit

    os.chdir(repository)
    yield repository
    os.chdir(repository)
    repo.head.reset(commit, index=True, working_tree=True)
    # remove any extra non-tracked files (.pyc, etc)
    repo.git.clean('-xdff')


@pytest.fixture
def client(project):
    """Return a Renku repository."""
    from renku.api import LocalClient

    yield LocalClient(path=project)


@pytest.fixture
def dataset(client):
    """Create a dataset."""
    with client.with_dataset(name='dataset') as dataset:
        dataset.authors = {
            'name': 'me',
            'email': 'me@example.com',
        }
    return dataset


@pytest.fixture
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


@pytest.fixture(scope='module')
def directory_tree(tmpdir_factory):
    """Create a test directory tree."""
    # initialize
    p = tmpdir_factory.mktemp('directory_tree')
    p.join('file').write('1234')
    p.join('dir2').mkdir()
    p.join('dir2/file2').write('5678')
    return p


@pytest.fixture(scope='module')
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
def add_client(doctest_namespace):
    """Add Renku client to doctest namespace."""
    from renku.api import LocalClient
    doctest_namespace['client'] = LocalClient(path=tempfile.mkdtemp())
