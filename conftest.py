# -*- coding: utf-8 -*-
#
# Copyright 2017-2019- Swiss Data Science Center (SDSC)
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
import pathlib
import re
import shutil
import tempfile
import time
import urllib

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
    with monkeypatch.context() as m:
        m.chdir(renku_path)
        yield renku_path


@pytest.fixture()
def runner(monkeypatch):
    """Create a runner on isolated filesystem."""
    from renku.core.management.config import RENKU_HOME
    monkeypatch.setenv('RENKU_CONFIG', RENKU_HOME)
    return CliRunner()


@pytest.fixture
def config_dir(monkeypatch, tmpdir_factory):
    """Create a temporary renku config directory."""
    from renku.core.management.config import ConfigManagerMixin

    with monkeypatch.context() as m:
        home_dir = tmpdir_factory.mktemp('fake_home')
        conf_path = home_dir / 'renku.ini'
        m.setattr(ConfigManagerMixin, 'config_path', conf_path)

        yield m


@pytest.fixture()
def run_shell():
    """Create a shell cmd runner."""
    import subprocess

    def run_(cmd, return_ps=None, sleep_for=None):
        """Spawn subprocess and execute shell command.

        :param return_ps: Return process object.
        :param sleep_for: After executing command sleep for n seconds.
        :returns: Process object or tuple (stdout, stderr).
        """
        ps = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        if return_ps:
            return ps

        output = ps.communicate()

        if sleep_for:
            time.sleep(sleep_for)

        return output

    return run_


@pytest.fixture()
def run(runner, capsys):
    """Return a callable runner."""
    from renku.cli import cli
    from renku.core.utils.contexts import Isolation

    def generate(args=('update', ), cwd=None, **streams):
        """Generate an output."""
        with capsys.disabled(), Isolation(cwd=cwd, **streams):
            try:
                cli.main(
                    args=args,
                    prog_name=runner.get_default_prog_name(cli),
                )
            except SystemExit as e:
                return 0 if e.code is None else e.code
            except Exception:
                raise

    return generate


@pytest.fixture()
def isolated_runner(monkeypatch):
    """Create a runner on isolated filesystem."""
    from renku.core.management.config import RENKU_HOME
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
    from renku.cli import cli
    runner = CliRunner()

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli, ['init', '.'], catch_exceptions=False)
        assert 0 == result.exit_code

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
    from renku.core.management import LocalClient

    yield LocalClient(path=project)


@pytest.fixture
def dataset(client):
    """Create a dataset."""
    with client.with_dataset(name='dataset') as dataset:
        dataset.creator = [{
            'affiliation': 'xxx',
            'email': 'me@example.com',
            '_id': 'me_id',
            'name': 'me',
        }]
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
        rsps.add_callback(
            responses.GET,
            'http://example.com/file.ext?foo=bar',
            callback=request_callback
        )
        yield rsps


@pytest.fixture(scope='function')
def directory_tree(tmpdir_factory):
    """Create a test directory tree."""
    # initialize
    p = tmpdir_factory.mktemp('directory_tree')
    p.join('file').write('1234')
    p.join('dir2').mkdir()
    p.join('dir2/file2').write('5678')
    return p


@pytest.fixture(scope='function')
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


@pytest.fixture(
    params=[
        {
            'name': 'old-datasets-v0.3.0.git',
            'exit_code': 1
        },
        {
            'name': 'old-datasets-v0.5.0.git',
            'exit_code': 1
        },
        {
            'name': 'old-datasets-v0.5.1.git',
            'exit_code': 0
        },
        {
            'name': 'test-renku-v0.3.0.git',
            'exit_code': 1
        },
    ],
    scope='module',
)
def old_bare_repository(request, tmpdir_factory):
    """Prepares a testing repo created by old version of renku."""
    import tarfile
    from pathlib import Path

    compressed_repo_path = Path(
        __file__
    ).parent / 'tests' / 'fixtures' / '{0}.tar.gz'.format(
        request.param['name']
    )

    working_dir_path = tmpdir_factory.mktemp(request.param['name'])

    with tarfile.open(str(compressed_repo_path), 'r') as fixture:
        fixture.extractall(working_dir_path.strpath)

    yield {
        'path': working_dir_path / request.param['name'],
        'exit_code': request.param['exit_code']
    }

    shutil.rmtree(working_dir_path.strpath)


@pytest.fixture(scope='module')
def old_repository(tmpdir_factory, old_bare_repository):
    """Create git repo of old repository fixture."""
    import shutil
    from git import Repo

    repo_path = tmpdir_factory.mktemp('repo')
    yield {
        'repo':
            Repo(old_bare_repository['path'].strpath).clone(repo_path.strpath),
        'exit_code': old_bare_repository['exit_code']
    }
    shutil.rmtree(repo_path.strpath)


@pytest.fixture
def old_project(old_repository):
    """Create a test project."""
    repo = old_repository['repo']
    repository_path = repo.working_dir

    commit = repo.head.commit

    os.chdir(repository_path)
    yield {
        'repo': repo,
        'path': repository_path,
        'exit_code': old_repository['exit_code']
    }
    os.chdir(repository_path)
    repo.head.reset(commit, index=True, working_tree=True)
    # remove any extra non-tracked files (.pyc, etc)
    repo.git.clean('-xdff')


@pytest.fixture(autouse=True)
def add_client(doctest_namespace):
    """Add Renku client to doctest namespace."""
    from renku.core.management import LocalClient
    doctest_namespace['client'] = LocalClient(path=tempfile.mkdtemp())


@pytest.fixture
def zenodo_sandbox(client):
    """Configure environment to use Zenodo sandbox environment."""
    os.environ['ZENODO_USE_SANDBOX'] = 'true'
    client.set_value(
        'zenodo', 'access_token',
        'HPwXfABPZ7JNiwXMrktL7pevuuo9jt4gsUCkh3Gs2apg65ixa3JPyFukdGup'
    )


@pytest.fixture
def doi_responses():
    """Responses for doi.org requests."""
    from renku.core.commands.providers.doi import DOI_BASE_URL
    from renku.core.commands.providers.dataverse import (
        DATAVERSE_API_PATH, DATAVERSE_VERSION_API
    )

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:

        def doi_callback(request):
            response_url = (
                'https://dataverse.harvard.edu/citation'
                '?persistentId=doi:10.11588/data/yyxx1122'
            )
            if 'zenodo' in request.url:
                response_url = 'https://zenodo.org/record/3363060'
            return (
                200, {
                    'Content-Type': 'application/json'
                },
                json.dumps({
                    'type': 'dataset',
                    'id': request.url,
                    'author': [{
                        'family': 'Doe',
                        'given': 'John'
                    }],
                    'contributor': [{
                        'contributorType': 'ContactPerson',
                        'family': 'Doe',
                        'given': 'John'
                    }],
                    'issued': {
                        'date-parts': [[2019]]
                    },
                    'abstract': 'Test Dataset',
                    'DOI': '10.11588/data/yyxx1122',
                    'publisher': 'heiDATA',
                    'title': 'dataset',
                    'URL': response_url
                })
            )

        rsps.add_callback(
            method='GET',
            url=re.compile('{base_url}/.*'.format(base_url=DOI_BASE_URL)),
            callback=doi_callback
        )

        def version_callback(request):
            return (
                200, {
                    'Content-Type': 'application/json'
                },
                json.dumps({
                    'status': 'OK',
                    'data': {
                        'version': '4.1.3',
                        'build': 'abcdefg'
                    }
                })
            )

        base_url = 'https://dataverse.harvard.edu'

        url_parts = list(urllib.parse.urlparse(base_url))
        url_parts[2] = pathlib.posixpath.join(
            DATAVERSE_API_PATH, DATAVERSE_VERSION_API
        )
        pattern = '{url}.*'.format(url=urllib.parse.urlunparse(url_parts))

        rsps.add_callback(
            method='GET', url=re.compile(pattern), callback=version_callback
        )
        yield rsps


@pytest.fixture
def cli(client, run):
    """Return a callable Renku CLI.

    It returns the exit code and content of the resulting CWL tool.
    """
    import yaml
    from renku.core.models.cwl import CWLClass

    def renku_cli(*args):
        before_cwl_files = set(client.workflow_path.glob('*.cwl'))
        exit_code = run(args)
        after_cwl_files = set(client.workflow_path.glob('*.cwl'))
        new_files = after_cwl_files - before_cwl_files
        assert len(new_files) <= 1
        if new_files:
            cwl_filepath = new_files.pop()
            with cwl_filepath.open('r') as f:
                content = CWLClass.from_cwl(yaml.safe_load(f))
        else:
            content = None

        return exit_code, content

    return renku_cli


@pytest.fixture
def doi_dataset():
    """Return a yaml of dataset using DOI for its id."""
    from pathlib import Path
    dataset_path = Path(
        __file__
    ).parent / 'tests' / 'fixtures' / 'doi-dataset.yml'
    with open(dataset_path.as_posix()) as f:
        dataset_yaml = f.read()

    return dataset_yaml


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time
    yield
    time.sleep(0.5)
