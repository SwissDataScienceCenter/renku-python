# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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
import contextlib
import json
import os
import pathlib
import re
import shutil
import tempfile
import time
import urllib
import uuid
import warnings
from copy import deepcopy
from pathlib import Path

import fakeredis
import git
import pytest
import requests
import responses
import yaml
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from git import Repo
from walrus import Database


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
def runner():
    """Create a runner on isolated filesystem."""
    return CliRunner()


@pytest.fixture
def global_config_dir(monkeypatch, tmpdir_factory):
    """Create a temporary renku config directory."""
    from renku.core.management.config import ConfigManagerMixin

    with monkeypatch.context() as m:
        home_dir = tmpdir_factory.mktemp('fake_home').strpath
        m.setattr(ConfigManagerMixin, 'global_config_dir', home_dir)

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
def isolated_runner():
    """Create a runner on isolated filesystem."""
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
        result = runner.invoke(
            cli, ['init', '.', '--template', 'Basic Python Project'],
            catch_exceptions=False
        )
        assert 0 == result.exit_code

        yield project_path


@pytest.fixture
def project(repository):
    """Create a test project."""
    from git import Repo

    repo = Repo(repository, search_parent_directories=True)
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
    with client.with_dataset('dataset', create=True) as dataset:
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
            Repo(
                old_bare_repository['path'].strpath,
                search_parent_directories=True
            ).clone(repo_path.strpath),
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
def local_client():
    """Add a Renku local client."""
    from renku.core.management import LocalClient

    with tempfile.TemporaryDirectory() as tempdir:
        yield LocalClient(path=tempdir)


@pytest.fixture
def zenodo_sandbox(client):
    """Configure environment to use Zenodo sandbox environment."""
    os.environ['ZENODO_USE_SANDBOX'] = 'true'
    access_token = os.getenv('ZENODO_ACCESS_TOKEN', '')
    client.set_value('zenodo', 'access_token', access_token)
    client.repo.git.add('.renku/renku.ini')
    client.repo.index.commit('update renku.ini')


@pytest.fixture
def dataverse_demo(client, dataverse_demo_cleanup):
    """Configure environment to use Dataverse demo environment."""
    access_token = os.getenv('DATAVERSE_ACCESS_TOKEN', '')
    client.set_value('dataverse', 'access_token', access_token)
    client.set_value('dataverse', 'server_url', 'https://demo.dataverse.org')
    client.repo.git.add('.renku/renku.ini')
    client.repo.index.commit('renku.ini')


@pytest.fixture(scope='module')
def dataverse_demo_cleanup(request):
    """Delete all Dataverse datasets at the end of the test session."""
    from renku.core.utils.requests import retry

    server_url = 'https://demo.dataverse.org'
    access_token = os.getenv('DATAVERSE_ACCESS_TOKEN', '')
    headers = {'X-Dataverse-key': access_token}

    def remove_datasets():
        url = f'{server_url}/api/v1/dataverses/sdsc-test-dataverse/contents'
        try:
            with retry() as session:
                response = session.get(url=url, headers=headers)
        except (ConnectionError, requests.exceptions.RequestException):
            warnings.warn('Cannot clean up Dataverse datasets')
            return

        if response.status_code != 200:
            warnings.warn('Cannot clean up Dataverse datasets')
            return

        datasets = response.json().get('data', [])

        for dataset in datasets:
            id = dataset.get('id')
            if id is not None:
                url = f'https://demo.dataverse.org/api/v1/datasets/{id}'
                try:
                    with retry() as session:
                        session.delete(url=url, headers=headers)
                except (ConnectionError, requests.exceptions.RequestException):
                    pass

    request.addfinalizer(remove_datasets)


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


@pytest.fixture(
    params=[{
        'path':
            Path(__file__).parent / 'tests' / 'fixtures' / 'doi-dataset.yml',
    }, {
        'path':
            Path(__file__).parent / 'tests' / 'fixtures' /
            'broken-dataset-v0.5.2.yml',
    }]
)
def dataset_metadata(request):
    """Return dataset metadata fixture."""
    from renku.core.models.jsonld import NoDatesSafeLoader

    file_path = request.param['path']

    data = yaml.load(file_path.read_text(), Loader=NoDatesSafeLoader)
    yield data


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time
    yield
    time.sleep(0.5)


@pytest.fixture
def remote_project(data_repository, directory_tree):
    """A second Renku project with a dataset."""
    from renku.cli import cli

    runner = CliRunner()

    with runner.isolated_filesystem() as project_path:
        runner.invoke(
            cli, ['-S', 'init', '.', '--template', 'Basic Python Project']
        )
        result = runner.invoke(
            cli, ['-S', 'dataset', 'create', 'remote-dataset']
        )
        assert 0 == result.exit_code

        result = runner.invoke(
            cli,
            [
                '-S', 'dataset', 'add', '-s', 'file', '-s', 'dir2',
                'remote-dataset', directory_tree.strpath
            ],
            catch_exceptions=False,
        )
        assert 0 == result.exit_code

        yield runner, project_path


@pytest.fixture(scope='function')
def datapack_zip(directory_tree):
    """Returns dummy data folder as a zip archive."""
    from renku.core.utils.contexts import chdir
    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive('datapack', 'zip', str(directory_tree))

    yield Path(workspace_dir.name) / 'datapack.zip'


@pytest.fixture(scope='function')
def datapack_tar(directory_tree):
    """Returns dummy data folder as a tar archive."""
    from renku.core.utils.contexts import chdir
    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive('datapack', 'tar', str(directory_tree))

    yield Path(workspace_dir.name) / 'datapack.tar'


@pytest.fixture(scope='module')
def mock_redis():
    """Monkey patch service cache with mocked redis."""
    from renku.service.cache.base import BaseCache
    from renku.service.cache.models.user import User
    from renku.service.cache.models.job import Job
    from renku.service.cache.models.file import File
    from renku.service.cache.models.project import Project
    from renku.service.jobs.queues import WorkerQueues

    monkey_patch = MonkeyPatch()
    with monkey_patch.context() as m:
        fake_redis = fakeredis.FakeRedis()
        fake_model_db = Database(connection_pool=fake_redis.connection_pool)

        m.setattr(WorkerQueues, 'connection', fake_redis)
        m.setattr(BaseCache, 'cache', fake_redis)
        m.setattr(BaseCache, 'model_db', fake_model_db)

        m.setattr(Job, '__database__', fake_model_db)
        m.setattr(User, '__database__', fake_model_db)
        m.setattr(File, '__database__', fake_model_db)
        m.setattr(Project, '__database__', fake_model_db)

        yield

    monkey_patch.undo()


@pytest.fixture(scope='module')
def svc_client(mock_redis):
    """Renku service client."""
    from renku.service.entrypoint import create_app

    flask_app = create_app()

    testing_client = flask_app.test_client()
    testing_client.testing = True

    ctx = flask_app.app_context()
    ctx.push()

    yield testing_client

    ctx.pop()


@pytest.fixture(scope='function')
def svc_client_cache(mock_redis):
    """Service jobs fixture."""
    from renku.service.entrypoint import create_app

    flask_app = create_app()

    testing_client = flask_app.test_client()
    testing_client.testing = True

    ctx = flask_app.app_context()
    ctx.push()

    yield testing_client, flask_app.config.get('cache')

    ctx.pop()


def integration_repo_path(headers, url_components):
    """Constructs integration repo path."""
    from renku.service.config import CACHE_PROJECTS_PATH
    project_path = (
        CACHE_PROJECTS_PATH / headers['Renku-User-Id'] / url_components.owner /
        url_components.name
    )

    return project_path


@contextlib.contextmanager
def integration_repo(headers, url_components):
    """With integration repo helper."""
    from renku.core.utils.contexts import chdir

    with chdir(integration_repo_path(headers, url_components)):
        repo = Repo('.')
        yield repo


@pytest.fixture(scope='module')
def integration_lifecycle(svc_client, mock_redis):
    """Setup and teardown steps for integration tests."""
    from renku.core.models.git import GitURL
    remote_url = 'https://dev.renku.ch/gitlab/contact/integration-test'
    url_components = GitURL.parse(remote_url)

    headers = {
        'Content-Type': 'application/json',
        'Renku-User-Id': 'b4b4de0eda0f471ab82702bd5c367fa7',
        'Renku-User-FullName': 'Just Sam',
        'Renku-User-Email': 'contact@justsam.io',
        'Authorization': 'Bearer {0}'.format(os.getenv('IT_OAUTH_GIT_TOKEN')),
    }

    payload = {'git_url': remote_url}

    response = svc_client.post(
        '/cache.project_clone',
        data=json.dumps(payload),
        headers=headers,
    )

    assert response
    assert 'result' in response.json
    assert 'error' not in response.json

    project_id = response.json['result']['project_id']
    assert isinstance(uuid.UUID(project_id), uuid.UUID)

    yield svc_client, headers, project_id, url_components

    # Teardown step: Delete all branches except master (if needed).
    if integration_repo_path(headers, url_components).exists():
        with integration_repo(headers, url_components) as repo:
            for repo_branch in repo.references:
                if repo_branch.name == 'master':
                    continue
                try:
                    repo.remote().push(
                        refspec=(':{0}'.format(repo_branch.name))
                    )
                except git.exc.GitCommandError:
                    continue


@pytest.fixture
def svc_client_with_repo(integration_lifecycle):
    """Service client with a remote repository."""
    svc_client, headers, project_id, url_components = integration_lifecycle

    with integration_repo(headers, url_components) as repo:
        new_branch = uuid.uuid4().hex
        current = repo.create_head(new_branch)
        current.checkout()

    yield svc_client, deepcopy(headers), project_id, url_components


@pytest.fixture(
    params=[
        {
            'url': '/cache.files_list',
            'allowed_method': 'GET',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/cache.files_upload',
            'allowed_method': 'POST',
            'headers': {}
        },
        {
            'url': '/cache.project_clone',
            'allowed_method': 'POST',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/cache.project_list',
            'allowed_method': 'GET',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/datasets.add',
            'allowed_method': 'POST',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/datasets.create',
            'allowed_method': 'POST',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/datasets.files_list',
            'allowed_method': 'GET',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
        {
            'url': '/datasets.list',
            'allowed_method': 'GET',
            'headers': {
                'Content-Type': 'application/json',
                'accept': 'application/json',
            }
        },
    ]
)
def service_allowed_endpoint(request, svc_client, mock_redis):
    """Ensure allowed methods and correct headers."""
    methods = {
        'GET': svc_client.get,
        'POST': svc_client.post,
        'HEAD': svc_client.head,
        'PUT': svc_client.put,
        'DELETE': svc_client.delete,
        'OPTIONS': svc_client.options,
        'TRACE': svc_client.trace,
        'PATCH': svc_client.patch,
    }

    yield methods, request.param, svc_client


@pytest.fixture
def service_job(svc_client, mock_redis):
    """Ensure correct environment during testing of service jobs."""
    old_environ = dict(os.environ)

    os.environ['RENKU_SVC_CLEANUP_TTL_FILES'] = '0'
    os.environ['RENKU_SVC_CLEANUP_TTL_PROJECTS'] = '0'

    try:
        yield svc_client, mock_redis
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


@pytest.fixture
def dummy_run_plugin_hook():
    """A dummy hook to be used with the renku run plugin."""
    from renku.core.plugins import hookimpl

    class _CmdlineToolAnnotations(object):
        """CmdlineTool Hook implementation namespace."""

        @hookimpl
        def cmdline_tool_annotations(self, tool):
            """``cmdline_tool_annotations`` hook implementation."""
            from renku.core.models.cwl.annotation import Annotation

            return [
                Annotation(
                    id='_:annotation',
                    source='Dummy Cmdline Hook',
                    body='dummy cmdline hook body'
                )
            ]

    return _CmdlineToolAnnotations()


@pytest.fixture
def dummy_pre_run_plugin_hook():
    """A dummy hook to be used with the renku run plugin."""
    from renku.core.plugins import hookimpl

    class _PreRun(object):
        """CmdlineTool Hook implementation namespace."""

        called = 0

        @hookimpl
        def pre_run(self, tool):
            """``cmdline_tool_annotations`` hook implementation."""
            self.called = 1

    return _PreRun()


@pytest.fixture
def dummy_processrun_plugin_hook():
    """A dummy hook to be used with the renku run plugin."""
    from renku.core.plugins import hookimpl

    class _ProcessRunAnnotations(object):
        """CmdlineTool Hook implementation namespace."""

        @hookimpl
        def process_run_annotations(self, run):
            """``process_run_annotations`` hook implementation."""
            from renku.core.models.cwl.annotation import Annotation

            return [
                Annotation(
                    id='_:annotation',
                    source='Dummy ProcessRun Hook',
                    body='dummy ProcessRun hook body'
                )
            ]

    return _ProcessRunAnnotations()
