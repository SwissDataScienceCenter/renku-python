# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
import secrets
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib
import uuid
import warnings
from copy import deepcopy
from pathlib import Path

import fakeredis
import jwt
import pytest
import requests
import responses
import yaml
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from git import GitCommandError, Repo
from walrus import Database
from werkzeug.utils import secure_filename

from tests.utils import make_dataset_add_payload

IT_PROTECTED_REMOTE_REPO_URL = os.getenv(
    "IT_PROTECTED_REMOTE_REPO", "https://dev.renku.ch/gitlab/renku-qa/core-it-protected.git"
)

IT_REMOTE_REPO_URL = os.getenv("IT_REMOTE_REPOSITORY", "https://dev.renku.ch/gitlab/renku-qa/core-integration-test")
IT_REMOTE_NON_RENKU_REPO_URL = os.getenv(
    "IT_REMOTE_NON_RENKU_REPO_URL", "https://dev.renku.ch/gitlab/renku-qa/core-it-non-renku"
)
IT_REMOTE_NO_COMMITS_REPO_URL = os.getenv(
    "IT_REMOTE_NO_COMMITS_REPO_URL", "https://dev.renku.ch/gitlab/renku-qa/core-it-no-commits"
)
IT_GIT_ACCESS_TOKEN = os.getenv("IT_OAUTH_GIT_TOKEN")


@pytest.fixture(scope="module")
def it_remote_repo():
    """Returns a remote path to integration test repository."""
    return IT_REMOTE_REPO_URL


@pytest.fixture(scope="module")
def it_remote_non_renku_repo():
    """Returns a remote path to integration test repository."""
    return IT_REMOTE_NON_RENKU_REPO_URL


@pytest.fixture(scope="module")
def it_remote_no_commits_repo():
    """Returns a remote path to integration test repository."""
    return IT_REMOTE_NO_COMMITS_REPO_URL


@contextlib.contextmanager
def _isolated_filesystem(tmpdir, name=None, delete=True):
    """Click CliRunner ``isolated_filesystem`` but xdist compatible."""
    from renku.core.utils.contexts import chdir

    if not name:
        name = secrets.token_hex(8)
    t = tmpdir.mkdir(name)

    with chdir(t):
        try:
            yield t
        finally:
            if delete:
                try:
                    shutil.rmtree(t)
                except OSError:  # noqa: B014
                    pass


@pytest.fixture()
def renku_path(tmpdir):
    """Temporary instance path."""
    path = str(tmpdir.mkdir("renku"))
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
def global_config_dir(monkeypatch, tmpdir):
    """Create a temporary renku config directory."""
    from renku.core.management.config import ConfigManagerMixin

    with monkeypatch.context() as m:
        home_dir = tmpdir.mkdir("fake_home").strpath
        m.setattr(ConfigManagerMixin, "global_config_dir", home_dir)

        yield m


@pytest.fixture()
def run_shell():
    """Create a shell cmd runner."""

    def run_(cmd, return_ps=None, sleep_for=None):
        """Spawn subprocess and execute shell command.

        :param return_ps: Return process object.
        :param sleep_for: After executing command sleep for n seconds.
        :returns: Process object or tuple (stdout, stderr).
        """
        ps = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

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

    def generate(args=("update", "--all",), cwd=None, **streams):
        """Generate an output."""
        with capsys.disabled(), Isolation(cwd=cwd, **streams):
            try:
                cli.main(
                    args=args, prog_name=runner.get_default_prog_name(cli),
                )
                return 0
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
def repository(tmpdir):
    """Yield a Renku repository."""
    from renku.cli import cli

    runner = CliRunner()
    with _isolated_filesystem(tmpdir, delete=True) as project_path:
        result = runner.invoke(cli, ["init", ".", "--template-id", "python-minimal"], "\n", catch_exceptions=False)
        assert 0 == result.exit_code

        yield os.path.realpath(project_path)


@pytest.fixture()
def template():
    """Yield template data."""
    template = {
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "id": "python-minimal",
        "index": 1,
        "ref": "master",
        "metadata": {"description": "nodesc"},
        "default_metadata": {
            "__template_source__": "renku",
            "__template_ref__": "master",
            "__template_id__": "python-minimal",
            "__namespace__": "",
            "__repository__": "",
            "__project_slug__": "",
        },
    }

    yield template


@pytest.fixture()
def project_init(template):
    """Yield template data."""
    data = {
        "test_project": "test-new-project",
        "test_project_alt": "test-new-project-2",
    }

    commands = {
        "init": ["init", "."],
        "init_test": ["init", data["test_project"]],
        "init_alt": ["init", data["test_project_alt"]],
        "remote": ["--template-source", template["url"], "--template-ref", template["ref"]],
        "id": ["--template-id", template["id"]],
        "index": ["--template-index", template["index"]],
        "force": ["--force"],
        "list": ["--list-templates"],
        "parameters": ["--parameter", "p1=v1", "--parameter", "p2=v2"],
        "parameters_equal_missing": ["--parameter", "p3:v3"],
        "parameters_equal_early": ["--parameter", "=p4v3"],
        "confirm": len(set(template["metadata"].keys())) * "\n",
    }

    yield data, commands


@pytest.fixture
def template_update(tmpdir, local_client, mocker, template):
    """Create a mocked template for updates."""

    def _template_update(immutable_files=None, docker=False, after_template_version="0.0.2"):
        """Fetches an updatable template with various options."""
        import pkg_resources

        from renku.core.commands.init import create_from_template, read_template_manifest

        template_local = Path(pkg_resources.resource_filename("renku", "templates"))

        # NOTE: get template
        tempdir = tmpdir.mkdir("template")
        temppath = Path(tempdir) / "local"
        shutil.copytree(str(template_local), str(temppath))
        manifest = read_template_manifest(temppath)
        template_path = temppath / manifest[0]["folder"]

        if docker:
            import renku

            mocker.patch.object(renku, "__version__", return_value="0.0.1")

            # TODO: remove this once the renku template contains RENKU_VERSION
            dockerfile_path = template_path / "Dockerfile"
            dockerfile = dockerfile_path.read_text()
            dockerfile_path.write_text(f"ARG RENKU_VERSION=0.0.1\n{dockerfile}")

        # NOTE: init project from template
        create_from_template(
            template_path,
            local_client,
            "name",
            {**template["default_metadata"], **template["metadata"]},
            template_version="0.0.1",
            immutable_template_files=immutable_files or [],
            automated_update=True,
        )
        project_files = [
            f
            for f in local_client.path.glob("**/*")
            if ".git" not in str(f)
            and not str(f).endswith(".renku/metadata.yml")
            and not str(f).endswith(".renku/template_checksums.json")
        ]
        template_files = []
        for project_file in project_files:
            expected_file = template_path / project_file.relative_to(local_client.path)
            template_files.append(expected_file)
            assert expected_file.exists()

        fetch_template = mocker.patch("renku.core.commands.init.fetch_template")
        fetch_template.return_value = (manifest, temppath, "renku", after_template_version)

        return {
            "template_files": template_files,
            "project_files": project_files,
            "manifest": manifest,
            "manifest_path": temppath,
            "template_path": template_path,
        }

    yield _template_update


@pytest.fixture()
def project(repository):
    """Create a test project."""
    from renku.cli import cli
    from renku.core.utils.contexts import chdir

    runner = CliRunner()

    repo = Repo(repository, search_parent_directories=True)
    commit = repo.head.commit

    with chdir(repository):
        yield repository

        os.chdir(repository)
        repo.head.reset(commit, index=True, working_tree=True)
        # INFO: remove any extra non-tracked files (.pyc, etc)
        repo.git.clean("-xdff")

        assert 0 == runner.invoke(cli, ["githooks", "install", "--force"]).exit_code


@pytest.fixture
def project_metadata(project):
    """Create project with metadata."""
    metadata = {
        "project_id": uuid.uuid4().hex,
        "name": Path(project).name,
        "fullname": "full project name",
        "email": "my@email.com",
        "owner": "me",
        "token": "awesome token",
        "git_url": "git@gitlab.com",
        "initialized": True,
    }

    yield project, metadata


@pytest.fixture()
def client(project):
    """Return a Renku repository."""
    from renku.core.management import LocalClient
    from renku.core.models.enums import ConfigFilter

    original_get_value = LocalClient.get_value

    def mocked_get_value(self, section, key, config_filter=ConfigFilter.ALL):
        """We don't want lfs warnings in tests."""
        if key == "show_lfs_message":
            return "False"
        return original_get_value(self, section, key, config_filter=config_filter)

    LocalClient.get_value = mocked_get_value

    yield LocalClient(path=project)

    LocalClient.get_value = original_get_value


@pytest.fixture()
def client_with_remote(client, tmpdir):
    """Return a client with a (local) remote set."""
    # create remote
    path = str(tmpdir.mkdir("remote"))
    Repo().init(path, bare=True)

    origin = client.repo.create_remote("origin", path)
    client.repo.git.push("--set-upstream", "origin", "master")
    yield {"client": client, "origin": origin}
    client.repo.git.branch("--unset-upstream")
    client.repo.delete_remote(origin)
    shutil.rmtree(path)


@pytest.fixture
def no_lfs_warning(client):
    """Sets show_lfs_message to False.

    For those times in life when mocking just isn't enough.
    """
    with client.commit():
        client.set_value("renku", "show_lfs_message", "False")

    yield client


@pytest.fixture()
def client_with_lfs_warning(project):
    """Return a Renku repository with lfs warnings active."""
    from renku.core.management import LocalClient

    client = LocalClient(path=project)
    client.set_value("renku", "lfs_threshold", "0b")
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")
    yield client


@pytest.fixture
def client_with_datasets(client, directory_tree):
    """A client with datasets."""
    from renku.core.models.provenance.agents import Person

    person_1 = Person.from_string("P1 <p1@example.com> [IANA]")
    person_2 = Person.from_string("P2 <p2@example.com>")

    client.create_dataset(name="dataset-1", keywords=["dataset", "1"], creators=[person_1])

    with client.with_dataset("dataset-2", create=True) as dataset:
        dataset.keywords = ["dataset", "2"]
        dataset.creators = [person_1, person_2]

        client.add_data_to_dataset(dataset=dataset, urls=[str(p) for p in directory_tree.glob("*")])

    yield client


@pytest.fixture()
def client_with_new_graph(client):
    """A client with new graph metadata."""
    from renku.core.incubation.graph import generate_graph

    generate_graph().build().execute(force=True)

    yield client


@pytest.fixture(params=[".", "some/sub/directory"])
def subdirectory(project, request):
    """Runs tests in root directory and a subdirectory."""
    from renku.core.utils.contexts import chdir

    if request.param != ".":
        path = Path(request.param) / ".gitkeep"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        Repo().git.add(str(path))
        Repo().index.commit("Create subdirectory", skip_hooks=True)

    with chdir(request.param):
        yield Path(request.param).resolve()


@pytest.fixture
def dataset_responses():
    """Authentication responses."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:

        def request_callback(request):
            return (200, {"Content-Type": "application/text"}, "123")

        rsps.add_callback(responses.GET, "http://example.com/file1", callback=request_callback)
        rsps.add_callback(responses.GET, "https://example.com/file1", callback=request_callback)
        rsps.add_callback(responses.GET, "http://example.com/file1.ext?foo=bar", callback=request_callback)
        rsps.add_callback(responses.HEAD, "http://example.com/file1", callback=request_callback)
        rsps.add_callback(responses.HEAD, "https://example.com/file1", callback=request_callback)
        rsps.add_callback(responses.HEAD, "http://example.com/file1.ext?foo=bar", callback=request_callback)
        yield rsps


@pytest.fixture
def missing_kg_project_responses():
    """KG project query responses for missing project."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:

        def request_callback(request):
            return (404, {"Content-Type": "application/text"}, json.dumps({"message": "no project found"}))

        rsps.add_callback(
            responses.GET, re.compile("http(s)*://dev.renku.ch/knowledge-graph/projects/.*"), callback=request_callback
        )
        rsps.add_passthru(re.compile("http(s)*://dev.renku.ch/datasets/.*"))
        rsps.add_passthru(re.compile("http(s)*://dev.renku.ch/knowledge-graph/datasets/.*"))
        yield rsps


@pytest.fixture()
def directory_tree(tmp_path):
    """Create a test directory tree."""
    # initialize
    p = tmp_path / "directory_tree"
    p.mkdir()
    p.joinpath("file1").write_text("123")
    p.joinpath("dir1").mkdir()
    p.joinpath("dir1/file2").write_text("456")
    p.joinpath("dir1/file3").write_text("789")
    return p


@pytest.fixture()
def data_repository(directory_tree):
    """Create a test repo."""
    from git import Actor, Repo

    # initialize
    repo = Repo.init(str(directory_tree))

    # add a file
    repo.index.add([str(directory_tree / "file1")])
    repo.index.commit("test commit", author=Actor("me", "me@example.com"))

    # commit changes to the same file with a different user
    directory_tree.joinpath("file1").write_text("5678")
    repo.index.add([str(directory_tree / "file1")])
    repo.index.commit("test commit", author=Actor("me2", "me2@example.com"))

    # commit a second file
    repo.index.add([str(directory_tree / "dir1" / "file2")])
    repo.index.commit("test commit", author=Actor("me", "me@example.com"))

    # return the repo
    return repo


def clone_compressed_repository(base_path, name):
    """Decompress and clone a repository."""

    compressed_repo_path = Path(__file__).parent / "tests" / "fixtures" / f"{name}.tar.gz"
    working_dir = base_path / name

    bare_base_path = working_dir / "bare"
    with tarfile.open(compressed_repo_path, "r") as fixture:
        fixture.extractall(str(bare_base_path))

    bare_path = bare_base_path / name
    repository_path = working_dir / "repository"
    repository = Repo(bare_path, search_parent_directories=True).clone(repository_path)

    return repository


@pytest.fixture(
    params=["old-datasets-v0.3.0.git", "old-datasets-v0.5.0.git", "old-datasets-v0.5.1.git", "test-renku-v0.3.0.git"]
)
def old_project(request, tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku.core.utils.contexts import chdir

    name = request.param
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.working_dir):
        yield repository


@pytest.fixture(
    params=[
        {
            "name": "old-workflows-v0.10.3.git",
            "log_path": "catoutput.txt",
            "expected_strings": [
                "catoutput.txt",
                "_cat.yaml",
                "_echo.yaml",
                "9ecc28b2 stdin.txt",
                "bdc801c6 stdout.txt",
            ],
        },
        {
            "name": "old-workflows-complicated-v0.10.3.git",
            "log_path": "concat2.txt",
            "expected_strings": [
                "concat2.txt",
                "5828275ae5344eba8bad475e7d3cf2d5.cwl",
                "_migrated.yaml",
                "88add2ea output_rand",
                "e6fa6bf3 input2.txt",
            ],
        },
    ],
)
def old_workflow_project(request, tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku.core.utils.contexts import chdir

    name = request.param["name"]
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)
    repository_path = repository.working_dir

    with chdir(repository_path):
        yield {
            "repo": repository,
            "path": repository_path,
            "log_path": request.param["log_path"],
            "expected_strings": request.param["expected_strings"],
        }


@pytest.fixture
def old_dataset_project(tmp_path):
    """Prepares a testing repo created by old version of renku."""
    from renku import LocalClient
    from renku.core.utils.contexts import chdir

    name = "old-datasets-v0.9.1.git"
    base_path = tmp_path / name
    repository = clone_compressed_repository(base_path=base_path, name=name)

    with chdir(repository.working_dir):
        yield LocalClient(path=repository.working_dir)


@pytest.fixture
def old_repository_with_submodules(request, tmp_path):
    """Prepares a testing repo that has datasets using git submodules."""
    from renku.core.utils.contexts import chdir

    name = "old-datasets-v0.6.0-with-submodules"
    base_path = Path(__file__).parent / "tests" / "fixtures" / f"{name}.tar.gz"

    working_dir = tmp_path / name

    with tarfile.open(str(base_path), "r") as repo:
        repo.extractall(working_dir)

    repo_path = working_dir / name
    repo = Repo(repo_path)

    with chdir(repo_path):
        yield repo


@pytest.fixture
def unsupported_project(client):
    """A client with a newer project version."""
    with client.with_metadata() as project:
        impossible_newer_version = 42000
        project.version = impossible_newer_version

    client.repo.git.add(".renku")
    client.repo.index.commit("update renku.ini", skip_hooks=True)

    yield client


@pytest.fixture(autouse=True)
def add_client(doctest_namespace):
    """Add Renku client to doctest namespace."""
    from renku.core.management import LocalClient

    doctest_namespace["client"] = LocalClient(path=tempfile.mkdtemp())


@pytest.fixture
def local_client():
    """Add a Renku local client."""
    from renku.core.management import LocalClient

    with tempfile.TemporaryDirectory() as tempdir:
        yield LocalClient(path=tempdir)


@pytest.fixture
def zenodo_sandbox(client):
    """Configure environment to use Zenodo sandbox environment."""
    os.environ["ZENODO_USE_SANDBOX"] = "true"
    access_token = os.getenv("ZENODO_ACCESS_TOKEN", "")
    client.set_value("zenodo", "access_token", access_token)
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")


@pytest.fixture
def dataverse_demo(client, dataverse_demo_cleanup):
    """Configure environment to use Dataverse demo environment."""
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    client.set_value("dataverse", "access_token", access_token)
    client.set_value("dataverse", "server_url", "https://demo.dataverse.org")
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("renku.ini")


@pytest.fixture(scope="module")
def dataverse_demo_cleanup(request):
    """Delete all Dataverse datasets at the end of the test session."""
    from renku.core.utils.requests import retry

    server_url = "https://demo.dataverse.org"
    access_token = os.getenv("DATAVERSE_ACCESS_TOKEN", "")
    headers = {"X-Dataverse-key": access_token}

    def remove_datasets():
        url = f"{server_url}/api/v1/dataverses/sdsc-test-dataverse/contents"
        try:
            with retry() as session:
                response = session.get(url=url, headers=headers)
        except (ConnectionError, requests.exceptions.RequestException):
            warnings.warn("Cannot clean up Dataverse datasets")
            return

        if response.status_code != 200:
            warnings.warn("Cannot clean up Dataverse datasets")
            return

        datasets = response.json().get("data", [])

        for dataset in datasets:
            id = dataset.get("id")
            if id is not None:
                url = f"https://demo.dataverse.org/api/v1/datasets/{id}"
                try:
                    with retry() as session:
                        session.delete(url=url, headers=headers)
                except (ConnectionError, requests.exceptions.RequestException):
                    pass

    request.addfinalizer(remove_datasets)


@pytest.fixture
def doi_responses():
    """Responses for doi.org requests."""
    from renku.core.commands.providers.dataverse import DATAVERSE_API_PATH, DATAVERSE_VERSION_API
    from renku.core.commands.providers.doi import DOI_BASE_URL

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:

        def doi_callback(request):
            response_url = "https://dataverse.harvard.edu/citation" "?persistentId=doi:10.11588/data/yyxx1122"
            if "zenodo" in request.url:
                response_url = "https://zenodo.org/record/3363060"
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    {
                        "type": "dataset",
                        "id": request.url,
                        "author": [{"family": "Doe", "given": "John"}],
                        "contributor": [{"contributorType": "ContactPerson", "family": "Doe", "given": "John"}],
                        "issued": {"date-parts": [[2019]]},
                        "abstract": "Test Dataset",
                        "DOI": "10.11588/data/yyxx1122",
                        "publisher": "heiDATA",
                        "title": "dataset",
                        "URL": response_url,
                    }
                ),
            )

        rsps.add_callback(
            method="GET", url=re.compile("{base_url}/.*".format(base_url=DOI_BASE_URL)), callback=doi_callback
        )

        def version_callback(request):
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps({"status": "OK", "data": {"version": "4.1.3", "build": "abcdefg"}}),
            )

        base_url = "https://dataverse.harvard.edu"

        url_parts = list(urllib.parse.urlparse(base_url))
        url_parts[2] = pathlib.posixpath.join(DATAVERSE_API_PATH, DATAVERSE_VERSION_API)
        pattern = "{url}.*".format(url=urllib.parse.urlunparse(url_parts))

        rsps.add_callback(method="GET", url=re.compile(pattern), callback=version_callback)
        yield rsps


@pytest.fixture()
def renku_cli(client, run):
    """Return a callable Renku CLI.

    It returns the exit code and content of the resulting CWL tool.
    """
    import yaml

    from renku.core.models.provenance.activities import Activity

    def renku_cli_(*args, **kwargs):
        before_wf_files = set(client.workflow_path.glob("*.yaml"))
        exit_code = run(args, **kwargs)
        after_wf_files = set(client.workflow_path.glob("*.yaml"))
        new_files = after_wf_files - before_wf_files
        assert len(new_files) <= 1
        if new_files:
            wf_filepath = new_files.pop()
            with wf_filepath.open("r") as f:
                content = Activity.from_jsonld(yaml.safe_load(f), client=client, commit=client.repo.head.commit)
            content = content.association.plan
        else:
            content = None

        return exit_code, content

    return renku_cli_


@pytest.fixture
def dataset_metadata():
    """Return dataset metadata fixture."""
    from renku.core.models.jsonld import NoDatesSafeLoader

    file_path = Path(__file__).parent / "tests" / "fixtures" / "doi-dataset.yml"

    data = yaml.load(file_path.read_text(), Loader=NoDatesSafeLoader)
    yield data


@pytest.fixture
def dataset_metadata_before_calamus():
    """Return dataset metadata fixture."""
    from renku.core.models.jsonld import NoDatesSafeLoader

    path = Path(__file__).parent / "tests" / "fixtures" / "dataset-v0.10.4-before-calamus.yml"
    yield yaml.load(path.read_text(), Loader=NoDatesSafeLoader)


@pytest.fixture()
def sleep_after():
    """Fixture that causes a delay after executing a test.

    Prevents spamming external providers when used, in case of rate limits.
    """
    import time

    yield
    time.sleep(0.5)


@pytest.fixture()
def datapack_zip(directory_tree):
    """Returns dummy data folder as a zip archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "zip", str(directory_tree))

    yield Path(workspace_dir.name) / "datapack.zip"


@pytest.fixture()
def datapack_tar(directory_tree):
    """Returns dummy data folder as a tar archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "tar", str(directory_tree))

    yield Path(workspace_dir.name) / "datapack.tar"


@pytest.fixture()
def datapack_gz(directory_tree):
    """Returns dummy data folder as a tar archive."""
    from renku.core.utils.contexts import chdir

    workspace_dir = tempfile.TemporaryDirectory()
    with chdir(workspace_dir.name):
        shutil.make_archive("datapack", "gztar", str(directory_tree))

    shutil.move(Path(workspace_dir.name) / "datapack.tar.gz", Path(workspace_dir.name) / "datapack.gz")

    yield Path(workspace_dir.name) / "datapack.gz"


@pytest.fixture(scope="module")
def mock_redis():
    """Monkey patch service cache with mocked redis."""
    from renku.core.commands import save
    from renku.service.cache.base import BaseCache
    from renku.service.cache.models.file import File
    from renku.service.cache.models.job import Job
    from renku.service.cache.models.project import Project
    from renku.service.cache.models.user import User
    from renku.service.jobs.queues import WorkerQueues

    def repo_sync_mock(p, remote=None):
        return None, "origin"

    monkey_patch = MonkeyPatch()
    with monkey_patch.context() as m:
        fake_redis = fakeredis.FakeRedis()
        fake_model_db = Database(connection_pool=fake_redis.connection_pool)

        m.setattr(WorkerQueues, "connection", fake_redis)
        m.setattr(BaseCache, "cache", fake_redis)
        m.setattr(BaseCache, "model_db", fake_model_db)

        m.setattr(Job, "__database__", fake_model_db)
        m.setattr(User, "__database__", fake_model_db)
        m.setattr(File, "__database__", fake_model_db)
        m.setattr(Project, "__database__", fake_model_db)

        save.repo_sync = repo_sync_mock

        yield

    monkey_patch.undo()


@pytest.fixture
def real_sync():
    """Enable remote sync."""
    import importlib

    from renku.core.commands import save

    # NOTE: Use this fixture only in serial tests. save.repo_sync is mocked; reloading the save module to undo the mock.
    importlib.reload(save)


@pytest.fixture(scope="module")
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


@pytest.fixture()
def svc_client_cache(mock_redis, identity_headers):
    """Service jobs fixture."""
    from renku.service.entrypoint import create_app

    flask_app = create_app()

    testing_client = flask_app.test_client()
    testing_client.testing = True

    ctx = flask_app.app_context()
    ctx.push()

    yield testing_client, identity_headers, flask_app.config.get("cache")

    ctx.pop()


def integration_repo_path(headers, project_id, url_components):
    """Constructs integration repo path."""
    from renku.service.serializers.headers import RequiredIdentityHeaders
    from renku.service.utils import make_project_path

    user = RequiredIdentityHeaders().load(headers)
    project = {
        "project_id": project_id,
        "owner": url_components.owner,
        "name": url_components.name,
    }

    project_path = make_project_path(user, project)
    return project_path


@contextlib.contextmanager
def integration_repo(headers, project_id, url_components):
    """With integration repo helper."""
    from renku.core.utils.contexts import chdir

    with chdir(integration_repo_path(headers, project_id, url_components)):
        repo = Repo(".")
        repo.heads.master.checkout()

        yield repo

        if integration_repo_path(headers, project_id, url_components).exists():
            repo.git.reset("--hard")
            repo.heads.master.checkout()
            repo.git.reset("--hard")
            repo.git.clean("-xdf")


@pytest.fixture(scope="module")
def identity_headers():
    """Get authentication headers."""
    from renku.service.serializers.headers import JWT_TOKEN_SECRET

    jwt_data = {
        "jti": "12345",
        "exp": int(time.time()) + 1e6,
        "nbf": 0,
        "iat": 1595317694,
        "iss": "https://stable.dev.renku.ch/auth/realms/Renku",
        "aud": ["renku"],
        "sub": "9ab2fc80-3a5c-426d-ae78-56de01d214df",
        "typ": "ID",
        "azp": "renku",
        "nonce": "12345",
        "auth_time": 1595317694,
        "session_state": "12345",
        "acr": "1",
        "email_verified": False,
        "preferred_username": "andi@bleuler.com",
        "given_name": "Andreas",
        "family_name": "Bleuler",
        "name": "Andreas Bleuler",
        "email": "andi@bleuler.com",
    }

    headers = {
        "Content-Type": "application/json",
        "Renku-User": jwt.encode(jwt_data, JWT_TOKEN_SECRET, algorithm="HS256"),
        "Authorization": "Bearer {0}".format(os.getenv("IT_OAUTH_GIT_TOKEN")),
    }

    return headers


@pytest.fixture(scope="module")
def authentication_headers_raw():
    """Get authentication headers without renku user identification."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {0}".format(os.getenv("IT_OAUTH_GIT_TOKEN")),
    }

    return headers


@pytest.fixture(scope="module")
def authentication_headers(authentication_headers_raw):
    """Get authentication headers."""
    identification = {
        "Renku-User-Id": "b4b4de0eda0f471ab82702bd5c367fa7",
        "Renku-User-FullName": "Just Sam",
        "Renku-User-Email": "contact@justsam.io",
    }

    return {**authentication_headers_raw, **identification}


@pytest.fixture(scope="module")
def integration_lifecycle(svc_client, mock_redis, identity_headers):
    """Setup and teardown steps for integration tests."""
    from renku.core.models.git import GitURL

    url_components = GitURL.parse(IT_REMOTE_REPO_URL)

    payload = {"git_url": IT_REMOTE_REPO_URL, "depth": 0}

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)

    assert response
    assert {"result"} == set(response.json.keys())

    project_id = response.json["result"]["project_id"]
    assert isinstance(uuid.UUID(project_id), uuid.UUID)

    yield svc_client, identity_headers, project_id, url_components

    # Teardown step: Delete all branches except master (if needed).
    if integration_repo_path(identity_headers, project_id, url_components).exists():
        with integration_repo(identity_headers, project_id, url_components) as repo:
            try:
                repo.remote().push(refspec=(":{0}".format(repo.active_branch.name)))
            except GitCommandError:
                pass


@pytest.fixture
def svc_client_setup(integration_lifecycle):
    """Service client setup."""
    svc_client, headers, project_id, url_components = integration_lifecycle

    with integration_repo(headers, project_id, url_components) as repo:
        repo.git.checkout("master")

        new_branch = uuid.uuid4().hex
        current = repo.create_head(new_branch)
        current.checkout()

        yield svc_client, deepcopy(headers), project_id, url_components

        if integration_repo_path(headers, project_id, url_components).exists():
            # NOTE: Some tests delete the repo
            repo.git.checkout("master")
            repo.git.branch("-D", current)


@pytest.fixture
def svc_client_with_user(svc_client_cache):
    """Service client with a predefined user."""
    from renku.service.serializers.headers import encode_b64

    svc_client, headers, cache = svc_client_cache

    user_id = encode_b64(secure_filename("9ab2fc80-3a5c-426d-ae78-56de01d214df"))
    user = cache.ensure_user({"user_id": user_id})

    yield svc_client, headers, cache, user


@pytest.fixture
def svc_client_with_repo(svc_client_setup):
    """Service client with a remote repository."""
    svc_client, headers, project_id, url_components = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert response.json["result"]

    yield svc_client, deepcopy(headers), project_id, url_components


@pytest.fixture
def svc_synced_client(svc_client_with_user, real_sync):
    """Renku service client with remote sync."""
    yield svc_client_with_user


@pytest.fixture
def svc_client_with_templates(svc_client, mock_redis, identity_headers, template):
    """Setup and teardown steps for templates tests."""

    yield svc_client, identity_headers, template


@pytest.fixture()
def svc_client_templates_creation(svc_client_with_templates):
    """Setup and teardown steps for templates tests."""
    from renku.core.utils.requests import retry
    from renku.core.utils.scm import normalize_to_ascii

    svc_client, authentication_headers, template = svc_client_with_templates
    parameters = []
    for parameter in template["metadata"].keys():
        parameters.append({"key": parameter, "value": template["metadata"][parameter]})

    payload = {
        **template,
        "identifier": template["id"],
        "parameters": parameters,
        "project_name": f"Test renku-core {uuid.uuid4().hex[:12]}",
        "project_namespace": "contact",
        "project_repository": "https://dev.renku.ch/gitlab",
    }

    # clenup by invoking the GitLab delete API
    # TODO: consider using the project delete endpoint once implemented
    def remove_project():
        project_slug = "{0}/{1}".format(payload["project_namespace"], normalize_to_ascii(payload["project_name"]))

        project_slug_encoded = urllib.parse.quote(project_slug, safe="")
        project_delete_url = "{0}/api/v4/projects/{1}".format(payload["project_repository"], project_slug_encoded)

        with retry() as session:
            session.delete(url=project_delete_url, headers=authentication_headers)

        return True

    yield svc_client, authentication_headers, payload, remove_project


@pytest.fixture
def svc_protected_repo(svc_client, identity_headers):
    """Service client with migrated remote protected repository."""
    payload = {
        "git_url": IT_PROTECTED_REMOTE_REPO_URL,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    data = {
        "project_id": response.json["result"]["project_id"],
        "skip_template_update": True,
        "skip_docker_update": True,
    }
    svc_client.post("/cache.migrate", data=json.dumps(data), headers=identity_headers)

    yield svc_client, identity_headers, payload, response


@pytest.fixture
def svc_protected_old_repo(svc_synced_client):
    """Service client with remote protected repository."""
    svc_client, identity_headers, cache, user = svc_synced_client

    payload = {
        "git_url": IT_PROTECTED_REMOTE_REPO_URL,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    project_id = response.json["result"]["project_id"]

    yield svc_client, identity_headers, project_id, cache, user


@pytest.fixture(
    params=[
        {
            "url": "/cache.files_list",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {"url": "/cache.files_upload", "allowed_method": "POST", "headers": {}},
        {
            "url": "/cache.project_clone",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {
            "url": "/cache.project_list",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {
            "url": "/datasets.add",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {
            "url": "/datasets.create",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {
            "url": "/templates.read_manifest",
            "allowed_method": "GET",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
        {
            "url": "/templates.create_project",
            "allowed_method": "POST",
            "headers": {"Content-Type": "application/json", "accept": "application/json",},
        },
    ]
)
def service_allowed_endpoint(request, svc_client, mock_redis):
    """Ensure allowed methods and correct headers."""
    methods = {
        "GET": svc_client.get,
        "POST": svc_client.post,
        "HEAD": svc_client.head,
        "PUT": svc_client.put,
        "DELETE": svc_client.delete,
        "OPTIONS": svc_client.options,
        "TRACE": svc_client.trace,
        "PATCH": svc_client.patch,
    }

    yield methods, request.param, svc_client


@pytest.fixture
def service_job(svc_client, mock_redis):
    """Ensure correct environment during testing of service jobs."""
    old_environ = dict(os.environ)

    os.environ["RENKU_SVC_CLEANUP_TTL_FILES"] = "0"
    os.environ["RENKU_SVC_CLEANUP_TTL_PROJECTS"] = "0"

    try:
        yield svc_client, mock_redis
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


@pytest.fixture
def unlink_file_setup(svc_client_with_repo):
    """Setup for testing of unlinking of a file."""
    svc_client, headers, project_id, _ = svc_client_with_repo

    payload = make_dataset_add_payload(project_id, [("file_path", "README.md")],)
    response = svc_client.post("/datasets.add", data=json.dumps(payload), headers=headers,)
    assert 200 == response.status_code

    unlink_payload = {
        "project_id": project_id,
        "name": response.json["result"]["name"],
        "include_filters": [response.json["result"]["files"][0]["file_path"]],
    }

    yield svc_client, headers, unlink_payload


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

            return [Annotation(id="_:annotation", source="Dummy Cmdline Hook", body="dummy cmdline hook body")]

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

            return [Annotation(id="_:annotation", source="Dummy ProcessRun Hook", body="dummy ProcessRun hook body")]

    return _ProcessRunAnnotations()


@pytest.fixture
def no_lfs_size_limit(client):
    """Configure environment track all files in LFS independent of size."""
    client.set_value("renku", "lfs_threshold", "0b")
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")
    yield client


@pytest.fixture
def large_file(tmp_path, client):
    """A file larger than the minimum LFS file size."""
    path = tmp_path / "large-file"
    with open(path, "w") as file_:
        file_.seek(client.minimum_lfs_file_size)
        file_.write("some data")

    yield path


@pytest.fixture()
def ctrl_init(svc_client_cache):
    """Cache object for controller testing."""
    from renku.service.serializers.headers import RequiredIdentityHeaders

    _, headers, cache = svc_client_cache

    headers["Authorization"] = "Bearer not-a-token"
    user_data = RequiredIdentityHeaders().load(headers)

    return cache, user_data


@pytest.fixture
def reset_environment(svc_client, mock_redis):
    """Restore environment variable to their values before test execution."""
    current_environment = os.environ.copy()

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(current_environment)
