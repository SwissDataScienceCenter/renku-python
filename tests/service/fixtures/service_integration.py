# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku service fixtures for integration testing."""
import contextlib
import json
import os
import shutil
import uuid
from copy import deepcopy

import pytest
from git import GitCommandError


@contextlib.contextmanager
def _mock_cache_sync(repo):
    # mock sync reset
    from renku.service.controllers.api import mixins

    current_head = repo.head.ref

    def _mocked_repo_reset(self, project):
        """Mock repo reset to work with mocked renku save."""
        repo.git.reset("--hard", current_head)

    reset_repo_function = mixins.ReadOperationMixin.reset_local_repo
    mixins.ReadOperationMixin.reset_local_repo = _mocked_repo_reset

    try:
        yield
    finally:
        mixins.ReadOperationMixin.reset_local_repo = reset_repo_function


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
    from git import Repo

    from renku.core.utils.contexts import chdir

    repo_path = integration_repo_path(headers, project_id, url_components)
    with chdir(repo_path):
        repo = Repo(repo_path)
        repo.heads.master.checkout()

        yield repo

        if integration_repo_path(headers, project_id, url_components).exists():
            repo.git.reset("--hard")
            repo.heads.master.checkout()
            repo.git.reset("--hard")
            repo.git.clean("-xdf")


@pytest.fixture(scope="module")
def integration_lifecycle(svc_client, mock_redis, identity_headers, it_remote_repo_url):
    """Setup and teardown steps for integration tests."""
    from renku.core.models.git import GitURL

    url_components = GitURL.parse(it_remote_repo_url)

    payload = {"git_url": it_remote_repo_url, "depth": 0}

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

        with _mock_cache_sync(repo):
            yield svc_client, deepcopy(headers), project_id, url_components, repo

        if integration_repo_path(headers, project_id, url_components).exists():
            # NOTE: Some tests delete the repo
            repo.git.checkout("master")
            repo.git.branch("-D", current)


@pytest.fixture
def svc_client_with_repo(svc_client_setup):
    """Service client with a remote repository."""
    svc_client, headers, project_id, url_components, repo = svc_client_setup

    response = svc_client.post(
        "/cache.migrate", data=json.dumps(dict(project_id=project_id, skip_docker_update=True)), headers=headers
    )
    assert response.json["result"]

    with _mock_cache_sync(repo):
        yield svc_client, deepcopy(headers), project_id, url_components


@pytest.fixture
def svc_protected_repo(svc_client, identity_headers, it_protected_repo_url):
    """Service client with migrated remote protected repository."""
    from renku.core.models.git import GitURL

    payload = {
        "git_url": it_protected_repo_url,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)

    data = {
        "project_id": response.json["result"]["project_id"],
        "skip_template_update": True,
        "skip_docker_update": True,
    }
    svc_client.post("/cache.migrate", data=json.dumps(data), headers=identity_headers)

    url_components = GitURL.parse(it_protected_repo_url)

    with integration_repo(identity_headers, response.json["result"]["project_id"], url_components) as repo:
        with _mock_cache_sync(repo):
            yield svc_client, identity_headers, payload, response


@pytest.fixture
def svc_protected_old_repo(svc_synced_client, it_protected_repo_url):
    """Service client with remote protected repository."""
    svc_client, identity_headers, cache, user = svc_synced_client

    payload = {
        "git_url": it_protected_repo_url,
        "depth": 0,
    }

    response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers)
    project_id = response.json["result"]["project_id"]

    yield svc_client, identity_headers, project_id, cache, user


@pytest.fixture()
def local_remote_repository(svc_client, tmp_path, mock_redis, identity_headers, real_sync):
    """Client with a local remote to test pushes."""
    from click.testing import CliRunner
    from git.config import GitConfigParser, get_config_path
    from marshmallow import post_load, pre_load

    from renku.cli import cli
    from renku.core.utils.contexts import chdir
    from renku.service.serializers import cache

    # NOTE: prevent service from adding an auth token as it doesn't work with local repos
    def _no_auth_format(self, data, **kwargs):
        data["url_with_auth"] = data["git_url"]
        return data

    orig_format_url = cache.ProjectCloneContext.format_url

    cache.ProjectCloneContext.format_url = post_load(_no_auth_format)

    # NOTE: mock owner/project so service is happy
    def _mock_owner(self, data, **kwargs):
        data["owner"] = "dummy"

        data["name"] = "project"

        return data

    orig_set_owner = cache.ProjectCloneContext.set_owner_name

    cache.ProjectCloneContext.set_owner_name = pre_load(_mock_owner)

    remote_repo = tmp_path / "remote_repo"
    remote_repo.mkdir()

    home = tmp_path / "user_home"
    home.mkdir()
    old_home = os.environ.get("HOME", "")
    old_xdg_home = os.environ.get("XDG_CONFIG_HOME", "")

    try:
        # NOTE: fake user home directory
        os.environ["HOME"] = str(home)
        os.environ["XDG_CONFIG_HOME"] = str(home)

        with GitConfigParser(get_config_path("global"), read_only=False) as global_config:
            global_config.set_value("user", "name", "Renku @ SDSC")
            global_config.set_value("user", "email", "renku@datascience.ch")

        # NOTE: init "remote" repo
        runner = CliRunner()
        with chdir(remote_repo):
            result = runner.invoke(cli, ["init", ".", "--template-id", "python-minimal"], "\n", catch_exceptions=False)
            assert 0 == result.exit_code

    finally:
        os.environ["HOME"] = old_home
        os.environ["XDG_CONFIG_HOME"] = old_xdg_home
        try:
            shutil.rmtree(home)
        except OSError:  # noqa: B014
            pass

        payload = {"git_url": f"file://{remote_repo}", "depth": 0}

        response = svc_client.post("/cache.project_clone", data=json.dumps(payload), headers=identity_headers,)

        assert response

        assert {"result"} == set(response.json.keys())

        project_id = response.json["result"]["project_id"]
        assert isinstance(uuid.UUID(project_id), uuid.UUID)

    try:
        yield svc_client, identity_headers, project_id, remote_repo
    finally:
        cache.ProjectCloneContext.format_url = orig_format_url
        cache.ProjectCloneContext.set_owner_name = orig_set_owner

        try:
            shutil.rmtree(remote_repo)
        except OSError:  # noqa: B014
            pass
