# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 -Swiss Data Science Center (SDSC)
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
"""Renku service project remote abstraction tests."""

import pytest
from marshmallow import ValidationError

import renku
from renku.core.commands.migrate import migrations_check
from renku.service.controllers.utils.remote_project import RemoteProject
from tests.utils import retry_failed


def test_project_metadata_remote():
    """Check path construction for remote project metadata path."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }
    request_data = {"git_url": "https://dev.renku.ch/gitlab/renku-python-integration-tests/import-me"}
    ctrl = RemoteProject(user_data, request_data)
    path = ctrl.remote_url

    assert path
    assert "https" == path.scheme
    assert "oauth2:123@dev.renku.ch" == path.netloc
    assert "/gitlab/renku-python-integration-tests/import-me" == path.path
    assert "" == path.params
    assert "" == path.query
    assert "" == path.fragment


def test_project_metadata_custom_remote():
    """Check path construction for remote project metadata path."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }

    request_data = {
        "git_url": "https://dev.renku.ch/gitlab/renku-python-integration-tests/import-me",
        "ref": "my-branch",
    }

    ctrl = RemoteProject(user_data, request_data)
    ref = ctrl.ctx["ref"]

    assert request_data["ref"] == ref


def test_project_metadata_remote_err():
    """Check exception raised during path construction for remote project metadata path."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }
    request_data = {"git_url": "/dev.renku.ch/gitlab/renku-python-integration-tests/import-me"}

    with pytest.raises(ValidationError):
        RemoteProject(user_data, request_data)

    request_data["git_url"] = "httpz://dev.renku.ch/gitlab/renku-python-integration-tests/import-me"

    with pytest.raises(ValidationError):
        RemoteProject(user_data, request_data)


@pytest.mark.integration
@pytest.mark.service
@retry_failed
def test_remote_project_context():
    """Check remote project context manager."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }
    request_data = {"git_url": "https://dev.renku.ch/gitlab/renku-python-integration-tests/import-me"}
    ctrl = RemoteProject(user_data, request_data)

    with ctrl.remote() as project_path:
        assert project_path
        result = migrations_check().build().execute().output
        assert result["core_renku_version"] == renku.__version__
        assert result["project_renku_version"] == "pre-0.11.0"
        assert result["core_compatibility_status"]["migration_required"] is True
        assert result["template_status"]["newer_template_available"] is False
        assert result["template_status"]["project_template_version"] is None
        assert result["template_status"]["latest_template_version"] is None
        assert result["template_status"]["template_source"] is None
        assert result["template_status"]["template_ref"] is None
        assert result["template_status"]["template_id"] is None
        assert result["template_status"]["automated_template_update"] is False
        assert result["dockerfile_renku_status"]["automated_dockerfile_update"] is False
        assert result["project_supported"] is True
