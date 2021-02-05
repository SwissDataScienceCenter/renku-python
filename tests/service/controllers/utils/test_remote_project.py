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
from renku.core.commands.migrate import migrations_check, migrations_versions
from renku.service.controllers.utils.remote_project import RemoteProject


def test_project_metadata_remote():
    """Check path construction for remote project metadata path."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }
    request_data = {"git_url": "https://dev.renku.ch/gitlab/contact/import-me"}
    ctrl = RemoteProject(user_data, request_data)
    path = ctrl.remote_url

    assert path
    assert "https" == path.scheme
    assert "oauth2:123@dev.renku.ch" == path.netloc
    assert "/gitlab/contact/import-me" == path.path
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

    request_data = {"git_url": "https://dev.renku.ch/gitlab/contact/import-me", "ref": "my-branch"}

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
    request_data = {"git_url": "/dev.renku.ch/gitlab/contact/import-me"}

    with pytest.raises(ValidationError):
        RemoteProject(user_data, request_data)

    request_data["git_url"] = "httpz://dev.renku.ch/gitlab/contact/import-me"

    with pytest.raises(ValidationError):
        RemoteProject(user_data, request_data)


def test_remote_project_context():
    """Check remote project context manager."""
    user_data = {
        "fullname": "testing user",
        "email": "testing@user.com",
        "token": "123",
    }
    request_data = {"git_url": "https://dev.renku.ch/gitlab/contact/import-me"}
    ctrl = RemoteProject(user_data, request_data)

    with ctrl.remote() as project_path:
        assert project_path
        latest_version, project_version = migrations_versions().build().execute().output
        assert renku.__version__ == latest_version
        assert "pre-0.11.0" == project_version

        (
            migration_required,
            project_supported,
            template_update_possible,
            current_template_version,
            latest_template_version,
            automated_update_possible,
            docker_update_possible,
        ) = (migrations_check().build().execute().output)
        assert migration_required is True
        assert template_update_possible is False
        assert current_template_version is None
        assert latest_template_version is None
        assert automated_update_possible is False
        assert docker_update_possible is False
        assert project_supported is True
