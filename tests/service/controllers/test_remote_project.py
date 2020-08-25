# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service cache views."""
import pytest

import renku
from renku.core.commands.migrate import migrations_check, migrations_versions
from renku.service.controllers.remote_project import RemoteProject


def test_project_metadata_remote():
    """Check path construction for remote project metadata path."""
    ctrl = RemoteProject("https://dev.renku.ch/gitlab/contact/import-me")
    path = ctrl.project_metadata_path

    assert path
    assert "https" == path.scheme
    assert "dev.renku.ch" == path.netloc
    assert "/gitlab/contact/import-me/raw/master/.renku/metadata.yml" == path.path
    assert "" == path.params
    assert "" == path.query
    assert "" == path.fragment


def test_project_metadata_custom_remote():
    """Check path construction for remote project metadata path."""
    ctrl = RemoteProject("https://dev.renku.ch/gitlab/contact/import-me", branch="my-branch")
    path = ctrl.project_metadata_path

    assert path
    assert "https" == path.scheme
    assert "dev.renku.ch" == path.netloc
    assert "/gitlab/contact/import-me/raw/my-branch/.renku/metadata.yml" == path.path
    assert "" == path.params
    assert "" == path.query
    assert "" == path.fragment


def test_project_metadata_remote_err():
    """Check exception raised during path construction for remote project metadata path."""
    ctrl = RemoteProject("/dev.renku.ch/gitlab/contact/import-me")

    with pytest.raises(ValueError):
        _ = ctrl.project_metadata_path

    ctrl = RemoteProject("httpz://dev.renku.ch/gitlab/contact/import-me")
    path = ctrl.project_metadata_path
    assert "https" == path.scheme


def test_remote_project_context():
    """Check remote project context manager."""
    ctrl = RemoteProject("https://dev.renku.ch/gitlab/contact/import-me")

    with ctrl.remote() as project_path:
        latest_version, project_version = migrations_versions()
        assert renku.__version__ == latest_version
        assert "pre-0.11.0" == project_version

        migration_required, project_supported = migrations_check()
        assert migration_required is True
        assert project_supported is True
