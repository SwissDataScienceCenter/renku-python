# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
"""Module tests."""

import pytest

from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.project.project_properties import project_properties
from renku.domain_model.project import Project
from renku.domain_model.provenance.agent import Person


def test_version():
    """Test version import."""
    from renku import __version__

    assert __version__


def test_minimum_version(mocker, tmpdir, monkeypatch):
    """Test minimum version required."""
    monkeypatch.setenv("RENKU_SKIP_MIN_VERSION_CHECK", "0")

    def mock_database_project(project):
        def mocked_getter(self, key):
            if key == "project":
                return project
            return getattr(self, key)

        return mocked_getter

    with tmpdir.as_cwd(), project_properties.with_path(tmpdir):
        # NOTE: Check doesn't raise with identical version
        mocker.patch("renku.domain_model.project.Project.minimum_renku_version", "1.0.0")
        project = Project(creator=Person(name="John Doe", email="jd@example.com"), name="testproject")
        mocker.patch("renku.infrastructure.database.Database.__getitem__", mock_database_project(project))
        mocker.patch("renku.version.__version__", "1.0.0")

        try:
            Command().command(lambda: "").with_database(write=False).build().execute()
        except errors.MinimumVersionError:
            pytest.fail("Shouldn't raise an exception.")

        # NOTE: Check doesn't raise with higher current version
        mocker.patch("renku.domain_model.project.Project.minimum_renku_version", "0.9.0")
        project.minimum_renku_version = "0.9.0"

        try:
            Command().command(lambda: "").with_database(write=False).build().execute()
        except errors.MinimumVersionError:
            pytest.fail("Shouldn't raise an exception.")
        assert "0.9.0" == project.minimum_renku_version

        # NOTE: Check that accessing a project with newer version fails
        project.minimum_renku_version = "1.1.0"

        with pytest.raises(errors.MinimumVersionError) as e:
            Command().command(lambda: "").with_database(write=False).build().execute()

        assert "1.0.0" == str(e.value.current_version)
        assert "1.1.0" == str(e.value.minimum_version)

        # NOTE: Check that accessing an old project with a newer minimum version works
        mocker.patch("renku.domain_model.project.Project.minimum_renku_version", "1.1.0")
        mocker.patch("renku.version.__version__", "1.1.0")
        project.minimum_renku_version = "0.9.0"

        try:
            Command().command(lambda: "").with_database(write=False).build().execute()
        except errors.MinimumVersionError:
            pytest.fail("Shouldn't raise an exception.")

        # NOTE: Assert that minimum_version is increased on write
        mocker.patch("renku.domain_model.project.Project.minimum_renku_version", "1.0.0")
        mocker.patch("renku.version.__version__", "1.1.0")
        project.minimum_renku_version = "0.9.0"

        try:
            Command().command(lambda: "").with_database(write=True).build().execute()
        except errors.MinimumVersionError:
            pytest.fail("Shouldn't raise an exception.")
        assert "1.0.0" == project.minimum_renku_version
