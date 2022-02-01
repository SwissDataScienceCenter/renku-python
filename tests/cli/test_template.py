# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Test ``template`` command."""

import re

import pytest
from packaging.version import Version

from renku.cli import cli
from tests.utils import format_result_exception, write_and_commit_file


@pytest.mark.skip(reason="sys.argv is not set correctly from the tests")
def test_template_list(isolated_runner):
    """Test list Renku templates."""
    result = isolated_runner.invoke(cli, ["template", "ls"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "python-minimal" in result.output


@pytest.mark.skip(reason="sys.argv is not set correctly from the tests")
@pytest.mark.integration
def test_template_list_from_source(isolated_runner):
    """Test list templates from other sources."""
    url = "https://github.com/SwissDataScienceCenter/renku-project-template"

    result = isolated_runner.invoke(cli, ["template", "ls", "--template-source", url])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "python-minimal" in result.output
    assert "julia-minimal" in result.output

    result = isolated_runner.invoke(cli, ["template", "ls", "--template-source", url, "--template-ref", "0.1.10"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "python-minimal" in result.output
    assert "julia-minimal" not in result.output


def test_template_set_outside_a_renku_project(isolated_runner):
    """Test setting template outside a project is not possible."""
    result = isolated_runner.invoke(cli, ["template", "set"])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "is not a renku repository" in result.output


def test_template_update_outside_a_renku_project(isolated_runner):
    """Test updating template outside a project is not possible."""
    result = isolated_runner.invoke(cli, ["template", "update"])

    assert 2 == result.exit_code, format_result_exception(result)
    assert "is not a renku repository" in result.output


def test_template_set_failure(runner, client, client_database_injection_manager):
    """Test setting template in a project with template fails."""
    result = runner.invoke(cli, ["template", "set"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "Project already has a template" in result.output
    with client_database_injection_manager(client):
        assert "python-minimal" == client.project.template_id


def test_template_set(runner, client, client_database_injection_manager):
    """Test setting a new template in a project."""
    result = runner.invoke(cli, ["template", "set", "--force", "--template-id", "R-minimal"])

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "R-minimal" == client.project.template_id


def test_template_set_overwrites_modified(runner, client, client_database_injection_manager):
    """Test setting a new template in a project overwrite modified files."""
    write_and_commit_file(client.repository, "Dockerfile", "my-changes")

    result = runner.invoke(cli, ["template", "set", "--force", "--template-id", "R-minimal"])

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "R-minimal" == client.project.template_id
    assert "my-changes" not in (client.path / "Dockerfile").read_text()
    assert not client.repository.is_dirty(untracked_files=True)


@pytest.mark.parametrize("overwrite, found", [["y", False], ["n", True]])
def test_template_set_interactive(runner, client, client_database_injection_manager, overwrite, found):
    """Test setting a template in interactive mode."""
    write_and_commit_file(client.repository, "Dockerfile", "my-changes")

    result = runner.invoke(cli, ["template", "set", "-f", "-t", "R-minimal", "-i"], input=f"{overwrite}\n" * 420)

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "R-minimal" == client.project.template_id
    assert ("my-changes" in (client.path / "Dockerfile").read_text()) is found
    assert not client.repository.is_dirty(untracked_files=True)


def test_template_set_preserve_renku_version(runner, client):
    """Test setting a template and overwriting Dockerfile still preserves Renku version."""
    content = (client.path / "Dockerfile").read_text()
    new_content = re.sub(r"^\s*ARG RENKU_VERSION=(.+)$", "ARG RENKU_VERSION=0.0.42", content, flags=re.MULTILINE)
    write_and_commit_file(client.repository, "Dockerfile", new_content)

    result = runner.invoke(cli, ["template", "set", "-f", "-t", "R-minimal", "--interactive"], input="y\n" * 420)

    assert 0 == result.exit_code, format_result_exception(result)

    content = (client.path / "Dockerfile").read_text()

    assert new_content != content
    assert "ARG RENKU_VERSION=0.0.42" in content


@pytest.mark.integration
def test_template_update(runner, client, client_database_injection_manager):
    """Test updating a template."""
    url = "https://github.com/SwissDataScienceCenter/renku-project-template"

    result = runner.invoke(
        cli,
        ["template", "set", "-f", "-t", "python-minimal", "-s", url, "-r", "0.1.10", "-p", "description=fixed-version"],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "python-minimal" == client.project.template_id
        assert "0.1.10" == client.project.template_ref
        assert "6c59d8863841baeca8f30062fd16c650cf67da3b" == client.project.template_version

    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" not in result.output
    with client_database_injection_manager(client):
        assert "python-minimal" == client.project.template_id
        assert Version(client.project.template_ref) > Version("0.1.10")
        assert "6c59d8863841baeca8f30062fd16c650cf67da3b" != client.project.template_version

    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" in result.output


def test_template_update_latest_version(runner, client):
    """Test updating template that is the latest version."""
    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" in result.output
