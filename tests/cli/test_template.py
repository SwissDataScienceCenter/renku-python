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
import sys

import pytest
from packaging.version import Version

from renku.cli import cli
from tests.utils import format_result_exception, write_and_commit_file

TEMPLATES_URL = "https://github.com/SwissDataScienceCenter/renku-project-template"


@pytest.mark.serial
def test_template_list(isolated_runner):
    """Test list Renku templates."""
    command = ["template", "ls"]
    argv = sys.argv
    sys.argv = command

    try:
        result = isolated_runner.invoke(cli, command)

        assert 0 == result.exit_code, format_result_exception(result)
        assert "python-minimal" in result.output
    finally:
        sys.argv = argv


@pytest.mark.integration
def test_template_list_from_source(isolated_runner):
    """Test list templates from other sources."""
    command = ["template", "ls"]
    argv = sys.argv
    sys.argv = command

    try:
        result = isolated_runner.invoke(cli, command + ["--source", TEMPLATES_URL])

        assert 0 == result.exit_code, format_result_exception(result)
        assert "python-minimal" in result.output
        assert "julia-minimal" in result.output

        result = isolated_runner.invoke(cli, command + ["-s", TEMPLATES_URL, "--reference", "0.1.10"])

        assert 0 == result.exit_code, format_result_exception(result)
        assert "python-minimal" in result.output
        assert "julia-minimal" not in result.output
    finally:
        sys.argv = argv


def test_template_show(isolated_runner):
    """Test show detailed template info."""
    command = ["template", "show"]
    argv = sys.argv
    sys.argv = command

    try:
        result = isolated_runner.invoke(cli, command + ["--template", "python-minimal"])

        assert 0 == result.exit_code, format_result_exception(result)
        assert "Name: Basic Python (3.9) Project" in result.output
    finally:
        sys.argv = argv


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
    write_and_commit_file(client.repository, "Dockerfile", "my-modifications")

    result = runner.invoke(cli, ["template", "set", "--force", "--template-id", "R-minimal"])

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "R-minimal" == client.project.template_id
    assert "my-modifications" not in (client.path / "Dockerfile").read_text()
    assert not client.repository.is_dirty(untracked_files=True)


@pytest.mark.parametrize("overwrite, found", [["y", False], ["n", True]])
def test_template_set_interactive(runner, client, client_database_injection_manager, overwrite, found):
    """Test setting a template in interactive mode."""
    write_and_commit_file(client.repository, "Dockerfile", "my-modifications")

    result = runner.invoke(cli, ["template", "set", "-f", "-t", "R-minimal", "-i"], input=f"{overwrite}\n" * 420)

    assert 0 == result.exit_code, format_result_exception(result)
    with client_database_injection_manager(client):
        assert "R-minimal" == client.project.template_id
    assert ("my-modifications" in (client.path / "Dockerfile").read_text()) is found
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


def test_template_set_dry_run(runner, client):
    """Test set dry-run doesn't make any changes."""
    commit_sha_before = client.repository.head.commit.hexsha

    result = runner.invoke(cli, ["template", "set", "-f", "-t", "R-minimal", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not client.repository.is_dirty()
    assert commit_sha_before == client.repository.head.commit.hexsha


@pytest.mark.integration
def test_template_update(runner, client, client_database_injection_manager):
    """Test updating a template."""
    result = runner.invoke(
        cli,
        ["template", "set", "-f", "-t", "python-minimal", "-s", TEMPLATES_URL, "-r", "0.1.10"]
        + ["-p", "description=fixed-version"],
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


@pytest.mark.integration
def test_template_update_dry_run(runner, client):
    """Test update dry-run doesn't make any changes."""
    result = runner.invoke(
        cli,
        ["template", "set", "-f", "-t", "python-minimal", "-s", TEMPLATES_URL, "-r", "0.1.10"]
        + ["-p", "description=fixed-version"],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_before = client.repository.head.commit.hexsha

    result = runner.invoke(cli, ["template", "update", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not client.repository.is_dirty()
    assert commit_sha_before == client.repository.head.commit.hexsha


def test_git_hook_for_modified_immutable_template_files(runner, client_with_template):
    """Test check for modified immutable template files."""
    from renku.core.utils.contexts import chdir

    (client_with_template.path / "immutable.file").write_text("Locally modified immutable files")

    with chdir(client_with_template.path):
        result = runner.invoke(cli, ["check-immutable-template-files", "Dockerfile"])
        assert result.exit_code == 0, result.output

        result = runner.invoke(cli, ["check-immutable-template-files", "immutable.file"])
        assert result.exit_code == 1
        assert "immutable.file" in result.output
