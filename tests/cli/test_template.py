# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
from pathlib import Path

import pytest
from packaging.version import Version

from renku.core.util.contexts import chdir
from renku.core.util.yaml import write_yaml
from renku.domain_model.project_context import project_context
from renku.domain_model.template import TemplateMetadata, TemplateParameter
from renku.infrastructure.repository import Actor, Repository
from renku.ui.cli import cli
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

        result = isolated_runner.invoke(cli, command + ["-s", TEMPLATES_URL, "--reference", "0.3.2"])

        assert 0 == result.exit_code, format_result_exception(result)
        assert "python-minimal" in result.output
        assert "julia-minimal" in result.output
    finally:
        sys.argv = argv


def test_template_show(isolated_runner):
    """Test show detailed template info."""
    command = ["template", "show"]
    argv = sys.argv
    sys.argv = command

    try:
        result = isolated_runner.invoke(cli, command + ["R-minimal"])

        assert 0 == result.exit_code, format_result_exception(result)
        assert re.search("^Name: Basic R (.*) Project$", result.output, re.MULTILINE) is not None
    finally:
        sys.argv = argv


def test_template_show_no_id(runner, project):
    """Test show current project's template."""
    result = runner.invoke(cli, ["template", "show"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert re.search("^Name: Basic Python (.*) Project$", result.output, re.MULTILINE) is not None


def test_template_show_no_id_outside_project(isolated_runner):
    """Test show with no id."""
    command = ["template", "show"]
    argv = sys.argv
    sys.argv = command

    try:
        result = isolated_runner.invoke(cli, command)

        assert 2 == result.exit_code, format_result_exception(result)
        assert "No Renku project found" in result.output
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


def test_template_set_failure(runner, project, with_injection):
    """Test setting template in a project with template fails."""
    result = runner.invoke(cli, ["template", "set"])

    assert 1 == result.exit_code, format_result_exception(result)
    assert "Project already has a template" in result.output
    with with_injection():
        assert "python-minimal" == project_context.project.template_id


def test_template_set(runner, project, with_injection):
    """Test setting a new template in a project."""
    from renku.version import __template_version__

    result = runner.invoke(cli, ["template", "set", "--force", "R-minimal"])

    assert 0 == result.exit_code, format_result_exception(result)
    with with_injection():
        assert "R-minimal" == project_context.project.template_id
        assert __template_version__ == project_context.project.template_version
        assert __template_version__ == project_context.project.template_ref

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_template_set_overwrites_modified(runner, project, with_injection):
    """Test setting a new template in a project overwrite modified files."""
    write_and_commit_file(project.repository, "Dockerfile", "my-modifications")

    result = runner.invoke(cli, ["template", "set", "--force", "R-minimal"])

    assert 0 == result.exit_code, format_result_exception(result)
    with with_injection():
        assert "R-minimal" == project_context.project.template_id
    assert "my-modifications" not in (project.path / "Dockerfile").read_text()
    assert not project.repository.is_dirty(untracked_files=True)


@pytest.mark.parametrize("overwrite, found", [["y", False], ["n", True]])
def test_template_set_interactive(runner, project, with_injection, overwrite, found):
    """Test setting a template in interactive mode."""
    write_and_commit_file(project.repository, "Dockerfile", "my-modifications")

    result = runner.invoke(cli, ["template", "set", "-f", "R-minimal", "-i"], input=f"{overwrite}\n" * 420)

    assert 0 == result.exit_code, format_result_exception(result)
    with with_injection():
        assert "R-minimal" == project_context.project.template_id
    assert ("my-modifications" in (project.path / "Dockerfile").read_text()) is found
    assert not project.repository.is_dirty(untracked_files=True)


def test_template_set_preserve_renku_version(runner, project):
    """Test setting a template and overwriting Dockerfile still preserves Renku version."""
    content = (project.path / "Dockerfile").read_text()
    new_content = re.sub(r"^\s*ARG RENKU_VERSION=(.+)$", "ARG RENKU_VERSION=0.0.42", content, flags=re.MULTILINE)
    write_and_commit_file(project.repository, "Dockerfile", new_content)

    result = runner.invoke(cli, ["template", "set", "-f", "R-minimal", "--interactive"], input="y\n" * 420)

    assert 0 == result.exit_code, format_result_exception(result)

    content = (project.path / "Dockerfile").read_text()

    assert new_content != content
    assert "ARG RENKU_VERSION=0.0.42" in content


@pytest.mark.integration
def test_template_set_uses_renku_version_when_non_existing(tmpdir, runner):
    """Test setting a template on a project with no RENKU_VERSION in its Dockerfile, uses Renku CLI version instead."""
    from renku.domain_model.project_context import project_context
    from renku.version import __version__

    url = "https://gitlab.dev.renku.ch/renku-testing/project-9.git"
    repo_path = tmpdir.mkdir("repo")
    Repository.clone_from(url=url, path=repo_path, env={"GIT_LFS_SKIP_SMUDGE": "1"})

    with project_context.with_path(path=repo_path), chdir(repo_path):
        assert 0 == runner.invoke(cli, ["migrate", "--strict"]).exit_code

        assert "RENKU_VERSION" not in project_context.docker_path.read_text()

        assert 0 == runner.invoke(cli, ["template", "set", "python-minimal"]).exit_code

        assert f"RENKU_VERSION={__version__}" in project_context.docker_path.read_text()


def test_template_set_dry_run(runner, project):
    """Test set dry-run doesn't make any changes."""
    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["template", "set", "-f", "R-minimal", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not project.repository.is_dirty()
    assert commit_sha_before == project.repository.head.commit.hexsha


@pytest.mark.integration
def test_template_update(runner, project, with_injection):
    """Test updating a template."""
    result = runner.invoke(
        cli,
        ["template", "set", "-f", "python-minimal", "-s", TEMPLATES_URL, "-r", "0.3.2"]
        + ["-p", "description=fixed-version"],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    with with_injection():
        assert "python-minimal" == project_context.project.template_id
        assert "0.3.2" == project_context.project.template_ref
        assert "b9ab266fba136bdecfa91dc8d7b6d36b9d427012" == project_context.project.template_version

    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" not in result.output
    with with_injection():
        assert "python-minimal" == project_context.project.template_id
        assert Version(project_context.project.template_ref) > Version("0.3.2")
        assert "6c59d8863841baeca8f30062fd16c650cf67da3b" != project_context.project.template_version

    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_template_update_latest_version(runner, project):
    """Test updating template that is the latest version."""
    result = runner.invoke(cli, ["template", "update"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Template is up-to-date" in result.output


@pytest.mark.integration
def test_template_update_missing_repo(runner, project_with_template):
    """Test update with a none-existing template repository fails with expected error."""
    result = runner.invoke(cli, ["template", "update"])

    assert 1 == result.exit_code
    assert "Template cannot be fetched" in result.output
    assert not project_with_template.repository.is_dirty()


@pytest.mark.integration
def test_template_update_dry_run(runner, project):
    """Test update dry-run doesn't make any changes."""
    result = runner.invoke(
        cli,
        ["template", "set", "-f", "python-minimal", "-s", TEMPLATES_URL, "-r", "0.3.2"]
        + ["-p", "description=fixed-version"],
    )

    assert 0 == result.exit_code, format_result_exception(result)

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["template", "update", "--dry-run"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert not project.repository.is_dirty()
    assert commit_sha_before == project.repository.head.commit.hexsha


def test_git_hook_for_modified_immutable_template_files(runner, project_with_template):
    """Test check for modified immutable template files."""
    (project_with_template.path / "immutable.file").write_text("Locally modified immutable files")

    with chdir(project_with_template.path):
        result = runner.invoke(cli, ["check-immutable-template-files", "Dockerfile"])
        assert result.exit_code == 0, result.output

        result = runner.invoke(cli, ["check-immutable-template-files", "immutable.file"])
        assert result.exit_code == 1
        assert "immutable.file" in result.output


def test_template_update_with_parameters(runner, project_with_template, templates_source, with_injection):
    """Test update prompts for new template parameters."""
    parameter = TemplateParameter(name="new-parameter", description="", type="", possible_values=[], default=None)
    templates_source.update(id="dummy", version="2.0.0", parameters=[parameter])

    result = runner.invoke(cli, ["template", "update"], input="new-value\n")

    assert result.exit_code == 0, result.output

    with with_injection():
        template_metadata = TemplateMetadata.from_project(project=project_context.project)
        assert "new-parameter" in template_metadata.metadata
        assert "new-value" == template_metadata.metadata["new-parameter"]


def test_template_update_with_parameters_with_defaults(runner, project_with_template, templates_source, with_injection):
    """Test update doesn't prompt for new template parameters with default value."""
    parameter = TemplateParameter(name="new-parameter", description="", type="", possible_values=[], default="def-val")
    templates_source.update(id="dummy", version="2.0.0", parameters=[parameter])

    result = runner.invoke(cli, ["template", "update"])

    assert result.exit_code == 0, result.output

    with with_injection():
        template_metadata = TemplateMetadata.from_project(project=project_context.project)
        assert "new-parameter" in template_metadata.metadata
        assert "def-val" == template_metadata.metadata["new-parameter"]


def test_template_set_with_parameters(runner, project_with_template, templates_source, with_injection):
    """Test template set doesn't prompts for new template parameters when passed on command line."""
    parameter = TemplateParameter(name="new-parameter", description="", type="", possible_values=[], default=None)
    templates_source.update(id="dummy", version="2.0.0", parameters=[parameter])

    result = runner.invoke(cli, ["template", "set", "-f", "-s", "dummy", "dummy", "-p", "new-parameter=param-value"])

    assert result.exit_code == 0, result.output

    with with_injection():
        template_metadata = TemplateMetadata.from_project(project=project_context.project)
        assert "new-parameter" in template_metadata.metadata
        assert "param-value" == template_metadata.metadata["new-parameter"]


def test_template_validate(runner, tmpdir_factory):
    """Test template validate command."""

    path = Path(tmpdir_factory.mktemp("valid"))
    valid_repo = Repository().initialize(path)

    readme_path = path / "README.md"
    readme_path.write_text("The readme")
    valid_repo.add(readme_path)
    valid_repo.commit("initial commit", author=Actor("me", "me@example.com"))

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate"])
        assert 1 == result.exit_code, format_result_exception(result)
        assert "There is no manifest file" in result.output

    manifest = path / "manifest.yaml"
    write_yaml(
        manifest,
        [
            {
                "id": "test",
                "name": "test",
                "description": "description",
                "variables": {"some_string": {"description": "some str desc", "type": "string"}},
            }
        ],
    )

    valid_repo.add(manifest)
    valid_repo.commit("add manifest", author=Actor("me", "me@example.com"))

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate"])
        assert 1 == result.exit_code, format_result_exception(result)
        assert "Template directory for 'test' does not exists" in result.output

    template_dir = path / "test"
    template_dir.mkdir()

    renku_dir = template_dir / ".renku"
    renku_dir.mkdir()
    renku_ini = template_dir / ".renku" / "renku.ini"
    renku_ini.write_text("a")

    valid_repo.add(renku_ini)
    valid_repo.commit("add renku.ini", author=Actor("me", "me@example.com"))

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate"])
        assert 1 == result.exit_code, format_result_exception(result)
        assert "These paths are required but missing" in result.output
        assert "Dockerfile" in result.output

    dockerfile = template_dir / "Dockerfile"
    dockerfile.write_text("a")

    valid_repo.add(renku_ini, dockerfile)
    valid_commit = valid_repo.commit("add dockerfile", author=Actor("me", "me@example.com"))

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "OK\n" == result.output

    metadata_folder = renku_dir / "metadata"
    metadata_folder.mkdir()

    project_file = metadata_folder / "project"
    project_file.write_text("test")

    valid_repo.add(renku_ini, project_file)
    valid_repo.commit("add prohibited file", author=Actor("me", "me@example.com"))

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate"])
        assert 1 == result.exit_code, format_result_exception(result)
        assert "These paths are not allowed in a template" in result.output
        assert ".renku/metadata" in result.output

    with chdir(path):
        result = runner.invoke(cli, ["template", "validate", "--reference", valid_commit.hexsha])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "OK\n" == result.output

        result = runner.invoke(cli, ["template", "validate", "--json", "--reference", valid_commit.hexsha])
        assert 0 == result.exit_code, format_result_exception(result)
        assert '"valid": true' in result.output


def test_template_validate_remote(runner):
    """Test template validate command on remote repository."""
    result = runner.invoke(
        cli, ["template", "validate", "--source", "https://github.com/SwissDataScienceCenter/renku-project-template"]
    )
    assert 0 == result.exit_code, format_result_exception(result)
