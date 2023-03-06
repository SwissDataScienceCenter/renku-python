# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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
"""Template tests."""

from pathlib import Path

import pytest

from renku.core import errors
from renku.core.template.template import (
    FileAction,
    TemplateAction,
    copy_template_to_project,
    fetch_templates_source,
    get_file_actions,
)
from renku.core.template.usecase import (
    check_for_template_update,
    does_dockerfile_contains_only_version_change,
    is_dockerfile_updated_by_user,
    update_template,
)
from renku.core.util.metadata import replace_renku_version_in_dockerfile
from renku.domain_model.project_context import project_context
from renku.domain_model.template import TEMPLATE_MANIFEST
from tests.utils import write_and_commit_file

TEMPLATES_URL = "https://github.com/SwissDataScienceCenter/renku-project-template"


@pytest.mark.integration
@pytest.mark.parametrize("reference", [None, "master", "0.3.0"])
def test_template_fetch_from_git(reference):
    """Test fetching a template from git."""
    templates_source = fetch_templates_source(source=TEMPLATES_URL, reference=reference)

    assert reference == templates_source.reference
    assert (templates_source.path / TEMPLATE_MANIFEST).exists()

    for template in templates_source.templates:
        assert template.path.exists()


def test_template_fetch_invalid_git_url():
    """Test fetching a template from an invalid git url."""
    with pytest.raises(errors.InvalidTemplateError):
        fetch_templates_source(source="invalid-url", reference=None)


@pytest.mark.integration
@pytest.mark.vcr
def test_template_fetch_invalid_git_reference():
    """Test fetching a template from an invalid reference."""
    with pytest.raises(errors.TemplateMissingReferenceError):
        fetch_templates_source(source=TEMPLATES_URL, reference="invalid-ref")


def test_check_for_template_update(project_with_template, templates_source, with_injection):
    """Test checking for a template update."""
    templates_source.update(id="dummy", version="2.0.0")

    with with_injection():
        project = project_context.project
        updates_available, _, current_version, new_version = check_for_template_update(project)

    assert updates_available is True
    assert "1.0.0" == current_version
    assert "2.0.0" == new_version


def test_template_update_files(project_with_template, templates_source, with_injection):
    """Test template update."""
    templates_source.update(id="dummy", version="2.0.0")

    with with_injection():
        files_before = {p: Path(p).read_text() for p in project_context.project.template_files}

        update_template(force=False, interactive=False, dry_run=False)

        for file in project_context.project.template_files:
            assert Path(file).read_text() != files_before[file]


def test_template_update_source_failure(project_with_template, with_injection):
    """Test template update with broken template source."""
    with with_injection():
        with pytest.raises(errors.TemplateUpdateError):
            update_template(force=False, interactive=False, dry_run=False)


@pytest.mark.parametrize(
    "action, content_type",
    [
        (FileAction.APPEND, "append"),
        (FileAction.CREATE, "template"),
        (FileAction.OVERWRITE, "template"),
        (FileAction.RECREATE, "template"),
        (FileAction.DELETED, "project"),
        (FileAction.IGNORE_IDENTICAL, "project"),
        (FileAction.IGNORE_UNCHANGED_REMOTE, "project"),
        (FileAction.KEEP, "project"),
    ],
)
def test_copy_template_actions(project, rendered_template, action, content_type, with_injection):
    """Test FileActions when copying a template."""
    project_content = (project_context.path / "Dockerfile").read_text()
    template_content = (rendered_template.path / "Dockerfile").read_text()

    # NOTE: Ignore all other files expect the Dockerfile
    actions = {f: FileAction.IGNORE_UNCHANGED_REMOTE for f in rendered_template.get_files()}
    actions["Dockerfile"] = action
    with with_injection():
        copy_template_to_project(rendered_template=rendered_template, project=project_context.project, actions=actions)

    # NOTE: Make sure that files have some content
    assert project_content
    assert template_content
    assert project_content != template_content

    if content_type == "append":
        expected_content = f"{project_content}\n{template_content}"
    elif content_type == "template":
        expected_content = template_content
    else:
        expected_content = project_content

    assert expected_content == (project_context.path / "Dockerfile").read_text()


def test_get_file_actions_for_initialize(project, rendered_template, with_injection):
    """Test getting file action when initializing."""
    with with_injection():
        actions = get_file_actions(
            rendered_template=rendered_template, template_action=TemplateAction.INITIALIZE, interactive=False
        )

    appended_file = ".gitignore"
    assert FileAction.APPEND == actions[appended_file]
    new_file = ".dummy"
    assert FileAction.CREATE == actions[new_file]
    existing_file = "Dockerfile"
    assert FileAction.OVERWRITE == actions[existing_file]
    kept_file = "README.md"
    assert FileAction.KEEP == actions[kept_file]


def test_get_file_actions_for_set(project, rendered_template, with_injection):
    """Test getting file action when setting a template."""
    with with_injection():
        actions = get_file_actions(
            rendered_template=rendered_template, template_action=TemplateAction.SET, interactive=False
        )

    new_file = ".dummy"
    assert FileAction.CREATE == actions[new_file]
    existing_file = "Dockerfile"
    assert FileAction.OVERWRITE == actions[existing_file]
    kept_file = "README.md"
    assert FileAction.KEEP == actions[kept_file]


def test_get_file_actions_for_update(project_with_template, rendered_template_with_update, with_injection):
    """Test getting file action when updating a template."""
    with with_injection():
        actions = get_file_actions(
            rendered_template=rendered_template_with_update, template_action=TemplateAction.UPDATE, interactive=False
        )

    identical_file = ".dummy"
    assert FileAction.IGNORE_IDENTICAL == actions[identical_file]
    remotely_modified = "Dockerfile"
    assert FileAction.OVERWRITE == actions[remotely_modified]


def test_update_with_locally_modified_file(project_with_template, rendered_template_with_update, with_injection):
    """Test a locally modified file that is remotely updated won't change."""
    (project_context.path / "Dockerfile").write_text("Local modification")

    with with_injection():
        actions = get_file_actions(
            rendered_template=rendered_template_with_update, template_action=TemplateAction.UPDATE, interactive=False
        )

    assert FileAction.KEEP == actions["Dockerfile"]


def test_update_with_locally_deleted_file(project_with_template, rendered_template_with_update, with_injection):
    """Test a locally deleted file that is remotely updated won't be re-created."""
    (project_context.path / "requirements.txt").unlink()

    with with_injection():
        actions = get_file_actions(
            rendered_template=rendered_template_with_update, template_action=TemplateAction.UPDATE, interactive=False
        )

    assert FileAction.DELETED == actions["requirements.txt"]


@pytest.mark.parametrize("delete", [False, True])
def test_update_with_locally_changed_immutable_file(
    project_with_template, rendered_template_with_update, with_injection, delete
):
    """Test a locally deleted file that is remotely updated won't be re-created."""
    if delete:
        (project_context.path / "immutable.file").unlink()
    else:
        (project_context.path / "immutable.file").write_text("Locally modified immutable files")

    with pytest.raises(
        errors.TemplateUpdateError, match="Can't update template as immutable template file .* has local changes."
    ), with_injection():
        get_file_actions(
            rendered_template=rendered_template_with_update, template_action=TemplateAction.UPDATE, interactive=False
        )


def test_detect_dockerfile_version_update(project, with_injection):
    """Test detecting a Dockerfile was only updated for changing Renku version."""
    dockerfile = project_context.path / "Dockerfile"
    new_content = replace_renku_version_in_dockerfile(dockerfile.read_text(), version="1.42.42")
    write_and_commit_file(project.repository, dockerfile, new_content)

    new_content = replace_renku_version_in_dockerfile(dockerfile.read_text(), version="2.42.42")
    write_and_commit_file(project.repository, dockerfile, new_content)

    with with_injection():
        assert does_dockerfile_contains_only_version_change()
        assert not is_dockerfile_updated_by_user()

    new_content = dockerfile.read_text() + "\nRUN echo 'user modifications'"
    write_and_commit_file(project.repository, dockerfile, new_content)

    with with_injection():
        assert not does_dockerfile_contains_only_version_change()
        assert is_dockerfile_updated_by_user()
