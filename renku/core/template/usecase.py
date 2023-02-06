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
"""Template use cases."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import click
from pydantic import validate_arguments

from renku.command.command_builder.command import inject
from renku.command.view_model.template import TemplateChangeViewModel, TemplateViewModel
from renku.core import errors
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.migration.migrate import is_renku_project
from renku.core.template.template import (
    FileAction,
    RepositoryTemplates,
    TemplateAction,
    copy_template_to_project,
    fetch_templates_source,
    get_file_actions,
    has_template_checksum,
    set_template_parameters,
)
from renku.core.util import communication
from renku.core.util.tabulate import tabulate
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context
from renku.domain_model.template import RenderedTemplate, Template, TemplateMetadata, TemplatesSource
from renku.infrastructure.repository import Repository


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def list_templates(source: Optional[str], reference: Optional[str]) -> List[TemplateViewModel]:
    """Return available templates from a source."""
    templates_source = fetch_templates_source(source=source, reference=reference)

    return [TemplateViewModel.from_template(t) for t in templates_source.templates]


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def show_template(source: Optional[str], reference: Optional[str], id: Optional[str]) -> TemplateViewModel:
    """Show template details."""
    if source or id:
        templates_source = fetch_templates_source(source=source, reference=reference)
        template = templates_source.get_template(id=id, reference=None)
    elif is_renku_project():
        metadata = TemplateMetadata.from_project(project=project_context.project)

        templates_source = fetch_templates_source(source=metadata.source, reference=metadata.reference)
        id = metadata.id
        template = templates_source.get_template(id=id, reference=None)
    else:
        raise errors.ParameterError("No Renku project found")

    if template is None:
        raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available")

    return TemplateViewModel.from_template(template)


def check_for_template_update(project: Optional[Project]) -> Tuple[bool, bool, Optional[str], Optional[str]]:
    """Check if the project can be updated to a newer version of the project template."""
    metadata = TemplateMetadata.from_project(project=project)

    templates_source = fetch_templates_source(source=metadata.source, reference=metadata.reference)
    update_available, latest_reference = templates_source.is_update_available(
        id=metadata.id, reference=metadata.reference, version=metadata.version
    )

    return update_available, metadata.allow_update, metadata.reference, latest_reference


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def set_template(
    source: Optional[str],
    reference: Optional[str],
    id: Optional[str],
    force: bool,
    interactive: bool,
    input_parameters: Optional[Dict[str, str]],
    dry_run: bool,
) -> TemplateChangeViewModel:
    """Set template for a project."""
    project = project_context.project

    if project.template_metadata.template_source and not force:
        raise errors.TemplateUpdateError("Project already has a template: To set a template use '-f/--force' flag")

    templates_source = fetch_templates_source(source=source, reference=reference)

    template = select_template(templates_source, id=id)

    if template is None:
        raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available")

    rendered_template, actions = _set_or_update_project_from_template(
        templates_source=templates_source,
        reference=template.reference,
        id=template.id,
        interactive=interactive,
        dry_run=dry_run,
        template_action=TemplateAction.SET,
        input_parameters=input_parameters,
    )

    return TemplateChangeViewModel.from_template(template=rendered_template, actions=actions)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def update_template(force: bool, interactive: bool, dry_run: bool) -> Optional[TemplateChangeViewModel]:
    """Update project's template if possible. Return True if updated."""
    template_metadata = TemplateMetadata.from_project(project=project_context.project)

    if not template_metadata.source:
        raise errors.TemplateUpdateError("Project doesn't have a template: Use 'renku template set'")
    if not has_template_checksum() and not interactive:
        raise errors.TemplateUpdateError("Required template metadata doesn't exist: Use '-i/--interactive' flag")

    if not template_metadata.allow_update and not force:
        raise errors.TemplateUpdateError(
            "Update is not allowed for this template. You can still update it using '-f/--force' flag but it might "
            "break your project"
        )

    try:
        templates_source = fetch_templates_source(
            source=template_metadata.source, reference=template_metadata.reference
        )
    except errors.TemplateMissingReferenceError as e:
        message = f"{str(e)}; You can still manually update the template and set a difference reference."
        raise errors.TemplateUpdateError(message)
    except errors.InvalidTemplateError:
        raise errors.TemplateUpdateError("Template cannot be fetched.")

    update_available, latest_reference = templates_source.is_update_available(
        id=template_metadata.id, reference=template_metadata.reference, version=template_metadata.version
    )

    if not update_available:
        return None

    rendered_template, actions = _set_or_update_project_from_template(
        templates_source=templates_source,
        reference=latest_reference,
        id=template_metadata.id,
        interactive=interactive,
        dry_run=dry_run,
        template_action=TemplateAction.UPDATE,
        input_parameters=None,
    )

    return TemplateChangeViewModel.from_template(template=rendered_template, actions=actions)


@inject.autoparams("project_gateway")
def _set_or_update_project_from_template(
    templates_source: TemplatesSource,
    reference: str,
    id: str,
    interactive,
    dry_run: bool,
    template_action: TemplateAction,
    input_parameters,
    project_gateway: IProjectGateway,
) -> Tuple[RenderedTemplate, Dict[str, FileAction]]:
    """Update project files and metadata from a template."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use interactive mode with no prompt")

    input_parameters = input_parameters or {}

    project = project_gateway.get_project()

    template = templates_source.get_template(id=id, reference=reference)

    if template is None:
        raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available")

    template_metadata = TemplateMetadata.from_project(project=project_context.project)
    template_metadata.update(template=template)

    if not dry_run:
        set_template_parameters(
            template=template,
            template_metadata=template_metadata,
            input_parameters=input_parameters,
            interactive=interactive,
        )

    rendered_template = template.render(metadata=template_metadata)
    actions = get_file_actions(
        rendered_template=rendered_template, template_action=template_action, interactive=interactive and not dry_run
    )

    if not dry_run:
        copy_template_to_project(rendered_template=rendered_template, project=project, actions=actions)
        project_gateway.update_project(project)

    return rendered_template, actions


def select_template(templates_source: TemplatesSource, id: Optional[str] = None) -> Optional[Template]:
    """Select a template from a template source."""

    def prompt_to_select_template():
        if not communication.has_prompt():
            raise errors.InvalidTemplateError("Cannot select a template")

        Selection = NamedTuple("Selection", [("index", int), ("id", str)])

        templates = [Selection(index=i, id=t.id) for i, t in enumerate(templates_source.templates, start=1)]
        tables = tabulate(templates, headers=["index", "id"])

        message = f"{tables}\nPlease choose a template by typing its index"

        template_index = communication.prompt(
            msg=message, type=click.IntRange(1, len(templates_source.templates)), show_default=False, show_choices=False
        )
        return templates_source.templates[template_index - 1]

    if id:
        try:
            return templates_source.get_template(id=id, reference=None)
        except errors.TemplateNotFoundError:
            raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available")
    elif len(templates_source.templates) == 1:
        return templates_source.templates[0]

    return prompt_to_select_template()


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def validate_templates(
    source: Optional[str] = None, reference: Optional[str] = None
) -> Dict[str, Union[str, Dict[str, List[str]]]]:
    """Validate a template repository.

    Args:
        source(str, optional): Remote repository URL to clone and check (Default value = None).
        reference(str, optional): Git commit/branch/tag to check (Default value = None).
    Returns:
        Dict[str, Union[str, Dict[str, List[str]]]]: Dictionary containing errors and warnings for manifest and
            templates, along with a ``valid`` field telling if all checks passed.
    """

    if source is not None:
        path = Path(tempfile.mkdtemp())
        repo = Repository.clone_from(path=path, url=source)
        repo.checkout(reference=reference)
    else:
        path = Path(os.getcwd())
        repo = Repository(path=path)

        if reference is not None:
            path = Path(tempfile.mkdtemp())
            repo.create_worktree(path, reference=reference)
            repo = Repository(path=path)

    version = repo.head.commit.hexsha

    result: Dict[str, Any] = {"manifest": None, "templates": {}, "warnings": [], "valid": True}

    try:
        template_source = RepositoryTemplates(
            path=path, source=path, reference="", version=version, repository=repo, skip_validation=True
        )
        result["warnings"] = template_source.manifest.validate(manifest_only=True)
    except errors.InvalidTemplateError as e:
        result["manifest"] = e.args[0] if e.args else str(e)
        result["valid"] = False
        return result

    for template in template_source.manifest.templates:
        template.templates_source = template_source
        issues = template.validate(skip_files=False, raise_errors=False)
        if issues:
            result["templates"][template.id] = issues
            result["valid"] = False

    return result
