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
"""Template utilities.

# TODO Fix all error messages to avoid command line parameters

# TODO Update this comment

# TODO: Should we create a TemplateVersion class to set reference and version

A project has three attributes to specify a template: ``template_source``, ``template_version``, and ``template_ref``.
In projects that use templates that are bundled with Renku, ``template_source`` is "renku" and ``template_version`` is
set to the installed Renku version. ``template_ref`` should not be set for such projects.

For projects that use a template from a Git repository, ``template_source`` is repository's URL and ``template_version``
is set to the current HEAD commit SHA. If a Git referenced was passed when setting the template, then project's
``template_ref`` is the same as the passed reference. In this case, Renku won't update a project's template if the
reference is a fixed value (i.e. a tag or a commit SHA).
"""

from collections import namedtuple
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import click
import jinja2

from renku.core import errors
from renku.core.commands.view_model.template import TemplateChangeViewModel, TemplateViewModel
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.template.template import (
    MetadataManager,
    RenderedTemplate,
    RenderType,
    TemplateRenderer,
    fetch_templates_source,
    is_renku_template,
)
from renku.core.models.tabulate import tabulate
from renku.core.models.template import SourceTemplate, TemplatesSource
from renku.core.utils import communication


def list_templates(source, reference) -> List[TemplateViewModel]:
    """Return available templates from a source."""
    templates_source = fetch_templates_source(source=source, reference=reference)

    return [TemplateViewModel.from_template(t) for t in templates_source.templates]


def show_template(source, reference, id) -> TemplateViewModel:
    """Show template details."""
    templates_source = fetch_templates_source(source=source, reference=reference)
    template = select_template(templates_source=templates_source, id=id)

    return TemplateViewModel.from_template(template)


def check_for_template_update(client) -> Tuple[bool, bool, Optional[str], Optional[str]]:
    """Check if the project can be updated to a newer version of the project template."""
    metadata = MetadataManager.from_project(client=client)

    templates_source = fetch_templates_source(source=metadata.source, reference=metadata.reference)
    latest_version = templates_source.get_latest_version(
        id=metadata.id, reference=metadata.reference, version=metadata.version
    )

    update_available = latest_version is not None and latest_version != metadata.version

    return update_available, metadata.allow_update, metadata.version, latest_version


def create_project_from_template(client, source_template: SourceTemplate, template_metadata):
    """Render template files from a template directory."""
    renderer = TemplateRenderer(
        source_template=source_template, metadata=template_metadata, render_type=RenderType.INITIALIZE
    )
    rendered_template = renderer.render(client=client, interactive=False)

    rendered_template.copy_files_to_project(client=client)


def get_template_files(template_base: Path, template_metadata) -> Generator[str, None, None]:
    """Return relative paths Gets paths in a rendered renku template."""
    for file in template_base.rglob("*"):
        relative_path = str(file.relative_to(template_base))
        # NOTE: The path could contain template variables, we need to template it
        relative_path = jinja2.Template(relative_path).render(template_metadata)

        yield relative_path


@inject.autoparams("client_dispatcher")
def set_template(
    source, reference, id, force, interactive, input_parameters, dry_run, client_dispatcher: IClientDispatcher
) -> TemplateChangeViewModel:
    """Set template for a project."""
    client = client_dispatcher.current_client
    project = client.project

    if project.template_source and not force:
        raise errors.TemplateUpdateError("Project already has a template: To set a template use '-f/--force' flag")
    if is_renku_template(source) and reference is not None:
        raise errors.ParameterError("Templates included in renku don't support specifying a reference")
    if not client.has_template_checksum() and not interactive:
        raise errors.TemplateUpdateError("Required template metadata doesn't exist: Use '-i/--interactive' flag")

    templates_source = fetch_templates_source(source=source, reference=reference)

    source_template = select_template(templates_source, id=id)

    rendered_template = _set_or_update_project_from_template(
        templates_source=templates_source,
        reference=source_template.reference,
        id=source_template.id,
        interactive=interactive,
        dry_run=dry_run,
        render_type=RenderType.SET,
        input_parameters=input_parameters,
    )

    return TemplateChangeViewModel.from_template(template=rendered_template)


@inject.autoparams("client_dispatcher")
def update_template(
    force, interactive, dry_run, client_dispatcher: IClientDispatcher
) -> Optional[TemplateChangeViewModel]:
    """Update project's template if possible. Return True if updated."""
    client = client_dispatcher.current_client

    template_metadata = MetadataManager.from_project(client=client)

    if not template_metadata.source:
        raise errors.TemplateUpdateError("Project doesn't have a template: Use 'renku template set'")
    if not client.has_template_checksum() and not interactive:
        raise errors.TemplateUpdateError("Required template metadata doesn't exist: Use '-i/--interactive' flag")

    if not template_metadata.allow_update and not force:
        raise errors.TemplateUpdateError(
            "Update is not allowed for this template. You can still update it using '-f/--force' flag but it might "
            "break your project"
        )

    templates_source = fetch_templates_source(source=template_metadata.source, reference=template_metadata.reference)

    update_available, latest_version = templates_source.is_update_available(
        id=template_metadata.id, reference=template_metadata.reference, version=template_metadata.version
    )

    if not update_available:
        return

    rendered_template = _set_or_update_project_from_template(
        templates_source=templates_source,
        reference=latest_version,
        id=template_metadata.id,
        interactive=interactive,
        dry_run=dry_run,
        render_type=RenderType.UPDATE,
        input_parameters=None,
    )

    return TemplateChangeViewModel.from_template(template=rendered_template)


@inject.autoparams("client_dispatcher", "project_gateway")
def _set_or_update_project_from_template(
    templates_source: TemplatesSource,
    reference: str,
    id: str,
    interactive,
    dry_run: bool,
    render_type: RenderType,
    client_dispatcher: IClientDispatcher,
    project_gateway: IProjectGateway,
    input_parameters=None,
) -> RenderedTemplate:
    """Update project files and metadata from a template."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use interactive mode with no prompt")

    input_parameters = input_parameters or {}

    client = client_dispatcher.current_client
    project = project_gateway.get_project()

    source_template = templates_source.get_template(id=id, reference=reference)

    metadata_manager = MetadataManager.from_project(client=client)
    metadata_manager.update_from_template(template=source_template)

    if not dry_run:
        metadata_manager.set_template_variables(
            template=source_template, input_parameters=input_parameters, interactive=interactive
        )

    renderer = TemplateRenderer(
        source_template=source_template, metadata=metadata_manager.metadata, render_type=render_type
    )
    rendered_template = renderer.render(client=client, interactive=interactive and not dry_run)

    if not dry_run:
        rendered_template.copy_files_to_project(client=client)
        metadata_manager.update_project(project=project)
        project_gateway.update_project(project)

    return rendered_template


def select_template(templates_source: TemplatesSource, id=None) -> SourceTemplate:
    """Select a template from a template source."""

    def prompt_to_select_template():
        if not communication.has_prompt():
            raise errors.InvalidTemplateError("Cannot select a template")

        Selection = namedtuple("Selection", ["index", "id"])

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
