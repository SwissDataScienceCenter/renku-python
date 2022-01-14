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
"""Template management commands."""

import json
import shutil

from renku.core import errors
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication
from renku.core.utils.os import hash_file
from renku.core.utils.templates import (
    create_template_sentence,
    fetch_template,
    is_template_update_available,
    read_renku_version_from_dockerfile,
    render_template,
    select_template_from_manifest,
    set_template_variables,
    update_project_metadata,
    write_template_checksum,
)


def list_templates_command():
    """Command to list available templates."""
    return Command().command(_list_templates)


def _list_templates(template_source, template_ref, verbose):
    """List available templates."""
    template_manifest, _, _, _ = fetch_template(template_source, template_ref)
    template_data, _ = select_template_from_manifest(template_manifest, prompt=False)
    data = [template_data] if template_data else template_manifest

    communication.echo(create_template_sentence(data, describe=verbose))


def show_template_command():
    """Command to show template details."""
    return Command().command(_show_template)


def _show_template(template_source, template_ref, template_id, template_index):
    """Show template details."""
    template_manifest, _, _, _ = fetch_template(template_source, template_ref)
    template_data, _ = select_template_from_manifest(
        template_manifest, template_id=template_id, template_index=template_index, prompt=True
    )
    data = [template_data] if template_data else template_manifest

    communication.echo(create_template_sentence(data, describe=True))


def set_template_command():
    """Command to set template for a project."""
    return Command().command(_set_template).require_migration().require_clean().with_database(write=True).with_commit()


@inject.autoparams()
def _set_template(
    template_source,
    template_ref,
    template_id,
    template_index,
    parameters,
    force,
    interactive,
    client_dispatcher: IClientDispatcher,
):
    """Set template for a project."""
    client = client_dispatcher.current_client
    project = client.project

    if project.template_source and not force:
        raise errors.TemplateUpdateError("Project already has a template: To set a template use '-f/--force' flag")

    template_manifest, template_folder, template_source, template_version = fetch_template(
        template_source, template_ref
    )

    template_data, template_id = select_template_from_manifest(template_manifest, template_id, template_index)

    template_metadata = json.loads(project.template_metadata) if project.template_metadata else {}
    template_metadata.update(parameters)

    template_metadata["__template_source__"] = template_source
    template_metadata["__template_ref__"] = template_ref
    template_metadata["__template_id__"] = template_id
    template_metadata["__template_version__"] = template_version

    # NOTE: Always set __renku_version__ to the value read from the Dockerfile (if available) since setting/updating the
    # template doesn't change project's metadata version
    renku_version = template_metadata.get("__renku_version__")
    template_metadata["__renku_version__"] = read_renku_version_from_dockerfile(client.docker_path) or renku_version

    template_metadata = set_template_variables(
        template_data=template_data, template_metadata=template_metadata, interactive=interactive
    )

    template_base = template_folder / template_data["folder"]
    rendered_base = render_template(
        template_base=template_base, template_metadata=template_metadata, interactive=interactive
    )

    checksums = {}

    for file in rendered_base.rglob("*"):
        if file.is_dir():
            continue

        relative_path = file.relative_to(rendered_base)
        destination = client.path / relative_path

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(file, destination, follow_symlinks=False)
        except OSError as e:
            client.repository.clean()

            raise errors.TemplateUpdateError(f"Cannot copy {file} to {destination}") from e
        else:
            checksums[str(relative_path)] = hash_file(destination)

    update_project_metadata(
        project=project,
        template_metadata=template_metadata,
        template_version=template_version,
        immutable_template_files=template_data.get("immutable_template_files", []),
        automated_update=template_data.get("allow_template_update", True),
    )

    write_template_checksum(client, checksums)


def update_template_command():
    """Command to update project's template."""
    return (
        Command().command(_update_template).require_migration().require_clean().with_database(write=True).with_commit()
    )


@inject.autoparams()
def _update_template(force, interactive, client_dispatcher: IClientDispatcher):
    """Update project's template."""
    client = client_dispatcher.current_client
    project = client.project

    # TODO: Check for automated_update
    # TODO: Check for existence of the checksum file

    if not project.template_source:
        raise errors.TemplateUpdateError("Project doesn't have a template: Use 'renku template set'")

    template_manifest, template_folder, template_source, template_version = fetch_template(
        project.template_source, project.template_ref
    )

    if not is_template_update_available(template_source=template_source, template_version=template_version):
        communication.info("Template is up-to-date")
        return
    elif (
        template_source != "renku"
        and project.template_ref
        and template_version != project.template_version
        and not force
    ):
        raise errors.TemplateUpdateError(
            "Project has a fixed template version and cannot be updated: Pass '-f/--force' to update"
        )

    templates = [t for t in template_manifest if t["folder"] == project.template_id]
    if len(templates) == 1:
        template_data = templates[0]
    else:
        raise errors.TemplateUpdateError(f"The template with id '{project.template_id}' is not available.")

    template_metadata = json.loads(project.template_metadata) if project.template_metadata else {}

    template_metadata["__template_source__"] = template_source
    template_metadata["__template_ref__"] = project.template_ref
    template_metadata["__template_id__"] = project.template_id
    template_metadata["__template_version__"] = template_version

    # NOTE: Always set __renku_version__ to the value read from the Dockerfile (if available) since setting/updating the
    # template doesn't change project's metadata version
    renku_version = template_metadata.get("__renku_version__")
    template_metadata["__renku_version__"] = read_renku_version_from_dockerfile(client.docker_path) or renku_version

    template_metadata = set_template_variables(
        template_data=template_data, template_metadata=template_metadata, interactive=interactive
    )

    template_base = template_folder / template_data["folder"]
    rendered_base = render_template(
        template_base=template_base, template_metadata=template_metadata, interactive=interactive
    )

    checksums = {}

    for file in rendered_base.rglob("*"):
        if file.is_dir():
            continue

        relative_path = file.relative_to(rendered_base)
        destination = client.path / relative_path

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(file, destination, follow_symlinks=False)
        except OSError as e:
            client.repository.clean()

            raise errors.TemplateUpdateError(f"Cannot copy {file} to {destination}") from e
        else:
            checksums[str(relative_path)] = hash_file(destination)

    update_project_metadata(
        project=project,
        template_metadata=template_metadata,
        template_version=template_version,
        immutable_template_files=template_data.get("immutable_template_files", []),
        automated_update=template_data.get("allow_template_update", True),
    )

    write_template_checksum(client, checksums)
