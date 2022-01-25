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

from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication
from renku.core.utils.templates import (
    create_template_sentence,
    fetch_template,
    select_template_from_manifest,
    set_template,
    update_template,
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


def _show_template(template_source, template_ref, template_id):
    """Show template details."""
    template_manifest, _, _, _ = fetch_template(template_source, template_ref)
    template_data, _ = select_template_from_manifest(template_manifest, template_id=template_id, prompt=True)
    data = [template_data] if template_data else template_manifest

    communication.echo(create_template_sentence(data, describe=True))


def set_template_command():
    """Command to set template for a project."""
    return (
        Command()
        .command(_set_template)
        .lock_project()
        .require_migration()
        .require_clean()
        .with_database(write=True)
        .with_commit()
    )


@inject.autoparams()
def _set_template(
    template_source,
    template_ref,
    template_id,
    parameters,
    force,
    interactive,
    client_dispatcher: IClientDispatcher,
):
    """Set template for a project."""
    client = client_dispatcher.current_client

    set_template(
        client=client,
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        force=force,
        interactive=interactive,
        parameters=parameters,
    )


def update_template_command():
    """Command to update project's template."""
    return (
        Command()
        .command(_update_template)
        .lock_project()
        .require_migration()
        .require_clean()
        .with_database(write=True)
        .with_commit()
    )


@inject.autoparams()
def _update_template(force, interactive, client_dispatcher: IClientDispatcher) -> bool:
    """Update project's template; return False if no update is possible."""
    client = client_dispatcher.current_client

    return update_template(client=client, force=force, interactive=interactive)
