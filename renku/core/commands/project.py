# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Project management."""

from renku.core.commands.view_model.project import ProjectViewModel
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.repository import DATABASE_METADATA_PATH
from renku.core.utils.metadata import construct_creator


@inject.autoparams()
def _edit_project(description, creator, keywords, custom_metadata, project_gateway: IProjectGateway):
    """Edit dataset metadata."""
    possible_updates = {
        "creator": creator,
        "description": description,
        "keywords": keywords,
        "custom_metadata": custom_metadata,
    }

    creator, no_email_warnings = construct_creator(creator, ignore_email=True)

    updated = {k: v for k, v in possible_updates.items() if v}

    if updated:
        project = project_gateway.get_project()
        project.update_metadata(
            creator=creator, description=description, keywords=keywords, custom_metadata=custom_metadata
        )
        project_gateway.update_project(project)

    return updated, no_email_warnings


def edit_project_command():
    """Command for editing project metadata."""
    command = Command().command(_edit_project).lock_project().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATABASE_METADATA_PATH)


@inject.autoparams()
def _show_project(client_dispatcher: IClientDispatcher):
    """Show project metadata."""
    return ProjectViewModel.from_project(client_dispatcher.current_client.project)


def show_project_command():
    """Command for showing project metadata."""
    return Command().command(_show_project).lock_project().with_database().require_migration()
