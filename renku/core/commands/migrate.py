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
"""Migrate project to the latest Renku version."""

from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.migrate import (
    is_docker_update_possible,
    is_migration_required,
    is_project_unsupported,
    is_renku_project,
    is_template_update_possible,
    migrate,
)

SUPPORTED_RENKU_PROJECT = 1
MIGRATION_REQUIRED = 2
UNSUPPORTED_PROJECT = 4
NON_RENKU_REPOSITORY = 8
TEMPLATE_UPDATE_POSSIBLE = 16
AUTOMATED_TEMPLATE_UPDATE_SUPPORTED = 32
DOCKERFILE_UPDATE_POSSIBLE = 64


def migrations_check():
    """Return a command for a migrations check."""
    return Command().command(_migrations_check).with_database(write=False)


@inject.autoparams()
def _migrations_check(client_dispatcher: IClientDispatcher):
    client = client_dispatcher.current_client

    template_update_possible, current_version, new_version = is_template_update_possible()

    try:
        template_source = client.project.template_source
        template_ref = client.project.template_ref
        template_id = client.project.template_id
        automated_update = bool(client.project.automated_update)
    except ValueError:
        template_source = None
        template_ref = None
        template_id = None
        automated_update = False

    return (
        is_migration_required(),
        not is_project_unsupported(),
        template_update_possible,
        current_version,
        new_version,
        template_source,
        template_ref,
        template_id,
        automated_update,
        is_docker_update_possible(),
    )


def migrations_versions():
    """Return a command to get source and destination migration versions."""
    return Command().command(_migrations_versions).lock_project().with_database()


@inject.autoparams()
def _migrations_versions(client_dispatcher: IClientDispatcher):
    """Return source and destination migration versions."""
    from renku import __version__

    client = client_dispatcher.current_client

    try:
        latest_agent = client.latest_agent
    except ValueError:
        # NOTE: maybe old project
        from renku.core.management.migrations.utils import read_latest_agent

        latest_agent = read_latest_agent(client)

    return __version__, latest_agent


def migrate_project():
    """Return a command to migrate all project's entities."""
    return Command().command(_migrate_project).lock_project().require_clean().with_database(write=True)


def _migrate_project(
    force_template_update=False,
    skip_template_update=False,
    skip_docker_update=False,
    skip_migrations=False,
    strict=False,
):
    """Migrate all project's entities."""
    return migrate(
        force_template_update=force_template_update,
        skip_template_update=skip_template_update,
        skip_docker_update=skip_docker_update,
        skip_migrations=skip_migrations,
        strict=strict,
    )


def check_project():
    """Return a command to check if repository is a renku project, unsupported, or requires migration."""
    return Command().command(_check_project).with_database(write=False)


@inject.autoparams()
def _check_project(client_dispatcher: IClientDispatcher):
    client = client_dispatcher.current_client

    if not is_renku_project():
        return NON_RENKU_REPOSITORY
    elif is_project_unsupported():
        return UNSUPPORTED_PROJECT

    try:
        client.project
    except ValueError:
        return MIGRATION_REQUIRED

    status = 0

    if is_template_update_possible():
        status |= TEMPLATE_UPDATE_POSSIBLE
    if client.project.automated_update:
        status |= AUTOMATED_TEMPLATE_UPDATE_SUPPORTED
    if is_docker_update_possible():
        status |= DOCKERFILE_UPDATE_POSSIBLE

    if is_migration_required():
        return status | MIGRATION_REQUIRED

    return status | SUPPORTED_RENKU_PROJECT


@inject.autoparams()
def _check_immutable_template_files(paths, client_dispatcher: IClientDispatcher):
    """Check paths and return a list of those that are marked immutable in the project template."""
    client = client_dispatcher.current_client

    if not client.project.immutable_template_files:
        return []

    immutable_template_files = client.project.immutable_template_files or []
    return [p for p in paths if str(p) in immutable_template_files]


def check_immutable_template_files_command():
    """Command for checking immutable template files."""
    return Command().command(_check_immutable_template_files).with_database()
