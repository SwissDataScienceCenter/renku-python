# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

from renku.core.incubation.command import Command
from renku.core.management.migrate import (
    is_docker_update_possible,
    is_migration_required,
    is_project_unsupported,
    is_renku_project,
    is_template_update_possible,
    migrate,
)

SUPPORTED_RENKU_PROJECT = 0
MIGRATION_REQUIRED = 1
UNSUPPORTED_PROJECT = 2
NON_RENKU_REPOSITORY = 3


def migrations_check():
    """Return a command for a migrations check."""
    return Command().command(_migrations_check).lock_project()


def _migrations_check(client):
    template_update_possible, current_version, new_version = is_template_update_possible(client)
    return (
        is_migration_required(client),
        not is_project_unsupported(client),
        template_update_possible,
        current_version,
        new_version,
        bool(client.project.automated_update),
        is_docker_update_possible(client),
    )


def migrations_versions():
    """Return a command to get source and destination migration versions."""
    return Command().command(_migrations_versions).lock_project()


def _migrations_versions(client):
    """Return source and destination migration versions."""
    from renku import __version__

    return __version__, client.latest_agent


def migrate_project():
    """Return a command to migrate all project's entities."""
    return Command().command(_migrate_project).lock_project().require_clean()


def _migrate_project(
    client, force_template_update=False, skip_template_update=False, skip_docker_update=False, skip_migrations=False,
):
    """Migrate all project's entities."""
    return migrate(
        client=client,
        force_template_update=force_template_update,
        skip_template_update=skip_template_update,
        skip_docker_update=skip_docker_update,
        skip_migrations=skip_migrations,
    )


def check_project():
    """Return a command to check if repository is a renku project, unsupported, or requires migration."""
    return Command().command(_check_project).lock_project()


def _check_project(client):
    if not is_renku_project(client):
        return NON_RENKU_REPOSITORY
    elif is_migration_required(client):
        return MIGRATION_REQUIRED
    elif is_project_unsupported(client):
        return UNSUPPORTED_PROJECT

    return SUPPORTED_RENKU_PROJECT
