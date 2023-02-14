# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

from typing import List

from pydantic import validate_arguments

from renku.command.command_builder.command import Command
from renku.domain_model.project_context import project_context

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


def _migrations_check():
    """Check migration status of project.

    Returns:
        Dictionary of project migrations, template and dockerfile status.
    """
    from renku.core.migration.migrate import is_project_unsupported

    core_version, latest_version = _migrations_versions()

    return {
        "project_supported": not is_project_unsupported(),
        "core_renku_version": core_version,
        "project_renku_version": latest_version,
        "core_compatibility_status": _metadata_migration_check(),
        "dockerfile_renku_status": _dockerfile_migration_check(),
        "template_status": _template_migration_check(),
    }


def migrations_versions():
    """Return a command to get source and destination migration versions."""
    return Command().command(_migrations_versions).lock_project().with_database()


def _migrations_versions():
    """Return source and destination migration versions.

    Returns:
        Tuple of current version and project version.
    """
    from renku import __version__

    try:
        latest_agent = project_context.latest_agent
    except ValueError:
        # NOTE: maybe old project
        from renku.core.migration.utils import read_latest_agent

        latest_agent = read_latest_agent()

    return __version__, latest_agent


def _template_migration_check():
    """Return template migration status.

    Returns:
        Dictionary of template migration status.
    """
    from renku.core.template.usecase import check_for_template_update

    try:
        project = project_context.project
        template_source = project.template_metadata.template_source
        template_ref = project.template_metadata.template_ref
        template_id = project.template_metadata.template_id
    except (ValueError, AttributeError):
        project = None
        template_source = None
        template_ref = None
        template_id = None

    update_available, update_allowed, current_version, new_version = check_for_template_update(project)

    return {
        "automated_template_update": update_allowed,
        "newer_template_available": update_available,
        "project_template_version": current_version,
        "latest_template_version": new_version,
        "template_source": template_source,
        "template_ref": template_ref,
        "template_id": template_id,
    }


def dockerfile_migration_check():
    """Return a command for a Dockerfile migrations check."""
    return Command().command(_dockerfile_migration_check)


def _dockerfile_migration_check():
    """Return Dockerfile migration status.

    Returns:
        Dictionary of Dockerfile migration status.
    """
    from renku import __version__
    from renku.core.migration.migrate import is_docker_update_possible

    automated_dockerfile_update, newer_renku_available, dockerfile_renku_version = is_docker_update_possible()

    return {
        "automated_dockerfile_update": automated_dockerfile_update,
        "newer_renku_available": newer_renku_available,
        "dockerfile_renku_version": dockerfile_renku_version,
        "latest_renku_version": __version__,
    }


def metadata_migration_check():
    """Return a command for a metadata migrations check."""
    return Command().command(_metadata_migration_check)


def _metadata_migration_check():
    """Return metadata migration status.

    Returns:
        Dictionary of metadata migration status.
    """
    from renku.core.migration.migrate import SUPPORTED_PROJECT_VERSION, get_project_version, is_migration_required

    return {
        "migration_required": is_migration_required(),
        "project_metadata_version": get_project_version(),
        "current_metadata_version": SUPPORTED_PROJECT_VERSION,
    }


def migrate_project_command():
    """Return a command to migrate all project's entities."""
    from renku.core.migration.migrate import migrate_project

    return Command().command(migrate_project).lock_project().require_clean().with_database(write=True)


def check_project():
    """Return a command to check if repository is a renku project, unsupported, or requires migration."""
    return Command().command(_check_project).with_database(write=False)


def _check_project():
    from renku.core.migration.migrate import (
        is_docker_update_possible,
        is_migration_required,
        is_project_unsupported,
        is_renku_project,
    )
    from renku.core.template.usecase import check_for_template_update

    if not is_renku_project():
        return NON_RENKU_REPOSITORY
    elif is_project_unsupported():
        return UNSUPPORTED_PROJECT

    try:
        _ = project_context.project
    except ValueError:
        return MIGRATION_REQUIRED
    else:
        if hasattr(project_context.project, "template_source"):
            # NOTE: v10 migration not done
            return MIGRATION_REQUIRED

    # NOTE: ``project.automated_update`` is deprecated and we always allow template update for a project
    status = AUTOMATED_TEMPLATE_UPDATE_SUPPORTED

    if check_for_template_update(project_context.project)[0]:
        status |= TEMPLATE_UPDATE_POSSIBLE
    if is_docker_update_possible()[0]:
        status |= DOCKERFILE_UPDATE_POSSIBLE

    if is_migration_required():
        return status | MIGRATION_REQUIRED

    return status | SUPPORTED_RENKU_PROJECT


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _check_immutable_template_files(paths: List[str]):
    """Check paths and return a list of those that are marked immutable in the project template.

    Args:
        paths(List[str]): Paths to check.

    Returns:
        List of immutable template files.
    """
    immutable_template_files = project_context.project.template_metadata.immutable_template_files or []

    return [p for p in paths if str(p) in immutable_template_files]


def check_immutable_template_files_command():
    """Command for checking immutable template files."""
    return Command().command(_check_immutable_template_files).with_database()
