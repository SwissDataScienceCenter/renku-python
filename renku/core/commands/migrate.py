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

from renku.core.management.migrate import is_docker_update_possible, is_migration_required, is_project_unsupported
from renku.core.management.migrate import is_renku_project as is_renku_project_helper
from renku.core.management.migrate import is_template_update_possible, migrate

from .client import pass_local_client


@pass_local_client
def migrations_check(client):
    """Public function for a migrations check."""

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


@pass_local_client
def migrations_versions(client):
    """Return source and destination migration versions."""
    from renku import __version__

    return __version__, client.latest_agent


@pass_local_client(clean=True, commit=True, commit_empty=False)
def migrate_project(
    client,
    force_template_update=False,
    skip_template_update=False,
    skip_docker_update=False,
    skip_migrations=False,
    progress_callback=None,
    commit_message=None,
):
    """Migrate all project's entities."""
    return migrate(
        client=client,
        force_template_update=force_template_update,
        skip_template_update=skip_template_update,
        skip_docker_update=skip_docker_update,
        skip_migrations=skip_migrations,
        progress_callback=progress_callback,
    )


@pass_local_client(clean=True, commit=False)
def migrate_project_no_commit(
    client,
    force_template_update=False,
    skip_template_update=False,
    skip_docker_update=False,
    skip_migrations=False,
    progress_callback=None,
):
    """Migrate all project's entities but do not commit changes."""
    return migrate(
        client=client,
        force_template_update=force_template_update,
        skip_template_update=skip_template_update,
        skip_docker_update=skip_docker_update,
        skip_migrations=skip_migrations,
        progress_callback=progress_callback,
    )


@pass_local_client
def is_renku_project(client):
    """Check if repository is a renku project."""
    return is_renku_project_helper(client)
