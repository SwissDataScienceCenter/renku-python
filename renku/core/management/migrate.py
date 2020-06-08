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
"""Renku migrations management.

Migrations files are put in renku/core/management/migrations directory. Name
of these files has m_1234__name.py format where 1234 is the migration version
and name can be any alphanumeric and underscore combination. Migration files
are sorted based on their lowercase name. Each migration file must define a
public "migrate" function that accepts a client as its argument.

When executing a migration, the migration file is imported as a module and the
"migrate" function is executed. Migration version is checked against the Renku
project version (in .renku/metadata.yml) and any migration which has a higher
version is applied to the project.
"""
import importlib
import re
from pathlib import Path

import pkg_resources

from renku.core.errors import MigrationRequired, ProjectNotSupported

SUPPORTED_PROJECT_VERSION = 4


def check_for_migration(client):
    """Checks if migration is required."""
    if is_migration_required(client):
        raise MigrationRequired
    elif is_project_unsupported(client):
        raise ProjectNotSupported


def is_migration_required(client):
    """Check if project requires migration."""
    return (
        _is_renku_project(client) and
        _get_project_version(client) < SUPPORTED_PROJECT_VERSION
    )


def is_project_unsupported(client):
    """Check if this version of Renku cannot work with the project."""
    return (
        _is_renku_project(client) and
        _get_project_version(client) > SUPPORTED_PROJECT_VERSION
    )


def migrate(client, progress_callback=None):
    """Apply all migration files to the project."""
    if not _is_renku_project(client):
        return

    project_version = _get_project_version(client)
    n_migrations_executed = 0

    for version, path in get_migrations():
        if version > project_version:
            module = importlib.import_module(path)
            if progress_callback:
                module_name = module.__name__.split('.')[-1]
                progress_callback(f'Applying migration {module_name}...')
            module.migrate(client)
            n_migrations_executed += 1
    if n_migrations_executed > 0:
        client.project.version = str(version)
        client.project.to_yaml()

        if progress_callback:
            progress_callback(
                f'Successfully applied {n_migrations_executed} migrations.'
            )

    return n_migrations_executed != 0


def _get_project_version(client):
    try:
        return int(client.project.version)
    except ValueError:
        return 1


def _is_renku_project(client):
    return client.project is not None


def get_migrations():
    """Return a sorted list of versions and migration modules."""
    migrations = []
    for file_ in pkg_resources.resource_listdir(
        'renku.core.management', 'migrations'
    ):
        match = re.search(r'm_([0-9]{4})__[a-zA-Z0-9_-]*.py', file_)

        if match is None:  # migration files match m_0000__[name].py format
            continue

        version = int(match.groups()[0])
        path = 'renku.core.management.migrations.{}'.format(Path(file_).stem)
        migrations.append((version, path))

    migrations = sorted(migrations, key=lambda v: v[1].lower())
    return migrations
