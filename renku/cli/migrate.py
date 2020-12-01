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
import json
import os

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import WARNING
from renku.core.commands.migrate import (
    MIGRATION_REQUIRED,
    NON_RENKU_REPOSITORY,
    UNSUPPORTED_PROJECT,
    check_project,
    migrate_project,
    migrate_project_no_commit,
    migrations_check,
    migrations_versions,
)
from renku.core.errors import MigrationRequired, ProjectNotSupported


@click.command()
@click.option("-c", "--check", is_flag=True, help="Check if migration is required and quit.")
@click.option("--no-commit", is_flag=True, hidden=True, help="Do not commit changes after the migration.")
def migrate(check, no_commit):
    """Check for migration and migrate to the latest Renku project version."""
    if check:
        status = check_project()
        if status == MIGRATION_REQUIRED:
            raise MigrationRequired
        elif status == UNSUPPORTED_PROJECT:
            raise ProjectNotSupported
        elif status == NON_RENKU_REPOSITORY:
            click.secho(WARNING + "Not a renku project.")

        return

    if no_commit:
        result, _, _ = migrate_project_no_commit(
            skip_template_update=True, skip_docker_update=True, progress_callback=click.secho,
        )
    else:
        result, _, _ = migrate_project(
            skip_template_update=True, skip_docker_update=True, progress_callback=click.secho,
        )

    if result:
        click.secho("OK", fg="green")
    else:
        if check_project() == NON_RENKU_REPOSITORY:
            click.secho(WARNING + "Not a renku project.")
        click.secho("No migrations required.")


@click.command(hidden=True)
def migrationscheck():
    """Check status of the project and current renku-python version."""
    latest_version, project_version = migrations_versions()
    (
        migration_required,
        project_supported,
        template_update_possible,
        current_template_version,
        latest_template_version,
        automated_update,
        docker_update_possible,
    ) = migrations_check()

    click.echo(
        json.dumps(
            {
                "latest_version": latest_version,
                "project_version": project_version,
                "migration_required": migration_required,
                "project_supported": project_supported,
                "template_update_possible": template_update_possible,
                "current_template_version": str(current_template_version),
                "latest_template_version": str(latest_template_version),
                "automated_update": automated_update,
                "docker_update_possible": docker_update_possible,
            }
        )
    )


@click.command(hidden=True)
@click.argument(
    "paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True,
)
@pass_local_client
def check_immutable_template_files(client, paths):
    """Check specified paths if they are marked immutable in the template."""
    paths = client.check_immutable_template_files(*paths)
    if paths:
        click.echo(os.linesep.join(paths))
        exit(1)
