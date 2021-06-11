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
"""Migrate project to the latest Renku version.

When the way Renku stores metadata changes or there are other changes to the
project structure or data that are needed for Renku to work, ``renku migrate``
can be used to bring the project up to date with the current version of Renku.
This does not usually affect how you use Renku and no data is lost.

In addition, ``renku migrate`` will update your ``Dockerfile` to install the
latest version of ``renku-python``, if supported, making sure your renku
version is up to date in interactive environments as well.

If you created your repository from a project template and the template has
changed since you created the project, it will also update files with their
newest version from the template, without overwriting local changes if there
are any.

You can check if a migration is necessary and what migrations are available
by running

.. code-block:: console

    $ renku migrate -c

"""
import json
import os

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.echo import ERROR, INFO
from renku.core.commands.migrate import (
    AUTOMATED_TEMPLATE_UPDATE_SUPPORTED,
    DOCKERFILE_UPDATE_POSSIBLE,
    MIGRATION_REQUIRED,
    NON_RENKU_REPOSITORY,
    TEMPLATE_UPDATE_POSSIBLE,
    UNSUPPORTED_PROJECT,
    check_immutable_template_files_command,
    check_project,
    migrate_project,
    migrations_check,
    migrations_versions,
)
from renku.core.errors import MigrationRequired, ProjectNotSupported


@click.command()
@click.option("-c", "--check", is_flag=True, help="Check if migration is required and quit.")
@click.option("--no-commit", is_flag=True, hidden=True, help="Do not commit changes after the migration.")
@click.option("-t", "--skip-template-update", is_flag=True, hidden=True, help="Do not update project template files.")
@click.option(
    "-d", "--skip-docker-update", is_flag=True, hidden=True, help="Do not update Dockerfile to current renku version."
)
def migrate(check, no_commit, skip_template_update, skip_docker_update):
    """Check for migration and migrate to the latest Renku project version."""
    status = check_project().build().execute().output

    template_update_possible = status & TEMPLATE_UPDATE_POSSIBLE and status & AUTOMATED_TEMPLATE_UPDATE_SUPPORTED
    docker_update_possible = status & DOCKERFILE_UPDATE_POSSIBLE

    if check:
        if template_update_possible:
            click.secho(
                INFO
                + "The project template used to create this project has updates which can be applied "
                + "using 'renku migrate'."
            )
        if docker_update_possible:
            click.secho(
                INFO
                + "The Dockerfile refers to an older version of renku and can be updated "
                + "using 'renku migrate'."
            )

        if status & MIGRATION_REQUIRED:
            raise MigrationRequired

    if status & UNSUPPORTED_PROJECT:
        raise ProjectNotSupported
    elif status & NON_RENKU_REPOSITORY:
        click.secho(ERROR + "Not a renku project.")
        return

    if check:
        return

    skip_docker_update = skip_docker_update or not docker_update_possible
    skip_template_update = skip_template_update or not template_update_possible

    communicator = ClickCallback()

    command = migrate_project().with_communicator(communicator)
    if not no_commit:
        command = command.with_commit()
    result = command.build().execute(skip_template_update=skip_template_update, skip_docker_update=skip_docker_update)

    result, _, _ = result.output

    if result:
        click.secho("OK", fg="green")
    else:
        click.secho("No migrations required.")


@click.command(hidden=True)
def migrationscheck():
    """Check status of the project and current renku-python version."""
    latest_version, project_version = migrations_versions().build().execute().output
    (
        migration_required,
        project_supported,
        template_update_possible,
        current_template_version,
        latest_template_version,
        template_source,
        template_ref,
        template_id,
        automated_update,
        docker_update_possible,
    ) = (
        migrations_check().lock_project().build().execute().output
    )

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
                "template_source": template_source,
                "template_ref": template_ref,
                "template_id": template_id,
                "automated_update": automated_update,
                "docker_update_possible": docker_update_possible,
            }
        )
    )


@click.command(hidden=True)
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def check_immutable_template_files(paths):
    """Check specified paths if they are marked immutable in the template."""
    result = check_immutable_template_files_command().build().execute(paths=paths)
    paths = result.output
    if paths:
        click.echo(os.linesep.join(paths))
        exit(1)
