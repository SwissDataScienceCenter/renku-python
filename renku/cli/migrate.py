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
import os

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import WARNING
from renku.core.commands.migrate import is_renku_project, migrate_project, migrate_project_no_commit


@click.command()
@click.option("--no-commit", is_flag=True, hidden=True, help="Do not commit changes after the migration.")
def migrate(no_commit):
    """Migrate to latest Renku version."""
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
        if not is_renku_project():
            click.secho(WARNING + "Not a renku project.")
        click.secho("No migrations required.")


@click.command()
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
