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
import click

from renku.core.commands.migrate import migrate_project, \
    migrate_project_no_commit


@click.command()
@click.option(
    '--no-commit',
    is_flag=True,
    hidden=True,
    help='Do not commit changes after the migration.'
)
def migrate(no_commit):
    """Migrate to latest Renku version."""
    if no_commit:
        result = migrate_project_no_commit(progress_callback=click.secho)
    else:
        result = migrate_project(progress_callback=click.secho)

    if result:
        click.secho('OK', fg='green')
    else:
        click.secho('No migrations required.')
