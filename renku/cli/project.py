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
r"""Renku CLI commands for handling of projects.

Editing projects
~~~~~~~~~~~~~~~~

Users can edit some project's metadata using by using ``renku project edit``
command.

The following options can be passed to this command to set various metadata
for a project.

+-------------------+------------------------------------------------------+
| Option            | Description                                          |
+===================+======================================================+
| -d, --description | Project's description.                               |
+-------------------+------------------------------------------------------+
| -c, --creator     | Creator's name, email, and an optional affiliation.  |
|                   | Accepted format is                                   |
|                   | 'Forename Surname <email> [affiliation]'.            |
+-------------------+------------------------------------------------------+
| -m, --metadata    | Path to json file containing custom metadata to be   |
|                   | added to the project knowledge graph.                |
+-------------------+------------------------------------------------------+
"""

import json
from pathlib import Path

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.project import edit_project_command


@click.group()
def project():
    """Project commands."""


@project.command()
@click.option("-d", "--description", default=None, type=click.STRING, help="Project's description.")
@click.option(
    "-c",
    "--creator",
    default=None,
    type=click.STRING,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option(
    "-m",
    "--metadata",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Custom metadata to be associated with the project.",
)
def edit(description, creator, metadata):
    """Edit project metadata."""
    custom_metadata = None

    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    result = (
        edit_project_command()
        .build()
        .execute(description=description, creator=creator, custom_metadata=custom_metadata)
    )

    updated, no_email_warning = result.output

    if not updated:
        click.echo("Nothing to update. Check available fields with `renku project edit --help`\n")
    else:
        click.echo("Successfully updated: {}.".format(", ".join(updated.keys())))
        if no_email_warning:
            click.echo(ClickCallback.WARNING + f"No email or wrong format for: {no_email_warning}")
