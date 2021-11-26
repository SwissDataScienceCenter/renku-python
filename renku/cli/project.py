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

Showing project metadata
~~~~~~~~~~~~~~~~~~~~~~~~

You can see the metadata of the current project by using ``renku project show``:
  .. code-block:: console

     $ renku project show
     Id: /projects/john.doe/flights-tutorial
     Name: flights-tutorial
     Description: Flight tutorial project
     Creator: John Doe <John Doe@datascience.ch>
     Created: 2021-11-05T10:32:57+01:00
     Keywords: keyword1, keyword2
     Renku Version: 1.0.0
     Project Template: python-minimal (1.0.0)


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


@click.group()
def project():
    """Project commands."""


@project.command()
@click.option("-d", "--description", default=None, type=click.STRING, help="Project's description.")
@click.option("-k", "--keyword", default=None, multiple=True, type=click.STRING, help="List of keywords.")
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
def edit(description, keyword, creator, metadata):
    """Edit project metadata."""
    from renku.core.commands.project import edit_project_command

    custom_metadata = None

    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    result = (
        edit_project_command()
        .build()
        .execute(description=description, creator=creator, keywords=keyword, custom_metadata=custom_metadata)
    )

    updated, no_email_warning = result.output

    if not updated:
        click.echo("Nothing to update. Check available fields with `renku project edit --help`\n")
    else:
        click.echo("Successfully updated: {}.".format(", ".join(updated.keys())))
        if no_email_warning:
            click.echo(ClickCallback.WARNING + f"No email or wrong format for: {no_email_warning}")


def _print_project(project):
    """Print project metadata."""
    click.echo(click.style("Id: ", bold=True, fg="magenta") + click.style(project.id, bold=True))
    click.echo(click.style("Name: ", bold=True, fg="magenta") + click.style(project.name, bold=True))
    click.echo(click.style("Description: ", bold=True, fg="magenta") + click.style(project.description, bold=True))
    click.echo(click.style("Creator: ", bold=True, fg="magenta") + click.style(project.creator_str, bold=True))
    click.echo(click.style("Created: ", bold=True, fg="magenta") + click.style(project.created_str, bold=True))
    click.echo(click.style("Keywords: ", bold=True, fg="magenta") + click.style(project.keywords_str, bold=True))
    click.echo(click.style("Renku Version: ", bold=True, fg="magenta") + click.style(project.agent, bold=True))
    click.echo(
        click.style("Project Template: ", bold=True, fg="magenta") + click.style(project.template_info, bold=True)
    )

    if project.annotations:
        click.echo(click.style("Annotations: ", bold=True, fg="magenta") + click.style(project.annotations, bold=True))


@project.command()
def show():
    """Show details for the project."""
    from renku.core.commands.project import show_project_command

    project = show_project_command().build().execute().output

    _print_project(project)
