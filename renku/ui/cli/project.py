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
r"""Renku CLI commands for handling of projects.

Description
~~~~~~~~~~~

Show and edit project's metadata.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.project:project
   :prog: renku project
   :nested: full
"""

import json
from pathlib import Path

import click

import renku.ui.cli.utils.color as color
from renku.core.util.util import NO_VALUE
from renku.ui.cli.utils.callback import ClickCallback


@click.group()
def project():
    """Project commands."""


@project.command()
@click.option("-d", "--description", default=NO_VALUE, type=click.UNPROCESSED, help="Project's description.")
@click.option(
    "-k", "--keyword", "keywords", default=[NO_VALUE], multiple=True, type=click.UNPROCESSED, help="List of keywords."
)
@click.option(
    "-c",
    "--creator",
    "creators",
    default=[NO_VALUE],
    type=click.UNPROCESSED,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option(
    "-m",
    "--metadata",
    default=NO_VALUE,
    type=click.UNPROCESSED,
    help="Custom metadata to be associated with the project.",
)
@click.option(
    "-u",
    "--unset",
    default=[],
    multiple=True,
    type=click.Choice(["keywords", "k", "metadata", "m"]),
    help="Remove keywords from dataset.",
)
@click.option(
    "--metadata-source",
    type=click.UNPROCESSED,
    default=NO_VALUE,
    help="Set the source field in the metadata when editing it if not provided, then the default is 'renku'.",
)
def edit(description, keywords, creators, metadata, unset, metadata_source):
    """Edit project metadata."""
    from renku.command.project import edit_project_command

    if list(creators) == [NO_VALUE]:
        creators = NO_VALUE

    if list(keywords) == [NO_VALUE]:
        keywords = NO_VALUE

    if "k" in unset or "keywords" in unset:
        if keywords is not NO_VALUE:
            raise click.UsageError("Cant use '--keyword' together with unsetting keywords")
        keywords = None

    if "m" in unset or "metadata" in unset:
        if metadata is not NO_VALUE:
            raise click.UsageError("Cant use '--metadata' together with unsetting metadata")
        metadata = None

    if metadata_source is not NO_VALUE and metadata is NO_VALUE:
        raise click.UsageError("The '--metadata-source' option can only be used with the '--metadata' flag")

    if metadata_source is NO_VALUE and metadata is not NO_VALUE:
        metadata_source = "renku"

    custom_metadata = metadata

    if metadata and metadata is not NO_VALUE:
        path = Path(metadata)

        if not path.exists():
            raise click.UsageError(f"Path {path} does not exist.")
        custom_metadata = json.loads(Path(metadata).read_text())

    result = (
        edit_project_command()
        .build()
        .execute(
            description=description,
            creator=creators,
            keywords=keywords,
            custom_metadata=custom_metadata,
            custom_metadata_source=metadata_source,
        )
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
    click.echo(click.style("Id: ", bold=True, fg=color.MAGENTA) + click.style(project.id, bold=True))
    click.echo(click.style("Name: ", bold=True, fg=color.MAGENTA) + click.style(project.name, bold=True))
    click.echo(click.style("Description: ", bold=True, fg=color.MAGENTA) + click.style(project.description, bold=True))
    click.echo(click.style("Creator: ", bold=True, fg=color.MAGENTA) + click.style(project.creator_str, bold=True))
    click.echo(click.style("Created: ", bold=True, fg=color.MAGENTA) + click.style(project.created_str, bold=True))
    click.echo(click.style("Keywords: ", bold=True, fg=color.MAGENTA) + click.style(project.keywords_str, bold=True))
    click.echo(click.style("Renku Version: ", bold=True, fg=color.MAGENTA) + click.style(project.agent, bold=True))
    click.echo(
        click.style("Project Template: ", bold=True, fg=color.MAGENTA) + click.style(project.template_info, bold=True)
    )

    if project.annotations:
        click.echo(
            click.style("Annotations: ", bold=True, fg=color.MAGENTA) + click.style(project.annotations, bold=True)
        )


@project.command()
def show():
    """Show details for the project."""
    from renku.command.project import show_project_command

    project = show_project_command().build().execute().output

    _print_project(project)
