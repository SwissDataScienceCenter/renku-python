# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""PoC command for testing the new graph design."""

import click

from renku.cli.utils.callback import ClickCallback
from renku.cli.utils.click import CaseInsensitiveChoice
from renku.core.incubation.command import Command
from renku.core.incubation.graph import FORMATS, add_to_dataset, create_dataset, export_graph, generate_graph
from renku.core.incubation.graph import status as get_status
from renku.core.incubation.graph import update as perform_update
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.utils.contexts import measure


@click.group(hidden=True)
def graph():
    """Proof-of-Concept command for testing the new graph design."""


@graph.command()
@click.option("-f", "--force", is_flag=True, help="Delete existing metadata and regenerate all.")
def generate(force):
    """Create new graph metadata."""

    communicator = ClickCallback()
    generate_graph().with_communicator(communicator).build().execute(force=force)

    click.secho("\nOK", fg="green")


@graph.command()
@click.pass_context
def status(ctx):
    r"""Equivalent of `renku status`."""

    communicator = ClickCallback()
    result = get_status().with_communicator(communicator).build().execute()

    stales, modified, deleted = result.output

    if not modified and not deleted:
        click.secho("Everything is up-to-date.", fg="green")
        return

    if stales:
        click.echo(
            f"Outdated outputs({len(stales)}):\n"
            "  (use `renku log [<file>...]` to see the full lineage)\n"
            "  (use `renku update [<file>...]` to generate the file from its latest inputs)\n"
        )
        for k, v in stales.items():
            paths = click.style(", ".join(sorted(v)), fg="red", bold=True)
            click.echo(f"\t{k}:{paths}")
        click.echo()
    else:
        click.secho("All files were generated from the latest inputs.", fg="green")

    if modified:
        click.echo(
            f"Modified inputs({len(modified)}):\n"
            "  (use `renku log --revision <sha1> <file>` to see a lineage for the given revision)\n"
        )
        for v in modified:
            click.echo(click.style(f"\t{v}", fg="blue", bold=True))
        click.echo()

    if deleted:
        click.echo(
            "Deleted files used to generate outputs:\n"
            "  (use `git show <sha1>:<file>` to see the file content for the given revision)\n"
        )
        for v in deleted:
            click.echo(click.style(f"\t{v}", fg="blue", bold=True))

        click.echo()

    ctx.exit(1 if stales else 0)


@graph.command()
@click.option("-n", "--dry-run", is_flag=True, help="Show steps that will be updated without running them.")
def update(dry_run):
    r"""Equivalent of `renku update`."""

    communicator = ClickCallback()
    perform_update().with_communicator(communicator).build().execute(dry_run=dry_run)


@graph.command()
@click.argument("path", type=click.Path(exists=False, dir_okay=False))
def save(path):
    r"""Save dependency graph as PNG."""
    with measure("CREATE DEPENDENCY GRAPH"):

        def _to_png(client, path):
            dg = DependencyGraph.from_json(client.dependency_graph_path)
            dg.to_png(path=path)

        Command().command(_to_png).build().execute(path=path)


@graph.command()
@click.option("--format", type=CaseInsensitiveChoice(FORMATS), default="json-ld", help="Choose an output format.")
@click.option("--strict", is_flag=True, default=False, help="Validate triples before output.")
@click.option("--workflows-only", is_flag=True, help="Exclude datasets metadata from export.")
def export(format, strict, workflows_only):
    r"""Equivalent of `renku log --format json-ld`."""
    communicator = ClickCallback()
    export_graph().with_communicator(communicator).build().execute(
        format=format, workflows_only=workflows_only, strict=strict
    )


@graph.group()
def dataset():
    """Proof-of-Concept command for dataset operations using new metadata."""


@dataset.command()
@click.argument("name")
@click.option("-t", "--title", default=None, type=click.STRING, help="Title of the dataset.")
@click.option("-d", "--description", default=None, type=click.STRING, help="Dataset's description.")
@click.option(
    "-c",
    "--creator",
    "creators",
    default=None,
    multiple=True,
    help="Creator's name, email, and affiliation. Accepted format is ``Forename Surname <email> [affiliation]``.",
)
@click.option("-k", "--keyword", default=None, multiple=True, type=click.STRING, help="List of keywords or tags.")
def create(name, title, description, creators, keyword):
    """Create a new dataset."""
    communicator = ClickCallback()

    result = (
        create_dataset()
        .with_communicator(communicator)
        .build()
        .execute(name=name, title=title, description=description, creators=creators, keywords=keyword)
    )

    new_dataset = result.output

    click.echo(f"Use the name ``{new_dataset.name}`` to refer to this dataset.")
    click.secho("OK", fg="green")


@dataset.command()
@click.argument("name")
@click.argument("urls", nargs=-1)
@click.option("-e", "--external", is_flag=True, help="Creates a link to external data.")
@click.option("--force", is_flag=True, help="Allow adding otherwise ignored files.")
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing files.")
@click.option("-c", "--create", is_flag=True, help="Create dataset if it does not exist.")
@click.option("-s", "--source", "sources", default=None, multiple=True, help="Paths within remote git repo to be added")
@click.option("-d", "--destination", "destination", default="", help="Destination directory within the dataset path")
@click.option("--ref", default=None, help="Add files from a specific commit/tag/branch.")
def add(name, urls, external, force, overwrite, create, sources, destination, ref):
    """Add data to a dataset."""
    communicator = ClickCallback()

    add_to_dataset().with_communicator(communicator).build().execute(
        urls=urls,
        name=name,
        external=external,
        force=force,
        overwrite=overwrite,
        create=create,
        sources=sources,
        destination=destination,
        ref=ref,
    )
    click.secho("OK", fg="green")
