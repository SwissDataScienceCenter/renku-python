# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Manage the set of CWL files created by ``renku`` commands.

With no arguments, shows a list of captured CWL files. Several subcommands
are available to perform operations on CWL files.

Reference tools and workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Managing large number of tools and workflows with automatically generated
names may be cumbersome. The names can be added to the last executed
``run``, ``rerun`` or ``update`` command by running
``renku workflow set-name <name>``. The name can be added to an arbitrary
file in ``.renku/workflow/*.cwl`` anytime later.
"""

from collections import defaultdict
from pathlib import Path

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.graph import Graph
from renku.core.models.workflow.converters.cwl import CWLConverter

# TODO: Finish refactoring (ticket #703)


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


@click.group(invoke_without_command=True)
@pass_local_client(requires_migration=True)
@click.pass_context
def workflow(ctx, client):
    """List or manage workflows with subcommands."""
    if ctx.invoked_subcommand is None:
        from renku.core.models.refs import LinkReference

        names = defaultdict(list)
        for ref in LinkReference.iter_items(client, common_path="workflows"):
            names[ref.reference.name].append(ref.name)

        for path in client.workflow_path.glob("*.yaml"):
            click.echo(
                "{path}: {names}".format(
                    path=path.name, names=", ".join(click.style(_deref(name), fg="green") for name in names[path.name]),
                )
            )


def validate_path(ctx, param, value):
    """Detect a workflow path if it is not passed."""
    client = ctx.obj

    if value is None:
        from renku.core.models.provenance.activities import ProcessRun

        activity = client.process_commit()

        if not isinstance(activity, ProcessRun):
            raise click.BadParameter("No tool was found.")

        return activity.path

    return value


@workflow.command("set-name")
@click.argument("name", metavar="<name>")
@click.argument(
    "path",
    metavar="<path>",
    type=click.Path(exists=True, dir_okay=False),
    callback=validate_path,
    default=None,
    required=False,
)
@click.option("--force", is_flag=True, help="Override the existence check.")
@pass_local_client(clean=True, commit=True)
def set_name(client, name, path, force):
    """Sets the <name> for remote <path>."""
    from renku.core.models.refs import LinkReference

    LinkReference.create(client=client, name=_ref(name), force=force).set_reference(path)


@workflow.command()
@click.argument("old", metavar="<old>")
@click.argument("new", metavar="<new>")
@click.option("--force", is_flag=True, help="Override the existence check.")
@pass_local_client(clean=True, commit=True)
def rename(client, old, new, force):
    """Rename the workflow named <old> to <new>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(old)).rename(_ref(new), force=force)


@workflow.command()
@click.argument("name", metavar="<name>")
@pass_local_client(clean=True, commit=True)
def remove(client, name):
    """Remove the remote named <name>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(name)).delete()


@workflow.command()
@click.option("--revision", default="HEAD")
@click.option(
    "-o",
    "--output-file",
    metavar="FILE",
    type=click.Path(exists=False),
    default=None,
    help="Write workflow to the FILE.",
)
@click.argument("paths", type=click.Path(dir_okay=True), nargs=-1)
@pass_local_client
def create(client, output_file, revision, paths):
    """Create a workflow description for a file."""
    graph = Graph(client)
    outputs = graph.build(paths=paths, revision=revision)

    workflow = graph.as_workflow(outputs=outputs,)

    if output_file:
        output_file = Path(output_file)

    wf, path = CWLConverter.convert(workflow, client, path=output_file)

    if not output_file:
        click.echo(wf.export_string())
