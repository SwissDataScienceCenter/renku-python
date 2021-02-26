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
"""Manage the set of CWL files created by ``renku`` commands.

Manipulating workflows
~~~~~~~~~~~~~~~~~~~~~~

Listing workflows:

.. code-block:: console

    $ renku workflow ls
    26be2e8d66f74130a087642768f2cef0_rerun.yaml:
    199c4b9d462f4b27a4513e5e55f76eb2_cat.yaml:
    9bea2eccf9624de387d9b06e61eec0b6_rerun.yaml:
    b681b4e229764ceda161f6551370af12_update.yaml:
    25d0805243e3468d92a3786df782a2c4_rerun.yaml:

Each ``*.yaml`` file corresponds to a renku run/update/rerun execution.

Exporting workflows:

You can export the workflow to create a file as Common Workflow Language
by using:

.. code-block:: console

    $ renku workflow set-name create output_file
    baseCommand:
    - cat
    class: CommandLineTool
    cwlVersion: v1.0
    id: 22943eca-fa4c-4f3b-a92d-f6ac7badc0d2
    inputs:
    - default:
        class: File
        path: /home/user/project/intermediate
    id: inputs_1
    inputBinding:
        position: 1
    type: File
    - default:
        class: File
        path: /home/user/project/intermediate2
    id: inputs_2
    inputBinding:
        position: 2
    type: File
    outputs:
    - id: output_stdout
    streamable: false
    type: stdout
    requirements:
    InitialWorkDirRequirement:
        listing:
        - entry: $(inputs.inputs_1)
        entryname: intermediate
        writable: false
        - entry: $(inputs.inputs_2)
        entryname: intermediate2
        writable: false
    stdout: output_file

You can use ``--revision`` to specify the revision of the output file to
generate the workflow for. You can also export to a file directly with
``-o <path>``.
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


@click.group()
def workflow():
    """Workflow commands."""
    pass


@workflow.command("ls")
@pass_local_client(requires_migration=True)
def list_workflows(client):
    """List or manage workflows with subcommands."""
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
