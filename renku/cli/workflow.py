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


import click
from rich.console import Console
from rich.markdown import Markdown

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.format.workflows import WORKFLOWS_COLUMNS, WORKFLOWS_FORMATS
from renku.core.commands.workflow import (
    create_workflow_command,
    list_workflows_command,
    remove_workflow_command,
    rename_workflow_command,
    set_workflow_name_command,
    show_workflow_command,
)


@click.group()
def workflow():
    """Workflow commands."""
    pass


@workflow.command("ls")
@click.option("--format", type=click.Choice(WORKFLOWS_FORMATS), default="tabular", help="Choose an output format.")
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="id,name,description",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(WORKFLOWS_COLUMNS.keys())),
    show_default=True,
)
def list_workflows(format, columns):
    """List or manage workflows with subcommands."""
    communicator = ClickCallback()
    result = list_workflows_command().with_communicator(communicator).build().execute(format=format, columns=columns)
    click.echo(result.output)


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
def set_name(name, path, force):
    """Sets the <name> for remote <path>."""
    set_workflow_name_command().build().execute(name=name, path=path, force=force)


@workflow.command()
@click.argument("old", metavar="<old>")
@click.argument("new", metavar="<new>")
@click.option("--force", is_flag=True, help="Override the existence check.")
def rename(old, new, force):
    """Rename the workflow named <old> to <new>."""
    rename_workflow_command().build().execute(old=old, new=new, force=force)


@workflow.command()
@click.argument("name", metavar="<name>")
def remove(name):
    """Remove the remote named <name>."""
    remove_workflow_command().build().execute(name=name)


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
def create(output_file, revision, paths):
    """Create a workflow description for a file."""
    result = create_workflow_command().build().execute(output_file=output_file, revision=revision, paths=paths)

    if not output_file:
        click.echo(result.output)


@workflow.command()
@click.argument("name_or_id", metavar="<name_or_id>")
def show(name_or_id):
    """Show details for workflow <name_or_id>."""
    plan = show_workflow_command().build().execute(name_or_id=name_or_id).output

    click.echo(click.style("Id: ", bold=True, fg="magenta") + click.style(plan.id_, bold=True))
    click.echo(click.style("Name: ", bold=True, fg="magenta") + click.style(plan.name, bold=True))
    Console().print(Markdown(plan.description))
    click.echo(click.style("Command: ", bold=True, fg="magenta") + click.style(plan.full_command, bold=True))
    click.echo(click.style("Success Codes: ", bold=True, fg="magenta") + click.style(plan.success_codes, bold=True))

    if plan.inputs:
        click.echo(click.style("Inputs: ", bold=True, fg="magenta"))
        for run_input in plan.inputs:
            click.echo(click.style(f"\t- {run_input.name}:", bold=True))

            if run_input.description:
                click.echo(click.style(f"\t\t{run_input.description}"))

            click.echo(
                click.style("\t\tDefault Value: ", bold=True, fg="magenta")
                + click.style(run_input.defaultValue, bold=True)
            )

            if run_input.position:
                click.echo(
                    click.style("\t\tPosition: ", bold=True, fg="magenta") + click.style(run_input.position, bold=True)
                )

            if run_input.prefix:
                click.echo(
                    click.style("\t\tPrefix: ", bold=True, fg="magenta") + click.style(run_input.prefix, bold=True)
                )

    if plan.outputs:
        click.echo(click.style("Outputs: ", bold=True, fg="magenta"))
        for run_output in plan.outputs:
            click.echo(click.style(f"\t- {run_output.name}:", bold=True))

            if run_output.description:
                click.echo(click.style(f"\t\t{run_output.description}"))

            click.echo(
                click.style("\t\tDefault Value: ", bold=True, fg="magenta")
                + click.style(run_output.defaultValue, bold=True)
            )

            if run_output.position:
                click.echo(
                    click.style("\t\tPosition: ", bold=True, fg="magenta") + click.style(run_output.position, bold=True)
                )

            if run_output.prefix:
                click.echo(
                    click.style("\t\tPrefix: ", bold=True, fg="magenta") + click.style(run_output.prefix, bold=True)
                )

    if plan.arguments:
        click.echo(click.style("Outputs: ", bold=True, fg="magenta"))
        for run_argument in plan.arguments:
            click.echo(click.style(f"\t- {run_argument.name}:", bold=True))

            if run_argument.description:
                click.echo(click.style(f"\t\t{run_argument.description}"))

            click.echo(
                click.style("\t\tDefault Value: ", bold=True, fg="magenta")
                + click.style(run_argument.defaultValue, bold=True)
            )

            if run_argument.position:
                click.echo(
                    click.style("\t\tPosition: ", bold=True, fg="magenta")
                    + click.style(run_argument.position, bold=True)
                )

            if run_argument.prefix:
                click.echo(
                    click.style("\t\tPrefix: ", bold=True, fg="magenta") + click.style(run_argument.prefix, bold=True)
                )
