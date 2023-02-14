# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Show status of data files created in the repository.

Description
~~~~~~~~~~~

Check if there are output files in a repository that are outdated and need
to be re-generated. Output files get outdated due to changes in input data
or source code (i.e. dependencies).

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.status:status
   :prog: renku status
   :nested: full

Examples
~~~~~~~~

This command shows a list of output files that need to be updated along with
a list of modified inputs for each file. It also display deleted inputs files
if any.

To check for a specific input or output files, you can pass them to this command:

.. code-block:: console

    $ renku status path/to/file1 path/to/file2

In this case, renku only checks if the specified path or paths are modified or
outdated and need an update, instead of checking all inputs and outputs.

The paths mentioned in the output are made relative to the current directory
if you are working in a subdirectory (this is on purpose, to help
cutting and pasting to other commands).

.. cheatsheet::
   :group: Running
   :command: $ renku status
   :description: The the status of generated output files in the project.
   :target: rp
"""

import click

import renku.ui.cli.utils.color as color
from renku.ui.cli.utils.callback import ClickCallback


@click.command()
@click.pass_context
@click.option("-i", "--ignore-deleted", is_flag=True, help="Ignore deleted paths.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=False), nargs=-1)
def status(ctx, paths, ignore_deleted):
    """Show a status of the repository."""
    from renku.command.status import get_status_command

    communicator = ClickCallback()
    result = (
        get_status_command().with_communicator(communicator).build().execute(paths=paths, ignore_deleted=ignore_deleted)
    ).output

    if (
        not result.outdated_outputs
        and not result.deleted_inputs
        and not result.outdated_activities
        and not result.modified_hidden_inputs
    ):
        click.secho("Everything is up-to-date.", fg=color.GREEN)
        return

    outdated = False

    if result.outdated_outputs:
        outdated = True
        click.echo(
            f"Outdated outputs({len(result.outdated_outputs)}):\n"
            "  (use `renku workflow visualize [<file>...]` to see the full lineage)\n"
            "  (use `renku update --all` to generate the file from its latest inputs)\n"
        )
        for k in sorted(result.outdated_outputs.keys()):
            v = result.outdated_outputs[k]
            paths = click.style(", ".join(sorted(v)), fg=color.BLUE, bold=True)
            output = click.style(k, fg=color.RED, bold=True)
            click.echo(f"\t{output}: {paths}")

    if result.modified_hidden_inputs:
        outdated = True
        click.echo(
            f"Outdated workflow files and their outputs({len(result.modified_hidden_inputs)}):\n"
            "  (use `renku run [<workflow-file>]` to generate the outputs from the latest workflow file)\n"
        )
        for k in sorted(result.modified_hidden_inputs.keys()):
            v = result.modified_hidden_inputs[k]
            outputs = click.style(", ".join(sorted(v)), fg=color.RED, bold=True)
            wff = click.style(k, fg=color.BLUE, bold=True)
            click.echo(f"\t{wff}: {outputs}")

        click.echo()

    if not outdated:
        click.secho("All files are generated from the latest inputs.", fg=color.GREEN)

    click.echo()

    if result.modified_inputs:
        click.echo(
            f"Modified inputs({len(result.modified_inputs)}):\n"
            # TODO: Enable once renku workflow visualize is implemented
            # "  (use `renku workflow visualize [<file>...]` to see the full lineage)\n"
        )
        for v in sorted(result.modified_inputs):
            click.echo(click.style(f"\t{v}", fg=color.BLUE, bold=True))
        click.echo()

    if result.deleted_inputs:
        click.echo("Deleted files used to generate outputs:\n")
        for v in sorted(result.deleted_inputs):
            click.echo(click.style(f"\t{v}", fg=color.BLUE, bold=True))
        click.echo()

    if result.outdated_activities:
        click.echo(f"Outdated activities that have no outputs({len(result.outdated_activities)}):\n")
        for k in sorted(result.outdated_activities.keys()):
            v = result.outdated_activities[k]
            paths = click.style(", ".join(sorted(v)), fg=color.BLUE, bold=True)
            activity = click.style(k, fg=color.RED, bold=True)
            click.echo(f"\t{activity}: {paths}")
        click.echo()

    ctx.exit(1 if result.outdated_outputs or result.outdated_activities else 0)
