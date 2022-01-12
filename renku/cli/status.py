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
"""Show status of data files created in the repository.

Inspecting a repository
~~~~~~~~~~~~~~~~~~~~~~~

``renku status`` command can be used to check if there are output files in
a repository that are outdated and need to be re-generated. Output files get
outdated due to changes in input data or source code (i.e. dependencies).

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
   :extended:
"""

import click

from renku.cli.utils.callback import ClickCallback


@click.command()
@click.pass_context
@click.argument("paths", type=click.Path(exists=True, dir_okay=False), nargs=-1)
def status(ctx, paths):
    """Show a status of the repository."""
    from renku.core.commands.status import get_status_command

    communicator = ClickCallback()
    result = get_status_command().with_communicator(communicator).build().execute(paths=paths)

    stales, stale_activities, modified, deleted = result.output

    if not stales and not deleted and not stale_activities:
        click.secho("Everything is up-to-date.", fg="green")
        return

    if stales:
        click.echo(
            f"Outdated outputs({len(stales)}):\n"
            # TODO: Enable once renku workflow visualize is implemented
            # "  (use `renku workflow visualize [<file>...]` to see the full lineage)\n"
            "  (use `renku update --all` to generate the file from its latest inputs)\n"
        )
        for k in sorted(stales.keys()):
            v = stales[k]
            paths = click.style(", ".join(sorted(v)), fg="blue", bold=True)
            output = click.style(k, fg="red", bold=True)
            click.echo(f"\t{output}: {paths}")
    else:
        click.secho("All files were generated from the latest inputs.", fg="green")

    click.echo()

    if modified:
        click.echo(
            f"Modified inputs({len(modified)}):\n"
            # TODO: Enable once renku workflow visualize is implemented
            # "  (use `renku workflow visualize [<file>...]` to see the full lineage)\n"
        )
        for v in sorted(modified):
            click.echo(click.style(f"\t{v}", fg="blue", bold=True))
        click.echo()

    if deleted:
        click.echo("Deleted files used to generate outputs:\n")
        for v in sorted(deleted):
            click.echo(click.style(f"\t{v}", fg="blue", bold=True))
        click.echo()

    if stale_activities:
        click.echo(f"Outdated activities that have no outputs({len(stale_activities)}):\n")
        for k in sorted(stale_activities.keys()):
            v = stale_activities[k]
            paths = click.style(", ".join(sorted(v)), fg="blue", bold=True)
            activity = click.style(k, fg="red", bold=True)
            click.echo(f"\t{activity}: {paths}")
        click.echo()

    ctx.exit(1 if stales or stale_activities else 0)
