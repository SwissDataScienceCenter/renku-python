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

Displays paths of outputs which were generated from newer inputs files
and paths of files that have been used in diverent versions.

The first paths are what need to be recreated by running ``renku update``.
See more in section about :ref:`renku update <cli-update>`.

The paths mentioned in the output are made relative to the current directory
if you are working in a subdirectory (this is on purpose, to help
cutting and pasting to other commands). They also contain first 8 characters
of the corresponding commit identifier after the ``#`` (hash). If the file was
imported from another repository, the short name of is shown together with the
filename before ``@``.
"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.ascii import _format_sha1
from renku.core.commands.status import get_status


@click.command()
@click.option("--revision", default="HEAD", help="Display status as it was in the given revision")
@click.option("--no-output", is_flag=True, default=False, help="Display commands without output files.")
@click.argument("path", type=click.Path(exists=True, dir_okay=False), nargs=-1)
@click.pass_context
def status(ctx, revision, no_output, path):
    """Show a status of the repository."""
    communicator = ClickCallback()

    result = (
        get_status().with_communicator(communicator).build().execute(revision=revision, no_output=no_output, path=path)
    )

    graph, status = result.output

    if status["outdated"]:
        click.echo(
            "Outdated outputs:\n"
            '  (use "renku log [<file>...]" to see the full lineage)\n'
            '  (use "renku update [<file>...]" to '
            "generate the file from its latest inputs)\n"
        )

        for filepath, stts in sorted(status["outdated"].items()):
            outdated = ", ".join(
                "{0}#{1}".format(click.style(graph._format_path(n.path), fg="blue", bold=True), _format_sha1(graph, n))
                for n in stts
                if n.path and n.path not in status["outdated"]
            )

            click.echo("\t{0}: {1}".format(click.style(graph._format_path(filepath), fg="red", bold=True), outdated))

        click.echo()

    else:
        click.secho("All files were generated from the latest inputs.", fg="green")

    if status["multiple-versions"]:
        click.echo(
            "Modified inputs:\n"
            '  (use "renku log --revision <sha1> <file>" to see a lineage '
            "for the given revision)\n"
        )

        for filepath, files in sorted(status["multiple-versions"].items()):
            # Do not show duplicated commits!  (see #387)
            commits = {_format_sha1(graph, key) for key in files}
            click.echo(
                "\t{0}: {1}".format(
                    click.style(graph._format_path(filepath), fg="blue", bold=True),
                    ", ".join(
                        # Sort the commit hashes alphanumerically to have a
                        # predictable output.
                        sorted(commits)
                    ),
                )
            )

        click.echo()

    if status["deleted"]:
        click.echo(
            "Deleted files used to generate outputs:\n"
            '  (use "git show <sha1>:<file>" to see the file content '
            "for the given revision)\n"
        )

        for filepath, node in status["deleted"].items():
            click.echo(
                "\t{0}: {1}".format(
                    click.style(graph._format_path(filepath), fg="blue", bold=True), _format_sha1(graph, node)
                )
            )

        click.echo()

    ctx.exit(1 if status["outdated"] else 0)
