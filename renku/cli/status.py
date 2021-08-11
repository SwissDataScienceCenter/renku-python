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
from renku.core.commands.status import get_status


@click.command()
@click.pass_context
def status(ctx):
    """Show a status of the repository."""
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
