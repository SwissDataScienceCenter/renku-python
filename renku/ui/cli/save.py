# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Convenience method to save local changes and push them to a remote server.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.save:save
   :prog: renku save
   :nested: full

Examples
~~~~~~~~

If you have local modification to files, you can save them using

.. code-block:: console

    $ renku save
    Username for 'https://renkulab.io': my.user
    Password for 'https://my.user@renkulab.io':
    Successfully saved:
        file1
        file2
    OK

.. warning:: The username and password for renku save are your gitlab
   user/password, not your renkulab login!

.. cheatsheet::
   :group: Misc
   :command: $ renku save [-m <message>]
   :description: Save (commit) and push all local changes. with optional
                 message.
   :target: rp

You can additionally supply a message that describes the changes that you
made by using the ``-m`` or ``--message`` parameter followed by your
message.

.. code-block:: console

    $ renku save -m "Updated file1 and 2."
    Successfully saved:
        file1
        file2
    OK

If no remote server has been configured, you can specify one by using the
``-d`` or ``--destination`` parameter. Otherwise you will get an error.

.. code-block:: console

    $ renku save
    Error: No remote has been set up for the current branch

    $ renku save -d https://renkulab.io/gitlab/my.user/my-project.git
    Successfully saved:
        file1
        file2
    OK

You can also specify which paths to save:

.. code-block:: console

    $ renku save file1
    Successfully saved:
        file1
    OK

"""

import click

import renku.ui.cli.utils.color as color
from renku.ui.cli.utils.callback import ClickCallback


@click.command(name="save")
@click.option("-m", "--message", default=None, help="The commit message to use")
@click.option(
    "-d",
    "--destination",
    default=None,
    help=(
        "The git remote to push to. Defaults to the remote set in git, " "automatically set in interactive environments"
    ),
)
@click.argument("paths", type=click.Path(exists=False, dir_okay=True), nargs=-1)
def save(message, destination, paths):
    """Save and push local changes."""
    from renku.command.save import save_and_push_command

    communicator = ClickCallback()
    saved_paths, branch = (
        save_and_push_command()
        .with_communicator(communicator)
        .build()
        .execute(message=message, remote=destination, paths=paths)
        .output
    )

    if saved_paths:
        paths_str = "\n\t".join(saved_paths)
        click.echo(f"Successfully saved to remote branch {branch}: \n\t{paths_str}")
    else:
        click.echo(f"Successfully saved to remote branch {branch}.")

    click.secho("OK", fg=color.GREEN)
