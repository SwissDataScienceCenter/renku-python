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
r"""Update outdated files created by the "run" command.

.. image:: ../_static/asciicasts/update.delay.gif
   :width: 850
   :alt: Update outdate files

Recreating outdated files
~~~~~~~~~~~~~~~~~~~~~~~~~

The information about dependencies for each file in a Renku project is stored
in various metadata.

When an update command is executed, Renku looks into the most recent execution
of each workflow (Run and Plan combination) and checks which one is outdated
(i.e. at least one of its inputs is modified). It generates a minimal
dependency graph for each outdated file stored in the repository. It means
that only the necessary steps will be executed.

Assume that the following history for the file ``H`` exists.

.. code-block:: text

          C---D---E
         /         \
    A---B---F---G---H

The first example shows situation when ``D`` is modified and files ``E`` and
``H`` become outdated.

.. code-block:: text

          C--*D*--(E)
         /          \
    A---B---F---G---(H)

    ** - modified
    () - needs update

In this situation, you can do effectively three things:

* Update all files

  .. code-block:: console

     $ renku update --all

* Update only ``E``

  .. code-block:: console

     $ renku update E

* Update ``E`` and ``H``

  .. code-block:: console

     $ renku update H

.. note:: If there were uncommitted changes then the command fails.
   Check :program:`git status` to see details.

.. cheatsheet::
   :group: Running
   :command: $ renku update [--all] [<path>...]
   :description: Update outdated output files created by renku run. With
                 <path>'s: Only recreate these files. With --all: Update
                 all outdated output files.
   :extended:

Pre-update checks
~~~~~~~~~~~~~~~~~

In the next example, files ``A`` or ``B`` are modified, hence the majority
of dependent files must be recreated.

.. code-block:: text

            (C)--(D)--(E)
           /            \
    *A*--*B*--(F)--(G)--(H)

To avoid excessive recreation of the large portion of files which could have
been affected by a simple change of an input file, consider specifying a single
file (e.g. ``renku update G``). See also :ref:`cli-status`.

.. _cli-update-with-siblings:

Update siblings
~~~~~~~~~~~~~~~

If a workflow step produces multiple output files, these outputs will be always
updated together.

.. code-block:: text

                   (B)
                  /
    *A*--[step 1]--(C)
                  \
                   (D)

An attempt to update a single file would update its siblings as well.

The following commands will produce the same result.

.. code-block:: console

   $ renku update C
   $ renku update B C D

"""

import click
from lazy_object_proxy import Proxy

from renku.cli.utils.callback import ClickCallback
from renku.cli.utils.plugins import available_workflow_providers
from renku.core import errors


@click.command()
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all outdated files.")
@click.option("--dry-run", "-n", is_flag=True, default=False, help="Show a preview of plans that will be executed.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.option(
    "provider",
    "-p",
    "--provider",
    default="cwltool",
    show_default=True,
    type=click.Choice(Proxy(available_workflow_providers), case_sensitive=False),
    help="The workflow engine to use.",
)
@click.option(
    "config", "-c", "--config", metavar="<config file>", help="YAML file containing configuration for the provider."
)
def update(update_all, dry_run, paths, provider, config):
    """Update existing files by rerunning their outdated workflow."""
    from renku.core.commands.format.activity import tabulate_activities
    from renku.core.commands.update import update_command

    communicator = ClickCallback()

    try:
        result = (
            update_command()
            .with_communicator(communicator)
            .build()
            .execute(update_all=update_all, dry_run=dry_run, paths=paths, provider=provider, config=config)
        )
    except errors.NothingToExecuteError:
        exit(1)
    else:
        if dry_run:
            activities, modified_inputs = result.output
            click.echo(tabulate_activities(activities, modified_inputs))
