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
r"""Update outdated files created by the "run" command.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.update:update
   :prog: renku update
   :nested: full

Recreating outdated files
~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: ../../_static/asciicasts/update.delay.gif
   :width: 850
   :alt: Update outdated files


The information about dependencies for each file in a Renku project is stored
in various metadata.

When an update command is executed, Renku looks into the most recent execution
of each workflow (Plan and Activity combination) and checks which one is outdated
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

In some cases it may be desirable to avoid updating the renku metadata
and to avoid committing this and any other change in the repository when the update
command is run. If this is the case then you can pass the ``--skip-metadata-update``
flag to ``renku update``.

.. cheatsheet::
   :group: Running
   :command: $ renku update [--all] [<path>...]
   :description: Update outdated output files created by renku run. With
                 <path>'s: Only recreate these files. With --all: Update
                 all outdated output files.
   :target: rp

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

Ignoring deleted paths
~~~~~~~~~~~~~~~~~~~~~~

The update command will regenerate any deleted files/directories. If you don't
want to regenerate deleted paths, pass ``--ignore-deleted`` to the update
command. You can make this the default behavior by setting
``update_ignore_delete`` config value for a project or globally:

  .. code-block:: console

     $ renku config set [--global] update_ignore_delete True

Note that deleted path always will be regenerated if they have siblings or
downstream dependencies that aren't deleted.

"""

import click
from lazy_object_proxy import Proxy

from renku.core import errors
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.plugins import available_workflow_providers


@click.command()
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all outdated files.")
@click.option("--dry-run", "-n", is_flag=True, default=False, help="Show a preview of plans that will be executed.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.option(
    "provider",
    "-p",
    "--provider",
    default="toil",
    show_default=True,
    type=click.Choice(Proxy(available_workflow_providers), case_sensitive=False),
    help="The workflow engine to use.",
)
@click.option(
    "config", "-c", "--config", metavar="<config file>", help="YAML file containing configuration for the provider."
)
@click.option("-i", "--ignore-deleted", is_flag=True, help="Ignore deleted paths.")
@click.option("--skip-metadata-update", is_flag=True, help="Do not update the metadata store for the execution.")
def update(update_all, dry_run, paths, provider, config, ignore_deleted, skip_metadata_update):
    """Update existing files by rerunning their outdated workflow."""
    from renku.command.format.activity import tabulate_activities
    from renku.command.update import update_command

    communicator = ClickCallback()

    try:
        result = (
            update_command(skip_metadata_update=skip_metadata_update)
            .with_communicator(communicator)
            .build()
            .execute(
                update_all=update_all,
                dry_run=dry_run,
                paths=paths,
                provider=provider,
                config=config,
                ignore_deleted=ignore_deleted,
            )
        )
    except errors.NothingToExecuteError:
        exit(1)
    else:
        if dry_run:
            activities, modified_inputs = result.output
            click.echo(tabulate_activities(activities, modified_inputs))
