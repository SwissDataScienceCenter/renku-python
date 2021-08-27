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

Recreating outdated files
~~~~~~~~~~~~~~~~~~~~~~~~~

The information about dependencies for each file in a Renku project is stored
in various metadata.

When an update command is executed, Renku looks into the most recent execution
of each workflow and checks which one is outdated (i.e. at least one of its
inputs is modified). It generates a minimal dependency graph for each outdated
file stored in the repository. It means that only the necessary steps will be
executed.

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

In this situation, you can do effectively two things:

* Recreate a single file by running

  .. code-block:: console

     $ renku update E H

* Update all files by simply running

  .. code-block:: console

     $ renku update --all

.. note:: If there were uncommitted changes then the command fails.
   Check :program:`git status` to see details.

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

If a tool produces multiple output files, these outputs need to be always
updated together.

.. code-block:: text

                   (B)
                  /
    *A*--[step 1]--(C)
                  \
                   (D)

An attempt to update a single file would updates its siblings as well.

The following commands will produce the same result.

.. code-block:: console

   $ renku update C
   $ renku update B C D

"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.update import update_command


@click.command()
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all outdated files.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
def update(update_all, paths):
    """Update existing files by rerunning their outdated workflow."""
    communicator = ClickCallback()
    update_command().with_communicator(communicator).build().execute(update_all=update_all, paths=paths)
