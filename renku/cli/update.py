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

The information about dependencies for each file in the repository is generated
from information stored in the underlying Git repository.

A minimal dependency graph is generated for each outdated file stored in the
repository. It means that only the necessary steps will be executed and the
workflow used to orchestrate these steps is stored in the repository.

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

     $ renku update E

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

An attempt to update a single file would fail with the following error.

.. code-block:: console

   $ renku update C
   Error: There are missing output siblings:

        B
        D

   Include the files above in the command or use --with-siblings option.

The following commands will produce the same result.

.. code-block:: console

   $ renku update --with-siblings C
   $ renku update B C D

"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.options import option_siblings
from renku.core.commands.update import update_workflows


@click.command()
@click.option("--revision", default="HEAD")
@click.option("--no-output", is_flag=True, default=False, help="Display commands without output files.")
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all outdated files.")
@option_siblings
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
def update(revision, no_output, update_all, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    communicator = ClickCallback()

    update_workflows().with_communicator(communicator).build().execute(
        revision=revision, no_output=no_output, update_all=update_all, siblings=siblings, paths=paths
    )
