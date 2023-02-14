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
"""Rollback project to a previous point in time.

Description
~~~~~~~~~~~

Undo actions taken using Renku in a project.
This command shows a list of all actions done by renku and lets you pick
one that you want to return to, discarding any changes done in the repo (by
Renku or manually) after that point.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.rollback:rollback
   :prog: renku rollback
   :nested: full

Examples
~~~~~~~~

Once you pick a checkpoint to return to,
the commands shows all files and Renku objects that would be affected by the
rollback and how they would be affected. If you confirm, the project will be
reset to that point in time, with anything done after that point being
deleted/lost.

.. code-block:: console

    $ renku rollback
    Select a checkpoint to roll back to:

    [0] 2021-10-20 09:50:04         renku workflow edit cp-blabla-asdasf-0b535 --name test
    [1] 2021-10-20 09:49:19         renku rerun asdasf
    [2] 2021-10-20 09:48:59         renku run cp blabla asdasf
    [3] 2021-10-20 08:37:00         renku dataset add e blabla
    [4] 2021-10-20 08:31:16         renku dataset create m
    Checkpoint ([q] to quit) [q]: 4
    The following changes would be done:

    Metadata:

        Modified ♻️:
            Dataset: e

        Removed 🔥:
            Plan: cp-blabla-asdasf-0b535
            Plan: test
            Run: /activities/cc3ab70952fc499e93e7e4075a076bf5 (Plan name: cp-blabla-asdasf-0b535)
            Run: /activities/48b89b22567d4282abe8a016fa91878f (Plan name: cp-blabla-asdasf-0b535)

    Files:

        Restored ↻:
            blabla

        Removed 🔥:
            asdasf

    Proceed? [y/N]: y

.. note:: This command was introduced in renku-python version 1.0.0. Commands
   executed with previous versions of renku can't be rolled back to.

.. cheatsheet::
   :group: Undo Renku Command
   :command: $ renku rollback
   :description: Rollback project to a previous point in time.
   :target: rp,ui

"""

import click

from renku.ui.cli.utils.callback import ClickCallback


@click.command()
@click.pass_context
def rollback(ctx):
    """Rollback project to a previous point in time.

    Only renku commands executed after the 1.0.0 release are supported,
    previous commands won't show up.
    """
    from renku.command.rollback import rollback_command

    communicator = ClickCallback()
    rollback_command().with_communicator(communicator).build().execute()
