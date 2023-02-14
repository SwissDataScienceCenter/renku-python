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
"""Move or rename a file, a directory, or a symlink.

Description
~~~~~~~~~~~

Moving a file that belongs to a dataset will update its metadata to include its
new path and commit. Moreover, tracking information in an external storage
(e.g. Git LFS) will be updated.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.move:move
   :prog: renku mv
   :nested: full

Examples
~~~~~~~~

Move operation fails if a destination already exists in the repo;
use ``--force`` flag to overwrite them.

If you want to move files to another dataset use ``--to-dataset`` along with
destination's dataset name. This removes source paths from all datasets'
metadata that include them (if any) and adds them to the destination's dataset
metadata.

The following command moves ``data/src`` and ``README`` to ``data/dst``
directory and adds them to ``target-dataset``'s metadata. If the source files
belong to one or more datasets then they will be removed from their metadata.

.. code-block:: console

    $ renku mv data/src README data/dst --to-dataset target-dataset

.. cheatsheet::
   :group: Misc
   :command: $ renku mv <path>... <destination>
   :description: Safely move files within a project.
   :target: rp

"""

import click

from renku.ui.cli.utils.callback import ClickCallback


@click.command(name="mv")
@click.argument("sources", type=click.Path(exists=True), nargs=-1)
@click.argument("destination", type=click.Path(), nargs=1)
@click.option("-f", "--force", is_flag=True, help="Override existing files.")
@click.option("-v", "--verbose", is_flag=True, help="Show move sources and destinations.")
@click.option("--to-dataset", type=str, default=None, nargs=1, help="A target dataset to move files to.")
def move(sources, destination, force, verbose, to_dataset):
    """Move files and check repository for potential problems."""
    from renku.command.move import move_command

    communicator = ClickCallback()
    move_command().with_communicator(communicator).build().execute(
        sources=sources, destination=destination, force=force, verbose=verbose, to_dataset=to_dataset
    )
