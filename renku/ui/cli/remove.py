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
"""Remove a file, a directory, or a symlink.

Description
~~~~~~~~~~~

Remove a file from then project, updating:

* its metadata, if the file belongs to a dataset
* the tracking information, if the file is stored in an external storage
  (using Git LFS).

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.remove:remove
   :prog: renku rm
   :nested: full

.. cheatsheet::
   :group: Misc
   :command: $ renku rm <path>...
   :description: Safely delete files from a project.
   :target: rp
"""

import click

from renku.ui.cli.utils.callback import ClickCallback


@click.command(name="rm")
@click.argument("sources", type=click.Path(exists=True), nargs=-1, required=True)
def remove(sources):
    """Remove files and check repository for potential problems."""
    from renku.command.remove import remove_command

    communicator = ClickCallback()
    remove_command().with_communicator(communicator).build().execute(sources=sources, edit_command=click.edit)
