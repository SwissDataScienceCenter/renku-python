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

Removing a file that belongs to a dataset will update its metadata. It also
will attempt to update tracking information for files stored in an external
storage (using Git LFS).

.. cheatsheet::
   :group: Misc
   :command: $ renku rm <path>...
   :description: Safely delete files from a project.
   :extended:

"""

import click

from renku.cli.utils.callback import ClickCallback


@click.command(name="rm")
@click.argument("sources", type=click.Path(exists=True), nargs=-1, required=True)
def remove(sources):
    """Remove files and check repository for potential problems."""
    from renku.core.commands.remove import remove_command

    communicator = ClickCallback()
    remove_command().with_communicator(communicator).build().execute(sources=sources, edit_command=click.edit)
