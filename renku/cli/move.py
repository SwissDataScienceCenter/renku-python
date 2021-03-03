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

Moving a file that belongs to a dataset will update its metadata. It also
will attempt to update tracking information for files stored in an external
storage (using Git LFS). Finally it makes sure that all relative symlinks work
after the move.
"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.move import move_command


@click.command(name="mv")
@click.argument("sources", type=click.Path(exists=True), nargs=-1)
@click.argument("destination", type=click.Path(), nargs=1)
def move(sources, destination):
    """Move files and check repository for potential problems."""
    communicator = ClickCallback()
    move_command().with_communicator(communicator).build().execute(
        sources=sources, destination=destination, edit_command=click.edit
    )
