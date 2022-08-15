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
"""Custom git merge tool for renku metadata.

Description
~~~~~~~~~~~

Renku stores all of its metadata in the repository, in compressed form.
When working in multiple branches with Renku, this metadata needs to be merged
when a git merge is made. To support users when doing this, Renku provides a
custom merge tool that takes care of merging the metadata.

The merge tool is set up automatically when creating a new project or when
using ``renku clone`` to clone a Renku project.
Keep in mind the merge tool configuration cannot be shared through remote
repositories and has to be set up on each clone separately.

You can manually set up the merge tool by running ``renku mergetool install``.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.mergetool:mergetool
   :prog: renku mergetool
   :nested: full
"""

import click

from renku.ui.cli.utils.callback import ClickCallback


@click.group()
def mergetool():
    """Mergetool commands."""
    pass


@mergetool.command(hidden=True)
@click.argument("base", type=click.Path(exists=True))
@click.argument("local", type=click.Path(exists=True))
@click.argument("remote", type=click.Path(exists=True))
def merge(base, local, remote):
    """Move files and check repository for potential problems."""
    from renku.command.mergetool import mergetool_command

    communicator = ClickCallback()
    mergetool_command().with_communicator(communicator).build().execute(local=local, remote=remote, base=base)


@mergetool.command()
def install():
    """Setup mergetool locally."""
    from renku.command.mergetool import mergetool_install_command

    mergetool_install_command().with_commit(commit_only=[".gitattributes"]).require_clean().build().execute()
