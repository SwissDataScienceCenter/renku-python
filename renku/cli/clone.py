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
"""Clone a Renku project.

Cloning a Renku project
~~~~~~~~~~~~~~~~~~~~~~~

To clone a Renku project use ``renku clone`` command. This command is preferred
over using ``git clone`` because it sets up required Git hooks and enables
Git-LFS automatically.

.. code-block:: console

    $ renku clone <repository-url> <destination-directory>

It creates a new directory with the same name as the project. You can change
the directory name by passing another name on the command line.

By default, ``renku clone`` pulls data from Git-LFS after cloning. If you don't
need the LFS data, pass ``--no-pull-data`` option to skip this step.

.. Note:: To move a project to another Renku deployment you need to create a
    new empty project in the target deployment and push both the
    repository and Git-LFS objects to the new remote. Refer to Git documentation
    for more details.

    .. code-block:: console

        $ git lfs fetch --all
        $ git remote remove origin
        $ git remote add origin <new-repository-url>
        $ git push --mirror origin
"""

import click

from renku.core.commands.clone import project_clone
from renku.core.commands.echo import GitProgress


@click.command()
@click.option("--no-pull-data", is_flag=True, help="Do not pull data from Git-LFS.", default=False)
@click.argument("url")
@click.argument("path", required=False, default=None)
def clone(no_pull_data, url, path):
    """Clone a Renku repository."""
    click.echo(f"Cloning {url} ...")
    project_clone(url=url, path=path, skip_smudge=no_pull_data, progress=GitProgress())
    click.secho("OK", fg="green")
