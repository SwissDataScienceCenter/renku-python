# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

Description
~~~~~~~~~~~

Clone a project and set up everything to use `renku` right away.

This command is preferred over using ``git clone`` because you don't need
to manually set up the required Git hooks, nor enable Git-LFS. It takes
care of that automatically.

Mind that, by default, ``renku clone`` pulls data from Git-LFS after cloning.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.clone:clone
   :prog: renku clone
   :nested: full


Handle special cases
~~~~~~~~~~~~~~~~~~~~

To clone private repositories with an HTTPS address, you first need to log into
a Renku deployment using the :ref:`cli-login` command. ``renku clone`` will use
the stored credentials when available.

To move a project to another Renku deployment you need to create a
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

import renku.ui.cli.utils.color as color


@click.command()
@click.option("--no-pull-data", is_flag=True, help="Do not pull data from Git-LFS.", default=False)
@click.argument("url")
@click.argument("path", required=False, default=None)
def clone(no_pull_data, url, path):
    """Clone a Renku repository."""
    from renku.command.clone import project_clone_command
    from renku.core.util.git import get_git_progress_instance

    click.echo(f"Cloning {url} ...")
    project_clone_command().build().execute(
        url=url, path=path, skip_smudge=no_pull_data, progress=get_git_progress_instance(), use_renku_credentials=True
    )
    click.secho("OK", fg=color.GREEN)
