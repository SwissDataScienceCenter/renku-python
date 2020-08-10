# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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

To clone a Renku project and set up required Git hooks and Git LFS use
``renku clone`` command.

.. code-block:: console

    $ renku clone git+ssh://host.io/namespace/project.git
        <destination-directory>

"""

import click

from renku.core.commands.clone import project_clone
from renku.core.commands.echo import GitProgress


@click.command()
@click.option("--pull-data", is_flag=True, help="Pull data from Git-LFS.", default=False)
@click.argument("url")
@click.argument("path", required=False, default=None)
def clone(pull_data, url, path):
    """Clone a Renku repository."""
    click.echo("Cloning {} ...".format(url))

    skip_smudge = not pull_data
    project_clone(url=url, path=path, skip_smudge=skip_smudge, progress=GitProgress())
    click.secho("OK", fg="green")
