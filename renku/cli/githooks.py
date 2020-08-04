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
"""Install and uninstall Git hooks.

Prevent modifications of output files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The commit hooks are enabled by default to prevent situation when
some output file is manually modified.

.. code-block:: console

    $ renku init
    $ renku run echo hello > greeting.txt
    $ edit greeting.txt
    $ git commit greeting.txt
    You are trying to update some output files.

    Modified outputs:
      greeting.txt

    If you are sure, use "git commit --no-verify".

"""

import click

from renku.core.commands.githooks import install_githooks, uninstall_githooks


@click.group()
def githooks():
    """Manage Git hooks for Renku repository."""


@githooks.command()
@click.option("--force", is_flag=True, help="Override existing hooks.")
def install(force):
    """Install Git hooks."""
    install_githooks(force)
    click.secho("OK", fg="green")


@githooks.command()
def uninstall():
    """Uninstall Git hooks."""
    uninstall_githooks()
    click.secho("OK", fg="green")
