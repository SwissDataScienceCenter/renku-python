# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Get and set Renku repository or global options.

Set values
~~~~~~~~~~

You can set various Renku configuration options, for example the image registry
URL, with a command like:

.. code-block:: console

    $ renku config registry https://registry.gitlab.com/demo/demo

By default, configuration is stored locally in the project's directory. Use
``--global`` option to store configuration for all projects in your home
directory.

Query values
~~~~~~~~~~~~

You display a previously set value with:

.. code-block:: console

    $ renku config registry
    https://registry.gitlab.com/demo/demo

Both local and global configuration files are read and values in local
configuration take precedence over global values. Use ``--local`` or
``--global`` flags to read corresponding configuration only.
"""
import click

from renku.core.commands.config import update_config


@click.command()
@click.argument('key', required=True)
@click.argument('value', required=False, default=None)
@click.option(
    '--local',
    'local_only',
    is_flag=True,
    help='Read from local configuration only.'
)
@click.option(
    '--global',
    'global_only',
    is_flag=True,
    help='Read/store from/to global configuration only.'
)
def config(key, value, local_only, global_only):
    """Manage configuration options."""
    updated = update_config(key, value, local_only, global_only)
    click.secho(updated)
