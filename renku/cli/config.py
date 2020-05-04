# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

Remove values
~~~~~~~~~~~~~

To remove a specific key from configuration use:

.. code-block:: console

    $ renku config --remove registry

By default, only local configuration is searched for removal. Use ``--global``
option to remove a global configuration value.

Query values
~~~~~~~~~~~~

You can display all configuration values with:

.. code-block:: console

    $ renku config

Both local and global configuration files are read. Values in local
configuration take precedence over global values. Use ``--local`` or
``--global`` flag to read corresponding configuration only.

You can provide a KEY to display only its value:

.. code-block:: console

    $ renku config registry
    https://registry.gitlab.com/demo/demo

Available configuration values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following values are available for the ``renku config`` command:

+------------------------+-------------------------------------+-----------+
| Name                   | Description                         | Default   |
+========================+=====================================+===========+
| registry               | The image registry to store Docker  | ``None``  |
|                        | images in                           |           |
+------------------------+-------------------------------------+-----------+
| zenodo.access_token    | Access token for Zenodo API         | ``None``  |
+------------------------+-------------------------------------+-----------+
| dataverse.access_token | Access token for Dataverse API      | ``None``  |
+------------------------+-------------------------------------+-----------+
| dataverse.server_url   | URL for the Dataverse API server    | ``None``  |
|                        | to use                              |           |
+------------------------+-------------------------------------+-----------+
| show_lfs_message       | Whether to show messages about      | ``True``  |
|                        | files being added to git LFS or not |           |
+------------------------+-------------------------------------+-----------+
| lfs_threshold          | Threshold file size below which     | ``100kb`` |
|                        | files are not added to git LFS      |           |
+------------------------+-------------------------------------+-----------+
"""
import click

from renku.core import errors
from renku.core.commands.config import read_config, update_config


@click.command()
@click.argument('key', required=False, default=None)
@click.argument('value', required=False, default=None)
@click.option('--remove', is_flag=True, help='Remove specified key.')
@click.option(
    '--local',
    'local_only',
    is_flag=True,
    help='Read/store from/to local configuration only.'
)
@click.option(
    '--global',
    'global_only',
    is_flag=True,
    help='Read/store from/to global configuration only.'
)
def config(key, value, remove, local_only, global_only):
    """Manage configuration options."""
    is_write = value is not None

    if is_write and remove:
        raise errors.UsageError('Cannot remove and set at the same time.')
    if remove and not key:
        raise errors.UsageError('KEY is missing.')
    if local_only and global_only:
        raise errors.UsageError('Cannot use --local and --global together.')

    if remove:
        update_config(key, remove=remove, global_only=global_only)
    elif is_write:
        update_config(key, value=value, global_only=global_only)
    else:
        value = read_config(key, local_only, global_only)
        click.secho(value)
