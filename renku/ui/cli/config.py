# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

Description
~~~~~~~~~~~

Save useful Renku configuration directives in a simple human-readable ini file.
The values are used by other renku commands and by interactive sessions.

Check the `Available configuration values`_ section to get more information
on the available values.


Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.config:config
   :prog: renku config
   :nested: full

Examples
~~~~~~~~

.. cheatsheet::
   :group: Config
   :command: $ renku config show [<key>]
   :description: Show current configuration.
   :target: rp

You can set various Renku configuration options, for example the image registry
URL, with a command like:

.. code-block:: console

    $ renku config set interactive.default_url "/tree"

By default, configuration is stored locally in the project's directory. Use
``--global`` option to store configuration for all projects in your home
directory.

.. cheatsheet::
   :group: Config
   :command: $ renku config set <key> <value>
   :description: Set entry <key> to <value> in renku config.
   :target: rp

To remove a specific key from configuration use:

.. code-block:: console

    $ renku config remove interactive.default_url

By default, only local configuration is searched for removal. Use ``--global``
option to remove a global configuration value.

.. cheatsheet::
   :group: Config
   :command: $ renku config remove <key>
   :description: Unset entry <key> renku config.
   :target: rp

Available configuration values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following values are used by renku-python and available for
the ``renku config`` command:

+--------------------------------+-------------------------------------+-----------+
| Name                           | Description                         | Default   |
+================================+=====================================+===========+
| ``show_lfs_message``           | Whether to show messages about      | ``True``  |
|                                | files being added to git LFS or not |           |
+--------------------------------+-------------------------------------+-----------+
| ``lfs_threshold``              | Threshold file size below which     | ``100kb`` |
|                                | files are not added to git LFS      |           |
+--------------------------------+-------------------------------------+-----------+
| ``data_directory``             | Path to the data directory          | ``data/`` |
|                                | (read-only after project creation)  |           |
+--------------------------------+-------------------------------------+-----------+
| ``zenodo.access_token``        | Access token for Zenodo API         | ``None``  |
+--------------------------------+-------------------------------------+-----------+
| ``dataverse.access_token``     | Access token for Dataverse API      | ``None``  |
+--------------------------------+-------------------------------------+-----------+
| ``dataverse.server_url``       | URL for the Dataverse API server    | ``None``  |
|                                | to use                              |           |
+--------------------------------+-------------------------------------+-----------+

See the section on `renku.ini <https://renku.readthedocs.io/en/latest/reference/templates.html#renku>`_
for more configuration values.


"""
import click

import renku.ui.cli.utils.color as color
from renku.ui.cli.utils.click import MutuallyExclusiveOption


@click.group()
def config():
    """Interact with renku configuration."""
    pass


@config.command()
@click.argument("key", required=False, default=None)
@click.option(
    "--local",
    "local_only",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="Read from local configuration only.",
    mutually_exclusive=[("global_only", "--global"), ("default_only", "--default")],
)
@click.option(
    "--global",
    "global_only",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="Read from global configuration only.",
    mutually_exclusive=[("local_only", "--local"), ("default_only", "--default")],
)
@click.option(
    "--default",
    "default_only",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="Show default values if applicable.",
    mutually_exclusive=[("local_only", "--local"), ("global_only", "--global")],
)
def show(key, local_only, global_only, default_only):
    """Show current configuration.

    KEY is of the form <group>.<entry>, e.g. 'interactive.default_url'.
    """
    from renku.command.config import read_config
    from renku.domain_model.enums import ConfigFilter

    config_filter = ConfigFilter.ALL

    if local_only:
        config_filter = ConfigFilter.LOCAL_ONLY
    elif global_only:
        config_filter = ConfigFilter.GLOBAL_ONLY
    elif default_only:
        config_filter = ConfigFilter.DEFAULT_ONLY

    value = read_config().build().execute(key, config_filter=config_filter)
    click.secho(value.output)


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.option("--global", "global_only", is_flag=True, help="Store to global configuration only.")
def set_(key, value, global_only):
    """Set a configuration value.

    KEY is of the form <group>.<entry>, e.g. 'interactive.default_url'.
    """
    from renku.command.config import update_config

    update_config().build().execute(key, value=value, global_only=global_only)
    click.secho("OK", fg=color.GREEN)


@config.command()
@click.argument("key")
@click.option("--global", "global_only", is_flag=True, help="Remove from global configuration only.")
def remove(key, global_only):
    """Remove a configuration value.

    KEY is of the form <group>.<entry>, e.g. 'interactive.default_url'.
    """
    from renku.command.config import update_config

    update_config().build().execute(key, remove=True, global_only=global_only)
    click.secho("OK", fg=color.GREEN)
