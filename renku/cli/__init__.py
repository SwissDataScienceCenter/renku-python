# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
r"""The base command for interacting with the Renku platform.

``renku`` (base command)
------------------------

To list the available commands, either run ``renku`` with no parameters or
execute ``renku help``:

.. code-block:: console

    $ renku help
    Usage: renku [OPTIONS] COMMAND [ARGS]...

    Check common Renku commands used in various situations.

    Options:
      --version            Print version number.
      --config PATH        Location of client config files.
      --config-path        Print application config path.
      --path <path>        Location of a Renku repository.  [default: .]
      --renku-home <path>  Location of Renku directory.  [default: .renku]
      -h, --help           Show this message and exit.

    Commands:
      # [...]

Configuration files
~~~~~~~~~~~~~~~~~~~

Depending on your system, you may find the configuration files used by Renku
command line in a different folder. By default, the following rules are used:

MacOS:
  ``~/Library/Application Support/Renku``
Unix:
  ``~/.config/renku``
Windows:
  ``C:\Users\<user>\AppData\Roaming\Renku``

If in doubt where to look for the configuration file, you can display its path
by running ``renku --config-path``.

You can specify a different location via the ``RENKU_CONFIG`` environment
variable or the ``--config`` command line option. If both are specified, then
the ``--config`` option value is used. For example:

.. code-block:: console

    $ renku --config ~/renku/config/ init

instructs Renku to store the configuration files in your ``~/renku/config/``
directory when running the ``init`` command.
"""

import uuid

import click
import yaml
from click_plugins import with_plugins
from pkg_resources import iter_entry_points

from ._config import RENKU_HOME, default_config_dir, print_app_config_path
from ._version import print_version


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


@with_plugins(iter_entry_points('renku.cli'))
@click.group(
    context_settings={
        'auto_envvar_prefix': 'RENKU',
        'help_option_names': ['-h', '--help'],
    }
)
@click.option(
    '--version',
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help=print_version.__doc__
)
@click.option(
    '--config',
    envvar='RENKU_CONFIG',
    default=default_config_dir,
    type=click.Path(),
    expose_value=False,
    help='Location of client config files.'
)
@click.option(
    '--config-path',
    is_flag=True,
    callback=print_app_config_path,
    expose_value=False,
    is_eager=True,
    help=print_app_config_path.__doc__
)
@click.option(
    '--path',
    show_default=True,
    metavar='<path>',
    default='.',
    help='Location of a Renku repository.'
)
@click.option(
    '--renku-home',
    envvar='RENKU_HOME',
    show_default=True,
    metavar='<path>',
    default=RENKU_HOME,
    help='Location of Renku directory.'
)
@click.pass_context
def cli(ctx, path, renku_home):
    """Check common Renku commands used in various situations."""


@cli.command()
@click.pass_context
def help(ctx):
    """Show help message and exit."""
    click.echo(ctx.parent.get_help())
