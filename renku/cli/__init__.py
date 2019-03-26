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
      --version                       Print version number.
      --config PATH                   Location of client config files.
      --config-path                   Print application config path.
      --install-completion            Install completion for the current shell.
      --path <path>                   Location of a Renku repository.
                                      [default: (dynamic)]
      --renku-home <path>             Location of the Renku directory.
                                      [default: .renku]
      --external-storage / -S, --no-external-storage
                                      Use an external file storage service.
      -h, --help                      Show this message and exit.

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
import click_completion
import yaml

from ..api.client import LocalClient
from ..api.repository import default_path
from ._config import RENKU_HOME, default_config_dir, print_app_config_path
from ._exc import IssueFromTraceback
from ._options import install_completion, option_use_external_storage
from ._version import check_version, print_version
from .config import config
from .dataset import dataset
from .doctor import doctor
from .githooks import githooks
from .image import image
from .init import init
from .log import log
from .migrate import migrate
from .move import move
from .pull import pull
from .remove import remove
from .rerun import rerun
from .run import run
from .runner import runner
from .show import show
from .status import status
from .storage import storage
from .update import update
from .workflow import workflow
from .workon import deactivate, workon

#: Monkeypatch Click application.
click_completion.init()


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


@click.group(
    cls=IssueFromTraceback,
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
    '--install-completion',
    is_flag=True,
    callback=install_completion,
    expose_value=False,
    is_eager=True,
    help=install_completion.__doc__,
)
@click.option(
    '--path',
    show_default=True,
    metavar='<path>',
    default=default_path,
    help='Location of a Renku repository.'
)
@click.option(
    '--renku-home',
    envvar='RENKU_HOME',
    show_default=True,
    metavar='<path>',
    default=RENKU_HOME,
    help='Location of the Renku directory.'
)
@option_use_external_storage
@click.option(
    '--disable-version-check',
    envvar='RENKU_DISABLE_VERSION_CHECK',
    is_flag=True,
    default=False,
    callback=check_version,
    expose_value=False,
    help='Do not periodically check PyPI for a new version of renku.',
)
@click.pass_context
def cli(ctx, path, renku_home, use_external_storage):
    """Check common Renku commands used in various situations."""
    ctx.obj = LocalClient(
        path=path,
        renku_home=renku_home,
        use_external_storage=use_external_storage,
    )


@cli.command()
@click.pass_context
def help(ctx):
    """Show help message and exit."""
    click.echo(ctx.parent.get_help())


# Register subcommands:
cli.add_command(config)
cli.add_command(dataset)
cli.add_command(deactivate)
cli.add_command(doctor)
cli.add_command(githooks)
cli.add_command(image)
cli.add_command(init)
cli.add_command(log)
cli.add_command(migrate)
cli.add_command(move)
cli.add_command(pull)
cli.add_command(remove)
cli.add_command(rerun)
cli.add_command(run)
cli.add_command(runner)
cli.add_command(show)
cli.add_command(status)
cli.add_command(storage)
cli.add_command(update)
cli.add_command(workflow)
cli.add_command(workon)
