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
      --global-config-path            Print global application's config path.
      --install-completion            Install completion for the current shell.
      --path <path>                   Location of a Renku repository.
                                      [default: (dynamic)]
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
by running ``renku --global-config-path``.
"""
import os
import sys
import uuid
from pathlib import Path

import click
import click_completion
import yaml

from renku.cli.clone import clone
from renku.cli.config import config
from renku.cli.dataset import dataset
from renku.cli.doctor import doctor
from renku.cli.exception_handler import IssueFromTraceback
from renku.cli.githooks import githooks
from renku.cli.init import init
from renku.cli.log import log
from renku.cli.migrate import migrate
from renku.cli.move import move
from renku.cli.remove import remove
from renku.cli.rerun import rerun
from renku.cli.run import run
from renku.cli.show import show
from renku.cli.status import status
from renku.cli.storage import storage
from renku.cli.update import update
from renku.cli.workflow import workflow
from renku.core.commands.echo import WARNING
from renku.core.commands.options import install_completion, \
    option_external_storage_requested
from renku.core.commands.version import check_version, print_version
from renku.core.errors import UsageError
from renku.core.management.client import LocalClient
from renku.core.management.config import RENKU_HOME, ConfigManagerMixin
from renku.core.management.repository import default_path

#: Monkeypatch Click application.
click_completion.init()

WARNING_UNPROTECTED_COMMANDS = ['init', 'clone', 'help']


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


def print_global_config_path(ctx, param, value):
    """Print global application's config path."""
    if not value or ctx.resilient_parsing:
        return
    click.echo(ConfigManagerMixin().global_config_path)
    ctx.exit()


def is_allowed_command(ctx):
    """Check if invoked command contains help command."""
    return (
        ctx.invoked_subcommand in WARNING_UNPROTECTED_COMMANDS or
        '-h' in sys.argv or '--help' in sys.argv
    )


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
    '--global-config-path',
    is_flag=True,
    callback=print_global_config_path,
    expose_value=False,
    is_eager=True,
    help=print_global_config_path.__doc__
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
@option_external_storage_requested
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
def cli(ctx, path, external_storage_requested):
    """Check common Renku commands used in various situations."""
    renku_path = Path(path) / RENKU_HOME
    if not renku_path.exists() and not is_allowed_command(ctx):
        raise UsageError((
            '`{0}` is not a renku repository.\n'
            'To initialize this as a renku '
            'repository use: `renku init`'.format(path)
        ))

    ctx.obj = LocalClient(
        path=path,
        external_storage_requested=external_storage_requested,
    )

    if (
        path != os.getcwd() and
        ctx.invoked_subcommand not in WARNING_UNPROTECTED_COMMANDS
    ):
        click.secho(
            WARNING +
            'Run CLI commands only from project\'s root directory.\n',
            err=True
        )


@cli.command()
@click.pass_context
def help(ctx):
    """Show help message and exit."""
    click.echo(ctx.parent.get_help())


# Register subcommands:
cli.add_command(clone)
cli.add_command(config)
cli.add_command(dataset)
cli.add_command(doctor)
cli.add_command(githooks)
cli.add_command(init)
cli.add_command(log)
cli.add_command(migrate)
cli.add_command(move)
cli.add_command(remove)
cli.add_command(rerun)
cli.add_command(run)
cli.add_command(show)
cli.add_command(status)
cli.add_command(storage)
cli.add_command(update)
cli.add_command(workflow)
