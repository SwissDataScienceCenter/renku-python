# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
import yaml
from click_plugins import with_plugins

from renku.cli.clone import clone
from renku.cli.config import config
from renku.cli.dataset import dataset
from renku.cli.doctor import doctor
from renku.cli.exception_handler import IssueFromTraceback
from renku.cli.githooks import githooks as githooks_command
from renku.cli.graph import graph
from renku.cli.init import init as init_command
from renku.cli.log import log
from renku.cli.login import credentials, login, logout
from renku.cli.migrate import check_immutable_template_files, migrate, migrationscheck
from renku.cli.move import move
from renku.cli.project import project
from renku.cli.remove import remove
from renku.cli.rerun import rerun
from renku.cli.rollback import rollback
from renku.cli.run import run
from renku.cli.save import save
from renku.cli.service import service
from renku.cli.status import status
from renku.cli.storage import storage
from renku.cli.update import update
from renku.cli.workflow import workflow
from renku.core.commands.echo import WARNING
from renku.core.commands.options import option_external_storage_requested
from renku.core.commands.version import check_version, print_version
from renku.core.errors import UsageError
from renku.core.utils.git import default_path

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points


def get_entry_points(name: str):
    """Get entry points from importlib."""
    all_entry_points = entry_points()

    if hasattr(all_entry_points, "select"):
        return all_entry_points.select(group=name)
    else:
        # Prior to Python 3.10, this returns a dict instead of the selection interface, which is slightly slower
        return all_entry_points.get(name, [])


WARNING_UNPROTECTED_COMMANDS = ["clone", "init", "help", "login", "logout", "service", "credentials"]


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


yaml.add_representer(uuid.UUID, _uuid_representer)


def print_global_config_path(ctx, param, value):
    """Print global application's config path."""
    from renku.core.management.config import ConfigManagerMixin

    if not value or ctx.resilient_parsing:
        return
    click.echo(ConfigManagerMixin().global_config_path)
    ctx.exit()


def is_allowed_command(ctx):
    """Check if invoked command contains help command."""
    return ctx.invoked_subcommand in WARNING_UNPROTECTED_COMMANDS or "-h" in sys.argv or "--help" in sys.argv


@with_plugins(get_entry_points("renku.cli_plugins"))
@click.group(
    cls=IssueFromTraceback, context_settings={"auto_envvar_prefix": "RENKU", "help_option_names": ["-h", "--help"]}
)
@click.option(
    "--version", is_flag=True, callback=print_version, expose_value=False, is_eager=True, help=print_version.__doc__
)
@click.option(
    "--global-config-path",
    is_flag=True,
    callback=print_global_config_path,
    expose_value=False,
    is_eager=True,
    help=print_global_config_path.__doc__,
)
@click.option(
    "--path", show_default=True, metavar="<path>", default=default_path, help="Location of a Renku repository."
)
@option_external_storage_requested
@click.option(
    "--disable-version-check",
    envvar="RENKU_DISABLE_VERSION_CHECK",
    is_flag=True,
    default=False,
    callback=check_version,
    expose_value=False,
    help="Do not periodically check PyPI for a new version of renku.",
)
@click.pass_context
def cli(ctx, path, external_storage_requested):
    """Check common Renku commands used in various situations."""
    from renku.core.management import RENKU_HOME
    from renku.core.management.client import LocalClient
    from renku.core.management.migrations.utils import OLD_METADATA_PATH
    from renku.core.management.repository import RepositoryApiMixin
    from renku.core.metadata.database import Database

    renku_path = Path(path) / RENKU_HOME
    old_metadata = renku_path / OLD_METADATA_PATH
    new_metadata = renku_path / RepositoryApiMixin.DATABASE_PATH / Database.ROOT_OID

    if not old_metadata.exists() and not new_metadata.exists() and not is_allowed_command(ctx):
        raise UsageError(
            (
                "`{0}` is not a renku repository.\n"
                "To initialize this as a renku "
                "repository use: `renku init`".format(path)
            )
        )

    ctx.obj = LocalClient(path=path, external_storage_requested=external_storage_requested)

    if path != os.getcwd() and ctx.invoked_subcommand not in WARNING_UNPROTECTED_COMMANDS:
        click.secho(WARNING + "Run CLI commands only from project's root directory.\n", err=True)


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
cli.add_command(githooks_command)
cli.add_command(graph)
cli.add_command(init_command)
cli.add_command(log)
cli.add_command(login)
cli.add_command(logout)
cli.add_command(migrate)
cli.add_command(migrationscheck)
cli.add_command(check_immutable_template_files)
cli.add_command(move)
cli.add_command(project)
cli.add_command(remove)
cli.add_command(rerun)
cli.add_command(rollback)
cli.add_command(run)
cli.add_command(save)
cli.add_command(status)
cli.add_command(storage)
cli.add_command(credentials)
cli.add_command(update)
cli.add_command(workflow)
cli.add_command(service)
