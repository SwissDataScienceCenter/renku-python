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
r"""The base command for interacting with the Renku platform.

renku (base command)
********************

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

.. cheatsheet::
   :group: Typical Workflow
   :command: $ git status
   :description: Take a look at what you have done since the last save.
   :target: ui,rp

.. cheatsheet::
   :group: Typical Workflow
   :command: $ renku save -m <msg>
   :description: Save your latest work, providing a message explaining what you have done.
   :target: ui,rp

.. cheatsheet::
   :group: Typical Workflow
   :command: $ renku run …
   :description: Run your code, capturing lineage of the inputs and outputs using Renku.
   :target: ui,rp

"""
import os
import sys
import uuid
from pathlib import Path

import click
import yaml
from click_plugins import with_plugins

from renku.command.options import option_external_storage_requested
from renku.command.util import WARNING
from renku.command.version import check_version, print_version
from renku.core import errors
from renku.core.constant import DATABASE_PATH
from renku.core.util.git import get_git_path
from renku.domain_model.project_context import project_context
from renku.ui.cli.clone import clone
from renku.ui.cli.config import config
from renku.ui.cli.dataset import dataset
from renku.ui.cli.doctor import doctor
from renku.ui.cli.env import env
from renku.ui.cli.exception_handler import IssueFromTraceback
from renku.ui.cli.gc import gc
from renku.ui.cli.githooks import githooks as githooks_command
from renku.ui.cli.graph import graph
from renku.ui.cli.init import init
from renku.ui.cli.log import log
from renku.ui.cli.login import credentials, login, logout
from renku.ui.cli.mergetool import mergetool
from renku.ui.cli.migrate import check_immutable_template_files, migrate, migrationscheck
from renku.ui.cli.move import move
from renku.ui.cli.project import project
from renku.ui.cli.remove import remove
from renku.ui.cli.rerun import rerun
from renku.ui.cli.rollback import rollback
from renku.ui.cli.run import run
from renku.ui.cli.save import save
from renku.ui.cli.service import service
from renku.ui.cli.session import session
from renku.ui.cli.status import status
from renku.ui.cli.storage import storage
from renku.ui.cli.template import template
from renku.ui.cli.update import update
from renku.ui.cli.workflow import workflow

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points  # type: ignore


def get_entry_points(name: str):
    """Get entry points from importlib."""
    all_entry_points = entry_points()

    if hasattr(all_entry_points, "select"):
        return all_entry_points.select(group=name)  # type: ignore
    else:
        # Prior to Python 3.10, this returns a dict instead of the selection interface, which is slightly slower
        return all_entry_points.get(name, [])


WARNING_UNPROTECTED_COMMANDS = ["clone", "credentials", "env", "help", "init", "login", "logout", "service", "template"]

WARNING_UNPROTECTED_SUBCOMMANDS = {"template": ["ls", "show", "validate"]}


def _uuid_representer(dumper, data):
    """Add UUID serializer for YAML."""
    return dumper.represent_str(str(data))


def _is_renku_project(path: Path) -> bool:
    """Check if a path is a renku project."""
    from renku.core.constant import RENKU_HOME
    from renku.core.migration.utils import OLD_METADATA_PATH
    from renku.infrastructure.database import Database

    metadata_path = Path(path) / RENKU_HOME
    old_metadata = metadata_path / OLD_METADATA_PATH
    new_metadata = metadata_path / DATABASE_PATH / Database.ROOT_OID

    return old_metadata.exists() or new_metadata.exists()


yaml.add_representer(uuid.UUID, _uuid_representer)


def print_global_config_path(ctx, _, value):
    """Print global application's config path."""
    if not value or ctx.resilient_parsing:
        return

    click.echo(project_context.global_config_path)
    ctx.exit()


def is_allowed_subcommand(ctx):
    """Called from subcommands to check if their sub-subcommand is allowed.

    Subcommands where some sub-subcommands are allowed should be added to ``WARNING_UNPROTECTED_COMMANDS`` so they pass
    through the parent check and then added to ``WARNING_UNPROTECTED_SUBCOMMANDS`` so they get checked here.
    """
    from renku.domain_model.project_context import project_context

    if not _is_renku_project(project_context.path) and (
        not WARNING_UNPROTECTED_SUBCOMMANDS.get(ctx.command.name, False)
        or ctx.invoked_subcommand not in WARNING_UNPROTECTED_SUBCOMMANDS[ctx.command.name]
    ):
        raise errors.UsageError(
            f"{project_context.path} is not a renku repository.\n"
            "To initialize this as a renku repository use: 'renku init'"
        )


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
    "--path", show_default=True, metavar="<path>", default=get_git_path, help="Location of a Renku repository."
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
    from renku.domain_model.project_context import project_context

    path = Path(path)

    is_command_allowed = is_allowed_command(ctx)
    is_renku_project = _is_renku_project(path)

    if not is_renku_project and not is_command_allowed:
        raise errors.UsageError(
            f"{path} is not a renku repository.\n" "To initialize this as a renku repository use: 'renku init'"
        )

    project_context.push_path(path)
    project_context.external_storage_requested = external_storage_requested

    if is_renku_project and path != Path(os.getcwd()) and not is_command_allowed:
        click.secho(WARNING + "Run CLI commands only from project's root directory.\n", err=True)


@cli.command()
@click.pass_context
def help(ctx):
    """Show help message and exit."""
    click.echo(ctx.parent.get_help())


# Register subcommands:
cli.add_command(check_immutable_template_files)
cli.add_command(clone)
cli.add_command(config)
cli.add_command(credentials)
cli.add_command(dataset)
cli.add_command(doctor)
cli.add_command(env)
cli.add_command(gc)
cli.add_command(githooks_command)
cli.add_command(graph)
cli.add_command(init)
cli.add_command(log)
cli.add_command(login)
cli.add_command(logout)
cli.add_command(mergetool)
cli.add_command(migrate)
cli.add_command(migrationscheck)
cli.add_command(move)
cli.add_command(project)
cli.add_command(remove)
cli.add_command(rerun)
cli.add_command(rollback)
cli.add_command(run)
cli.add_command(save)
cli.add_command(service)
cli.add_command(session)
cli.add_command(status)
cli.add_command(storage)
cli.add_command(template)
cli.add_command(update)
cli.add_command(workflow)
