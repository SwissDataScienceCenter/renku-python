# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
r"""Manage an cloud storage.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.storage:storage
   :prog: renku storage
   :nested: full

"""
import os

import click

import renku.ui.cli.utils.color as color
from renku.command.format.storage import CLOUD_STORAGE_COLUMNS, CLOUD_STORAGE_FORMATS
from renku.command.util import WARNING
from renku.ui.cli.utils.callback import ClickCallback


@click.group()
def storage():
    """Manage storage."""


@storage.command()
@click.option(
    "--columns",
    type=click.STRING,
    default=None,
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(CLOUD_STORAGE_COLUMNS.keys())),
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(list(CLOUD_STORAGE_FORMATS.keys())), default="log", help="Choose an output format."
)
def ls(columns, format):
    """List configured cloud storage for a project."""
    from renku.command.storage import list_storage_command

    result = list_storage_command().build().execute()

    storages = [s.storage for s in result.output]

    click.echo(CLOUD_STORAGE_FORMATS[format](storages, columns=columns))


# =============================================
#  Deprecated LFS commands below, see lfs.py
# =============================================
@storage.command(hidden=True, deprecated=True)
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def pull(paths):
    """Pull the specified paths from external storage."""
    from renku.command.lfs import pull_command

    pull_command().build().execute(paths=paths)


@storage.command(hidden=True, deprecated=True)
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def clean(paths):
    """Remove files from lfs cache/turn them back into pointer files."""
    from renku.command.lfs import clean_command

    communicator = ClickCallback()
    clean_command().with_communicator(communicator).build().execute(paths=paths)

    click.secho("OK", fg=color.GREEN)


@storage.command("check-lfs-hook", hidden=True, deprecated=True)
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def check_lfs_hook(paths):
    """Check specified paths are tracked in external storage."""
    from renku.command.lfs import check_lfs_hook_command

    paths = check_lfs_hook_command().build().execute(paths=paths).output
    if paths:
        click.echo(os.linesep.join(paths))
        exit(1)


@storage.command(hidden=True, deprecated=True)
@click.option("--all", is_flag=True, help="Include all branches.")
def check(all):
    """Check if large files are committed to Git history."""
    from renku.command.lfs import check_lfs_command

    files = check_lfs_command().build().execute(everything=all).output
    if files:
        message = WARNING + "Git history contains large files\n\t" + "\n\t".join(files)
        click.echo(message)
        exit(1)
    else:
        click.secho("OK", fg=color.GREEN)


@storage.command(hidden=True, deprecated=True)
@click.option("--all", "-a", "migrate_all", is_flag=True, default=False, help="Migrate all large files not in git LFS.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
def migrate(migrate_all, paths):
    """Migrate large files committed to git by moving them to LFS."""
    from renku.command.lfs import check_lfs_command, fix_lfs_command

    if not paths:
        if not migrate_all:
            click.echo("Please specify paths to migrate or use the --all flag to migrate all large files.")
            exit(1)

        lfs_paths = check_lfs_command().build().execute(everything=migrate_all).output

        if not lfs_paths:
            click.echo("All files are already in LFS")
            exit(0)

        if not click.confirm("The following files will be moved to Git LFS:\n\t" + "\n\t".join(lfs_paths)):
            exit(0)

    fix_lfs_command().build().execute(paths)
