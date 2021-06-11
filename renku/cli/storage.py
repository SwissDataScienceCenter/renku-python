# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
r"""Manage an external storage.

Pulling files from git LFS
~~~~~~~~~~~~~~~~~~~~~~~~~~

LFS works by checking small pointer files into git and saving the actual
contents of a file in LFS. If instead of your file content, you see
something like this, it means the file is stored in git LFS and its
contents are not currently available locally (they are not pulled):

.. code-block:: console

    version https://git-lfs.github.com/spec/v1
    oid sha256:42b5c7fb2acd54f6d3cd930f18fee3bdcb20598764ca93bdfb38d7989c054bcf
    size 12

You can manually pull contents of file(s) you want with:

.. code-block:: console

    $ renku storage pull file1 file2

Removing local content of files stored in git LFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to restore a file back to its pointer file state, for instance
to free up space locally, you can run:

.. code-block:: console

    $ renku storage clean file1 file2

This removes any data cached locally for files tracked in in git LFS.

Migrate large files to git LFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you accidentally checked a large file into git or are moving a non-LFS
renku repo to git LFS, you can use the following command to migrate the files
to LFS:

.. code-block:: console

    $ renku storage migrate --all

This will move all files that are bigger than the renku `lfs_threshold` config
value and are not excluded by `.renkulfsignore` into git LFS.

To only migrate specific files, you can also pass their paths to the command
like:

.. code-block:: console

    $ renku storage migrate big_file other_big_file
"""
import os

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.echo import WARNING
from renku.core.commands.storage import (
    check_lfs_command,
    check_lfs_hook_command,
    clean_command,
    fix_lfs_command,
    pull_command,
)


@click.group()
def storage():
    """Manage an external storage."""


@storage.command()
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def pull(paths):
    """Pull the specified paths from external storage."""
    pull_command().build().execute(paths=paths)


@storage.command()
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def clean(paths):
    """Remove files from lfs cache/turn them back into pointer files."""
    communicator = ClickCallback()
    clean_command().with_communicator(communicator).build().execute(paths=paths)

    click.secho("OK", fg="green")


@storage.command("check-lfs-hook", hidden=True)
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True)
def check_lfs_hook(paths):
    """Check specified paths are tracked in external storage."""
    paths = check_lfs_hook_command().build().execute(paths=paths).output
    if paths:
        click.echo(os.linesep.join(paths))
        exit(1)


@storage.command()
@click.option("--all", is_flag=True, help="Include all branches.")
def check(all):
    """Check if large files are committed to Git history."""
    files = check_lfs_command().build().execute(everything=all).output
    if files:
        message = WARNING + "Git history contains large files\n\t" + "\n\t".join(files)
        click.echo(message)
        exit(1)
    else:
        click.secho("OK", fg="green")


@storage.command()
@click.option("--all", "-a", "migrate_all", is_flag=True, default=False, help="Migrate all large files not in git LFS.")
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
def migrate(migrate_all, paths):
    """Migrate large files committed to git by moving them to LFS."""
    if not paths:
        if not migrate_all:
            click.echo("Please specify paths to migrate or use the --all flag to migrate all large files.")
            exit(1)

        lfs_paths = check_lfs_command().build().execute(everything=all).output

        if not lfs_paths:
            click.echo("All files are already in LFS")
            exit(0)

        if not click.confirm("The following files will be moved to Git LFS:\n\t" + "\n\t".join(lfs_paths)):
            exit(0)

    fix_lfs_command().build().execute(paths)
