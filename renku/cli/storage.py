# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""
import os

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import WARNING


@click.group()
def storage():
    """Manage an external storage."""


@storage.command()
@click.argument(
    "paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True,
)
@pass_local_client
def pull(client, paths):
    """Pull the specified paths from external storage."""
    client.pull_paths_from_storage(*paths)


@storage.command()
@click.argument(
    "paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True,
)
@pass_local_client
def clean(client, paths):
    """Remove files from lfs cache/turn them back into pointer files."""
    untracked_paths, local_only_paths = client.clean_storage_cache(*paths)

    if untracked_paths:
        click.echo(
            WARNING
            + "These paths were ignored as they are not tracked"
            + " in git LFS:\n\t{}\n".format("\n\t".join(untracked_paths))
        )

    if local_only_paths:
        click.echo(
            WARNING
            + "These paths were ignored as they are not pushed to "
            + "a remote with git LFS:\n\t{}\n".format("\n\t".join(local_only_paths))
        )

    click.secho("OK", fg="green")


@storage.command("check-lfs-hook", hidden=True)
@click.argument(
    "paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True,
)
@pass_local_client
def check_lfs_hook(client, paths):
    """Check specified paths are tracked in external storage."""
    paths = client.check_requires_tracking(*paths)
    if paths:
        click.echo(os.linesep.join(paths))
        exit(1)


@storage.command()
@click.option("--all", is_flag=True, help="Include all branches.")
@pass_local_client
def check(client, all):
    """Check if large files are committed to Git history."""
    files = client.check_lfs_migrate_info(everything=all)
    if files:
        message = WARNING + "Git history contains large files\n\t" + "\n\t".join(files)
        click.echo(message)
        exit(1)
    else:
        click.secho("OK", fg="green")
