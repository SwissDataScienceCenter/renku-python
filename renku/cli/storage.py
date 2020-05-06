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
"""Manage an external storage."""

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import WARNING


@click.group()
def storage():
    """Manage an external storage."""


@storage.command()
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=True),
    nargs=-1,
    required=True,
)
@pass_local_client
def pull(client, paths):
    """Pull the specified paths from external storage."""
    client.pull_paths_from_storage(*paths)


@storage.command()
@click.option('--all', is_flag=True, help='Include all branches.')
@pass_local_client
def check(client, all):
    """Check if large files are committed to Git history."""
    files = client.check_lfs_migrate_info(everything=all)
    if files:
        message = (
            WARNING + 'Git history contains large files\n\t' +
            '\n\t'.join(files)
        )
        click.echo(message)
        exit(1)
    else:
        click.secho('OK', fg='green')
