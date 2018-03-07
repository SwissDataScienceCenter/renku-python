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
"""Create an empty Renga project or reinitialize an existing one.

Starting a Renga project
~~~~~~~~~~~~~~~~~~~~~~~~

If you have an existing directory which you want to turn into a Renga project,
you can type:

.. code-block:: console

    $ cd ~/my_project
    $ renga init

or:

.. code-block:: console

    $ renga init ~/my_project

This creates a new subdirectory named ``.renga`` that contains all the
necessary files for managing the project configuration.
"""

import os

import click

from ._client import pass_local_client
from ._git import set_git_home, with_git


def validate_name(ctx, param, value):
    """Validate a project name."""
    if not value:
        value = os.path.basename(ctx.params['directory'].rstrip(os.path.sep))
    return value


def store_directory(ctx, param, value):
    """Store directory as a new Git home."""
    set_git_home(value)
    return value


@click.command()
@click.argument(
    'directory',
    default='.',
    callback=store_directory,
    type=click.Path(
        exists=True, writable=True, file_okay=False, resolve_path=True
    )
)
@click.option('--name', callback=validate_name)
@click.option('--force', is_flag=True)
@click.option(
    'use_external_storage',
    '--external-storage/--no-external-storage',
    ' /-S',
    is_flag=True,
    default=True,
    help='Configure the file storage service.'
)
@pass_local_client
@click.pass_context
@with_git(clean=False)
def init(ctx, repo, directory, name, force, use_external_storage):
    """Initialize a project."""
    try:
        project_config_path = repo.init_repository(
            name=name, force=force, use_external_storage=use_external_storage
        )
    except FileExistsError:
        raise click.UsageError(
            'Renga repository is not empty. '
            'Please use --force flag to use the directory as Renga repository.'
        )

    from .runner import template
    ctx.invoke(template)

    click.echo('Initialized empty project in {0}'.format(project_config_path))
