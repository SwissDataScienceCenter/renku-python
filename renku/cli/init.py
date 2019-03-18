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
r"""Create an empty Renku project or reinitialize an existing one.

Starting a Renku project
~~~~~~~~~~~~~~~~~~~~~~~~

If you have an existing directory which you want to turn into a Renku project,
you can type:

.. code-block:: console

    $ cd ~/my_project
    $ renku init

or:

.. code-block:: console

    $ renku init ~/my_project

This creates a new subdirectory named ``.renku`` that contains all the
necessary files for managing the project configuration.

If provided directory does not exist, it will be created.

Updating an existing project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are situations when the required structure of a Renku project needs
to be recreated or you have an **existing** Git repository. You can solve
these situation by simply adding the ``--force`` option.

.. code-block:: console

    $ git init .
    $ echo "# Example\nThis is a README." > README.md
    $ git add README.md
    $ git commit -m 'Example readme file'
    # renku init would fail because there is a git repository
    $ renku init --force

You can also enable the external storage system for output files, if it
was not installed previously.

.. code-block:: console

    $ renku init --force --external-storage

"""

import contextlib
import os

import attr
import click

from renku._compat import Path

from ._client import pass_local_client
from ._git import set_git_home
from ._options import option_use_external_storage


def validate_name(ctx, param, value):
    """Validate a project name."""
    if not value:
        value = os.path.basename(ctx.params['directory'].rstrip(os.path.sep))
    return value


def store_directory(ctx, param, value):
    """Store directory as a new Git home."""
    Path(value).mkdir(parents=True, exist_ok=True)
    set_git_home(value)
    return value


@click.command()
@click.argument(
    'directory',
    default='.',
    callback=store_directory,
    type=click.Path(writable=True, file_okay=False, resolve_path=True)
)
@click.option('--name', callback=validate_name)
@click.option('--force', is_flag=True, help='Override project templates.')
@option_use_external_storage
@pass_local_client
@click.pass_context
def init(ctx, client, directory, name, force, use_external_storage):
    """Initialize a project."""
    if not client.use_external_storage:
        use_external_storage = False

    ctx.obj = client = attr.evolve(
        client,
        path=directory,
        use_external_storage=use_external_storage,
    )

    msg = 'Initialized empty project in {path}'
    branch_name = None

    stack = contextlib.ExitStack()

    if force and client.repo:
        msg = 'Initialized project in {path} (branch {branch_name})'
        merge_args = ['--no-ff', '-s', 'recursive', '-X', 'ours']

        try:
            commit = client.find_previous_commit(
                str(client.renku_metadata_path),
            )
            branch_name = 'renku/init/' + str(commit)
        except KeyError:
            from git import NULL_TREE
            commit = NULL_TREE
            branch_name = 'renku/init/root'
            merge_args.append('--allow-unrelated-histories')

        ctx.obj = client = stack.enter_context(
            client.worktree(
                branch_name=branch_name,
                commit=commit,
                merge_args=merge_args,
            )
        )

    try:
        path = client.init_repository(name=name, force=force)
    except FileExistsError:
        raise click.UsageError(
            'Renku repository is not empty. '
            'Please use --force flag to use the directory as Renku '
            'repository.'
        )

    stack.enter_context(client.commit())

    with stack:
        # Install Git hooks.
        from .githooks import install
        ctx.invoke(install, force=force)

        # Create all necessary template files.
        from .runner import template
        ctx.invoke(template, force=force)

    click.echo(msg.format(path=path, branch_name=branch_name))
