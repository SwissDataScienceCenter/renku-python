# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Install and uninstall Git hooks.

Prevent modifications of output files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The commit hooks are enabled by default to prevent situation when
some output file is manually modified.

.. code-block:: console

    $ renku init
    $ renku run echo hello > greeting.txt
    $ edit greeting.txt
    $ git commit greeting.txt
    You are trying to update some output files.

    Modified outputs:
      greeting.txt

    If you are sure, use "git commit --no-verify".

"""

import stat

import click

from renku._compat import Path

from ._client import pass_local_client

HOOKS = ('pre-commit', )


@click.group()
def githooks():
    """Manage Git hooks for Renku repository."""


@githooks.command()
@click.option('--force', is_flag=True, help='Override existing hooks.')
@pass_local_client
def install(client, force):
    """Install Git hooks."""
    import pkg_resources
    from git.index.fun import hook_path as get_hook_path

    for hook in HOOKS:
        hook_path = Path(get_hook_path(hook, client.repo.git_dir))
        if hook_path.exists():
            if not force:
                click.echo(
                    "Hook already exists. Skipping {0}".format(str(hook_path)),
                    err=True
                )
                continue
            else:
                hook_path.unlink()

        # Make sure the hooks directory exists.
        hook_path.parent.mkdir(parents=True, exist_ok=True)

        Path(hook_path).write_bytes(
            pkg_resources.resource_string(
                'renku.data', '{hook}.sh'.format(hook=hook)
            )
        )
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)


@githooks.command()
@pass_local_client
def uninstall(client):
    """Uninstall Git hooks."""
    from git.index.fun import hook_path as get_hook_path

    for hook in HOOKS:
        hook_path = Path(get_hook_path(hook, client.repo.git_dir))
        if hook_path.exists():
            hook_path.unlink()
