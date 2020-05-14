# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Commit changes and push them to a remote.

This command combines git add, git commit and git push, with some extra
functionality.
"""

import datetime

import click

from renku.core.commands.save import save_and_push


@click.command(name='save')
@click.option(
    '-m',
    '--message',
    default=datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S'),
    help='The commit message to use'
)
@click.option(
    '-d', '--destination', default=None, help='The git remote to push to'
)
@click.argument('paths', type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.pass_context
def save(ctx, message, destination, paths):
    """Save and push local changes."""

    saved_paths = save_and_push(message, remote=destination, paths=paths)

    if saved_paths:
        click.echo(
            'Successfully saved: \n\t{}'.format('\n\t'.join(saved_paths))
        )
    else:
        click.echo('There were no changes to save.')

    click.secho('OK', fg='green')
