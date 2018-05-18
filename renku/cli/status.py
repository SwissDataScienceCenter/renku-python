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
"""Show status of data created in Renku repository."""

import click

from ._ascii import _format_sha1
from ._client import pass_local_client
from ._git import with_git
from ._graph import Graph


@click.command()
@click.option('--revision', default='HEAD')
@click.argument('path', type=click.Path(exists=True, dir_okay=False), nargs=-1)
@pass_local_client
@click.pass_context
@with_git(commit=False)
def status(ctx, client, revision, path):
    """Show a status of the repository."""
    paths = set(path)
    graph = Graph(client)
    status = graph.build_status(revision=revision)

    click.echo('On branch {0}'.format(client.git.active_branch))
    if status['outdated']:
        click.echo('Files generated from outdated inputs:')
        click.echo('  (use "renku log <file>..." to see the full lineage)')
        # click.echo('  (use "renku update <file>..." to '
        #            'generate file from latest inputs)')
        click.echo()

        for filepath, files in status['outdated'].items():
            paths = (
                ', '.join(
                    '{0}#{1}'.format(
                        click.style(p, fg='blue', bold=True),
                        _format_sha1(graph, (c, p)),
                    ) for c, p in stts
                    if not p.startswith('.renku/workflow/') and
                    p not in status['outdated']
                ) for stts in files
            )

            click.echo(
                '\t{0}: {1}'.format(
                    click.style(filepath, fg='red', bold=True),
                    ', '.join(paths)
                )
            )

        click.echo()

    else:
        click.secho(
            'All files were generated from the latest inputs.', fg='green'
        )

    if status['multiple-versions']:
        click.echo('Input files used in different versions:')
        click.echo(
            '  (use "renku log --revision <sha1> <file>" to see a lineage '
            'for the given revision)'
        )
        click.echo()

        for filepath, files in status['multiple-versions'].items():
            commits = (_format_sha1(graph, key) for key in files)
            click.echo(
                '\t{0}: {1}'.format(
                    click.style(filepath, fg='blue', bold=True),
                    ', '.join(commits)
                )
            )

        click.echo()

    ctx.exit(1 if status['outdated'] else 0)
