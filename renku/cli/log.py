# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Show provenance of data created by executing programs.

File provenance
~~~~~~~~~~~~~~~

Unlike the traditional file history format, which shows previous revisions
of the file, this format presents tool inputs together with their revision
identifiers.

A ``*`` character shows to which lineage the specific file belongs to.
A ``@`` character in the graph lineage means that the corresponding file does
not have any inputs and the history starts there.

When called without file names, ``renku log`` shows the history of most
recently created files. With the ``--revision <refname>`` option the output is
shown as it was in the specified revision.

Provenance examples
```````````````````

``renku log B``
   Show the history of file ``B`` since its last creation or modification.

``renku log --revision HEAD~5``
   Show the history of files that have been created or modified 5 commits ago.

``renku log --revision e3f0bd5a D E``
   Show the history of files ``D`` and ``E`` as it looked in the commit
   ``e3f0bd5a``.

Output formats
~~~~~~~~~~~~~~

Following formats supported when specified with ``--format`` option:

* `ascii`
* `dot`

You can generate a PNG of the full history of all files in the repository
using the :program:`dot` program.

.. code-block:: console

   $ FILES=$(git ls-files --no-empty-directory --recurse-submodules)
   $ renku log --format dot $FILES | dot -Tpng > /tmp/graph.png
   $ open /tmp/graph.png

"""

import click
from git import NULL_TREE

from ._client import pass_local_client
from ._format.graph import FORMATS
from ._graph import Graph


@click.command()
@click.option('--revision', default='HEAD')
@click.option(
    '--format',
    type=click.Choice(FORMATS),
    default='ascii',
    help='Choose an output format.'
)
@click.option(
    '--no-output',
    is_flag=True,
    default=False,
    help='Display commands without output files.'
)
@click.argument('paths', type=click.Path(exists=True), nargs=-1)
@pass_local_client
def log(client, revision, format, no_output, paths):
    """Show logs for a file."""
    graph = Graph(client)
    if not paths:
        start, is_range, stop = revision.partition('..')
        if not is_range:
            stop = start
        elif not stop:
            stop = 'HEAD'

        commit = client.repo.rev_parse(stop)
        paths = (
            str(client.path / item.a_path)
            for item in commit.diff(commit.parents or NULL_TREE)
            # if not item.deleted_file
        )

    # NOTE shall we warn when "not no_output and not paths"?
    graph.build(paths=paths, revision=revision, can_be_cwl=no_output)

    FORMATS[format](graph)
