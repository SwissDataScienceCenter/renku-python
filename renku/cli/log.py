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
"""

import click

from ._ascii import DAG
from ._client import pass_local_client
from ._echo import echo_via_pager
from ._graph import Graph, _safe_path


@click.command()
@click.option('--revision', default='HEAD')
@click.argument(
    'paths', type=click.Path(exists=True, dir_okay=False), nargs=-1
)
@pass_local_client
def log(client, revision, paths):
    """Show logs for a file."""
    graph = Graph(client)
    paths = paths or (
        path for path in client.git.rev_parse(revision).stats.files.keys()
        if _safe_path(path)
    )

    for path in paths:
        graph.add_file(graph.normalize_path(path), revision=revision)

    echo_via_pager(DAG(graph=graph))
