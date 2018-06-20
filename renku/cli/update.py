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
r"""Update outdated files created by the "run" command.

Recreating outdated files
~~~~~~~~~~~~~~~~~~~~~~~~~

The information about dependencies for each file in the repository is generated
from information stored in the underlying Git repository.

A minimal dependency graph is generated for each outdated file stored in the
repository. It means that only the necessary steps will be executed and the
workflow used to orchestrate these steps is stored in the repository.

Assume that the following history for the file ``H`` exists.

.. code-block:: text

          C---D---E
         /         \
    A---B---F---G---H

The first example shows situation when ``D`` is modified and files ``E`` and
``H`` become outdated.

.. code-block:: text

          C--*D*--(E)
         /          \
    A---B---F---G---(H)

    ** - modified
    () - needs update

In this situation, you can do efectively two things:

* Recreate a single file by running

  .. code-block:: console

     $ renku update E

* Update all files by simply running

  .. code-block:: console

     $ renku update

.. note:: If there were uncommitted changes then the command fails.
   Check :program:`git status` to see details.

Pre-update checks
~~~~~~~~~~~~~~~~~

In the next example, files ``A`` or ``B`` are modified, hence the majority
of dependent files must be recreated.

.. code-block:: text

            (C)--(D)--(E)
           /            \
    *A*--*B*--(F)--(G)--(H)

To avoid excesive recreation of the large portion of files which could have
been affected by a simple change of an input file, consider speficing a single
file (e.g. ``renku update G``). See also :ref:`cli-status`.
"""

import sys
import uuid

import click

from renku.models.cwl._ascwl import ascwl

from ._client import pass_local_client
from ._git import with_git
from ._graph import Graph
from ._options import option_siblings


@click.command()
@click.option('--revision', default='HEAD')
@option_siblings
@click.argument(
    'paths', type=click.Path(exists=True, dir_okay=False), nargs=-1
)
@pass_local_client
@click.pass_context
@with_git()
def update(ctx, client, revision, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    graph = Graph(client)
    status = graph.build_status(revision=revision)
    paths = {graph.normalize_path(path) for path in paths} \
        if paths else status['outdated'].keys()
    outputs = {graph.add_file(path, revision=revision) for path in paths}

    if not outputs:
        click.secho(
            'All files were generated from the latest inputs.', fg='green'
        )
        sys.exit(0)

    # Check or extend siblings of outputs.
    outputs = siblings(graph, outputs)
    output_paths = {path for _, path in outputs}

    # Get parents of all clean nodes
    import networkx as nx

    clean_paths = set(status['up-to-date'].keys()) - output_paths
    clean_nodes = {(c, p) for (c, p) in graph.G if p in clean_paths}
    clean_parents = set()
    for key in clean_nodes:
        clean_parents |= nx.ancestors(graph.G, key)

    subnodes = set()
    for key in outputs:
        if key in graph.G:
            subnodes |= nx.shortest_path_length(graph.G, target=key).keys()

    graph.G.remove_nodes_from(clean_parents)
    graph.G.remove_nodes_from([n for n in graph.G if n not in subnodes])

    # Store the generated workflow used for updating paths.
    import yaml

    output_file = client.workflow_path / '{0}.cwl'.format(uuid.uuid4().hex)
    with output_file.open('w') as f:
        f.write(
            yaml.dump(
                ascwl(
                    graph.ascwl(global_step_outputs=True),
                    filter=lambda _, x: x is not None and x != [],
                    basedir=client.workflow_path,
                ),
                default_flow_style=False
            )
        )

    from ._cwl import execute
    execute(client, output_file, output_paths=output_paths)
