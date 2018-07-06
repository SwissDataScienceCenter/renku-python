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
r"""Recreate files created by the "run" command.

Recreating files
~~~~~~~~~~~~~~~~

Assume you have run a step 2 that uses a stochastic algorithm, so each run
will be slightly different. The goal is to regenerate output ``C`` several
times to compare the output. In this situation it is not possible to simply
call :ref:`cli-update` since the input file ``A`` has not been modified
after the execution of step 2.

.. code-block:: text

    A-(step 1)-B-(step 2*)-C

Recreate a specific output file by running:

  .. code-block:: console

     $ renku rerun C

"""

import uuid

import click

from renku.models.cwl._ascwl import ascwl

from ._client import pass_local_client
from ._git import with_git
from ._graph import Graph
from ._options import option_siblings


@click.command()
@click.option('--revision', default='HEAD')
@click.option(
    '--from',
    'roots',
    type=click.Path(exists=True, dir_okay=False),
    multiple=True,
    help='Start an execution from this file.',
)
@option_siblings
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=False),
    nargs=-1,
    required=True,
)
@pass_local_client
@click.pass_context
@with_git()
def rerun(ctx, client, revision, roots, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    graph = Graph(client)
    outputs = {
        graph.add_file(graph.normalize_path(path), revision=revision)
        for path in paths
    }

    # Check or extend siblings of outputs.
    outputs = siblings(graph, outputs)
    output_paths = {path for _, path in outputs}

    # Get parents of all new roots
    import networkx as nx

    roots = {graph.normalize_path(root) for root in roots}
    assert not roots & output_paths, "--from colides with output paths"

    clean_nodes = {(c, p) for (c, p) in graph.G if p in roots}
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
