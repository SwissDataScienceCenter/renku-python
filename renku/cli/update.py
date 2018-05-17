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
"""Update an existing file."""

import uuid
from subprocess import call

import click
import networkx as nx
import yaml

from renku.models.cwl._ascwl import ascwl

from ._client import pass_local_client
from ._git import with_git
from ._graph import Graph


@click.command()
@click.option('--revision', default='HEAD')
@click.argument(
    'paths', type=click.Path(exists=True, dir_okay=False), nargs=-1
)
@pass_local_client
@click.pass_context
@with_git()
def update(ctx, client, revision, paths):
    """Update existing files by rerunning their outdated workflow."""
    graph = Graph(client)

    status = graph.build_status(revision=revision)

    if not paths:
        outputs = {
            graph.add_file(path, revision=revision)
            for path in status['outdated']
        }
    else:
        outputs = {graph.add_file(path, revision=revision) for path in paths}

    # Get parents of all clean nodes
    clean_paths = status['up-to-date'].keys()
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

    output_file = client.workflow_path / '{0}.cwl'.format(uuid.uuid4().hex)
    with open(output_file, 'w') as f:
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

    # TODO remove existing outputs?
    call(['cwl-runner', str(output_file)])
