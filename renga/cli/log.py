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
"""Show provenance of data created by executing programs."""

import click
from networkx.algorithms.dag import topological_sort

from ._graph import Graph
from ._repo import pass_repo


@click.command()
@click.option('--revision', default='HEAD')
@click.argument('path', type=click.Path(exists=True, dir_okay=False))
@pass_repo
def log(repo, revision, path):
    """Show logs for a file."""
    graph = Graph(repo)
    root = graph.add_file(path, revision=revision)

    for key in reversed(list(topological_sort(graph.G))):
        node = graph.G.node[key]
        commit = node['commit']
        click.echo('* {sha} {latest}{message}'.format(
            sha=str(commit)[:7],
            message=node['path'],
            latest='(latest {0}) '.format(node['latest'])
            if node.get('latest') else '',
        ))
