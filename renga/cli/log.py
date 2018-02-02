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

    H = graph.G.copy()

    from collections import defaultdict
    from networkx.algorithms.dag import dag_to_branching, dag_longest_path

    slots = []
    slot = list(dag_longest_path(H))
    while slot:
        slots.append(slot)
        H.remove_nodes_from(slot)
        slot = list(dag_longest_path(H))

    line = [None] * len(slots)

    def find_index(node, slots, line):
        line = [False if item else item for item in line]
        result = None

        for index, slot in enumerate(slots):
            if slot and slot[0] == node:
                line[index] = True if line[index] is not None else click.style(
                    '@', fg='blue')
                slot.pop(0)
                result = index
            # elif not slot and line[index] is not None:
            #     line[index] = None
        return result, line

    char_map = {None: ' ', True: '*', False: '|'}

    slot_lookup = {key: index for index,
                   slot in enumerate(slots) for key in slot}
    merge = {}

    nodes = list(topological_sort(graph.G))

    for key in nodes:

        join_branches = None
        for (close, merge_line) in merge.get(key, []):
            join_branches = join_branches or 2 * len(slots) * [' ']
            for i, c in enumerate(merge_line):
                if join_branches[i] == ' ':
                    join_branches[i] = c
            join_branches[2 * close] = '+'
            line[close] = None

        index, line = find_index(key, slots, line)
        prefix = ' '.join(char_map.get(i, i) for i in line).rstrip()

        node = graph.G.nodes[key]
        commit, _ = key

        if join_branches:
            for i, c in enumerate(prefix):
                if join_branches[i] == ' ' and c != ' ':
                    join_branches[i] = c
            join_branches[2 * index] = '+'
            click.echo(''.join(join_branches))

        click.echo('{prefix} {sha} {latest}{message}'.format(
            prefix=prefix,
            sha=click.style(str(commit)[:7], fg='yellow'),
            message=node['path'],
            latest='(latest -> {0}) '.format(
                click.style(str(node['latest'])[:7], fg='red'))
            if node.get('latest') else '',
        ))

        for source, target in graph.G.out_edges(key):
            merge_to = slot_lookup.get(target)
            if merge_to != index:
                merge_line = 2 * min(merge_to, index) * \
                    ' ' + 2 * abs(index - merge_to) * '-'
                merge.setdefault(target, [])
                merge[target].append((index, merge_line))
