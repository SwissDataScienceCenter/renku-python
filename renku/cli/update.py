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

import os
import sys
import uuid

import click
import networkx as nx
import yaml

from renku.models.cwl._ascwl import ascwl

from ._client import pass_local_client
from ._echo import progressbar
from ._git import with_git
from ._graph import Graph


def check_siblings(graph, outputs):
    """Check that all outputs have their siblings listed."""
    siblings = set()
    for node in outputs:
        siblings |= graph.siblings(node)

    missing = siblings - outputs
    if missing:
        raise click.ClickException(
            'There are missing output siblings:\n\n'
            '{0}\n\n'
            'Include the files above or use --with-siblings option.'.format(
                '\n'.join(
                    '  {0}'.format(click.style(path, fg='red'))
                    for _, path in missing
                )
            ),
        )
    return outputs


def with_siblings(graph, outputs):
    """Include all missing siblings."""
    siblings = set()
    for node in outputs:
        siblings |= graph.siblings(node)
    return siblings


@click.command()
@click.option('--revision', default='HEAD')
@click.option(
    '--check-siblings',
    'check_siblings',
    flag_value=check_siblings,
    default=True,
    help=check_siblings.__doc__,
)
@click.option(
    '--with-siblings',
    'check_siblings',
    flag_value=with_siblings,
    default=True,
    help=with_siblings.__doc__,
)
@click.argument(
    'paths', type=click.Path(exists=True, dir_okay=False), nargs=-1
)
@pass_local_client
@click.pass_context
@with_git()
def update(ctx, client, revision, check_siblings, paths):
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
        # Check siblings *only* when paths were specified.
        outputs = check_siblings(graph, outputs)

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

    import cwltool.factory
    from cwltool import workflow
    from cwltool.utils import visit_class

    def makeTool(toolpath_object, **kwargs):
        """Fix missing locations."""
        protocol = 'file://'

        def addLocation(d):
            if 'location' not in d and 'path' in d:
                d['location'] = protocol + d['path']

        visit_class(toolpath_object, ('File', 'Directory'), addLocation)
        return workflow.defaultMakeTool(toolpath_object, **kwargs)

    argv = sys.argv
    sys.argv = ['cwltool']

    # Keep all environment variables.
    execkwargs = {
        'preserve_entire_environment': True,
    }

    factory = cwltool.factory.Factory(makeTool=makeTool, **execkwargs)
    process = factory.make(os.path.relpath(output_file))
    outputs = process()

    sys.argv = argv

    output_dirs = process.factory.executor.output_dirs

    def remove_prefix(location, prefix='file://'):
        if location.startswith(prefix):
            return location[len(prefix):]
        return location

    locations = {
        remove_prefix(output['location'])
        for output in outputs.values()
    }

    with progressbar(
        locations,
        label='Moving outputs',
    ) as bar:
        for location in bar:
            for output_dir in output_dirs:
                if location.startswith(output_dir):
                    output_path = client.path / location[len(output_dir):
                                                         ].lstrip(os.path.sep)
                    os.rename(location, output_path)
                    continue
