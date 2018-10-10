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

.. _cli-update-with-siblings:

Update siblings
~~~~~~~~~~~~~~~

If a tool produces multiple output files, these outputs need to be always
updated together.

.. code-block:: text

                   (B)
                  /
    *A*--[step 1]--(C)
                  \
                   (D)

An attempt to update a single file would fail with the following error.

.. code-block:: console

   $ renku update C
   Error: There are missing output siblings:

        B
        D

   Include the files above in the command or use --with-siblings option.

The following commands will produce the same result.

.. code-block:: console

   $ renku update --with-siblings C
   $ renku update B C D

"""

import sys
import uuid

import click

from renku.models.cwl._ascwl import ascwl

from ._client import pass_local_client
from ._git import with_git
from ._graph import Graph, _safe_path
from ._options import option_siblings


@click.command()
@click.option('--revision', default='HEAD')
@click.option(
    '--no-output',
    is_flag=True,
    default=False,
    help='Display commands without output files.'
)
@option_siblings
@click.argument(
    'paths', type=click.Path(exists=True, dir_okay=False), nargs=-1
)
@pass_local_client
@click.pass_context
@with_git()
def update(ctx, client, revision, no_output, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    graph = Graph(client)
    outputs = graph.build(revision=revision, can_be_cwl=no_output, paths=paths)
    outputs = {node for node in outputs if graph.need_update(node)}

    if not outputs:
        click.secho(
            'All files were generated from the latest inputs.', fg='green'
        )
        sys.exit(0)

    # Check or extend siblings of outputs.
    outputs = siblings(graph, outputs)
    output_paths = {node.path for node in outputs if _safe_path(node.path)}

    # Get all clean nodes
    input_paths = {node.path for node in graph.nodes} - output_paths

    # Store the generated workflow used for updating paths.
    import yaml

    output_file = client.workflow_path / '{0}.cwl'.format(uuid.uuid4().hex)
    workflow = graph.ascwl(
        input_paths=input_paths,
        output_paths=output_paths,
        outputs=outputs,
    )

    with output_file.open('w') as f:
        f.write(
            yaml.dump(
                ascwl(
                    workflow,
                    filter=lambda _, x: x is not None,
                    basedir=client.workflow_path,
                ),
                default_flow_style=False
            )
        )

    from ._cwl import execute
    execute(client, output_file, output_paths=output_paths)
