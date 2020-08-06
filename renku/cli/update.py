# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
from git import Actor

from renku.core.commands.client import pass_local_client
from renku.core.commands.cwl_runner import execute
from renku.core.commands.graph import Graph, _safe_path
from renku.core.commands.options import option_siblings
from renku.core.models.locals import with_reference
from renku.core.models.provenance.activities import WorkflowRun
from renku.core.models.workflow.converters.cwl import CWLConverter
from renku.version import __version__, version_url


@click.command()
@click.option("--revision", default="HEAD")
@click.option("--no-output", is_flag=True, default=False, help="Display commands without output files.")
@option_siblings
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@pass_local_client(
    clean=True, requires_migration=True, commit=True,
)
def update(client, revision, no_output, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    graph = Graph(client)
    outputs = graph.build(revision=revision, can_be_cwl=no_output, paths=paths)
    outputs = {node for node in outputs if graph.need_update(node)}
    if not outputs:
        click.secho("All files were generated from the latest inputs.", fg="green")
        sys.exit(0)

    # Check or extend siblings of outputs.
    outputs = siblings(graph, outputs)
    output_paths = {node.path for node in outputs if _safe_path(node.path)}

    # Get all clean nodes.
    input_paths = {node.path for node in graph.nodes} - output_paths

    # Store the generated workflow used for updating paths.
    workflow = graph.as_workflow(input_paths=input_paths, output_paths=output_paths, outputs=outputs,)

    wf, path = CWLConverter.convert(workflow, client)
    # Don't compute paths if storage is disabled.
    if client.check_external_storage():
        # Make sure all inputs are pulled from a storage.
        paths_ = (i.consumes.path for i in workflow.inputs)
        client.pull_paths_from_storage(*paths_)

    execute(client, path, output_paths=output_paths)

    paths = [o.produces.path for o in workflow.outputs]

    client.repo.git.add(*paths)

    if client.repo.is_dirty():
        commit_msg = ("renku update: " "committing {} newly added files").format(len(paths))

        committer = Actor("renku {0}".format(__version__), version_url)

        client.repo.index.commit(
            commit_msg, committer=committer, skip_hooks=True,
        )

    workflow_name = "{0}_update.yaml".format(uuid.uuid4().hex)

    path = client.workflow_path / workflow_name

    workflow.update_id_and_label_from_commit_path(client, client.repo.head.commit, path)

    with with_reference(path):
        run = WorkflowRun.from_run(workflow, client, path, update_commits=True)
        run.to_yaml()
        client.add_to_activity_index(run)
