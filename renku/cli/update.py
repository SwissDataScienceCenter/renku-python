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

In this situation, you can do effectively two things:

* Recreate a single file by running

  .. code-block:: console

     $ renku update E

* Update all files by simply running

  .. code-block:: console

     $ renku update --all

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

To avoid excessive recreation of the large portion of files which could have
been affected by a simple change of an input file, consider specifying a single
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
from renku.core.errors import ParameterError
from renku.core.models.cwl.command_line_tool import delete_indirect_files_list, read_indirect_parameters
from renku.core.models.provenance.activities import ProcessRun, WorkflowRun
from renku.core.models.workflow.converters.cwl import CWLConverter
from renku.core.models.workflow.parameters import RunParameter
from renku.version import __version__, version_url


@click.command()
@click.option("--revision", default="HEAD")
@click.option("--no-output", is_flag=True, default=False, help="Display commands without output files.")
@click.option("--all", "-a", "update_all", is_flag=True, default=False, help="Update all outdated files.")
@option_siblings
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.pass_context
@pass_local_client(clean=True, requires_migration=True, commit=True, commit_empty=False)
def update(client, ctx, revision, no_output, update_all, siblings, paths):
    """Update existing files by rerunning their outdated workflow."""
    if not paths and not update_all:
        click.echo(ctx.get_help(), color=ctx.color)
        return
    elif paths and update_all:
        raise ParameterError("Cannot use PATHS and --all/-a at the same time.")

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

    execute_workflow(
        client=client, workflow=workflow, output_paths=output_paths, command_name="update", update_commits=True
    )


def execute_workflow(client, workflow, output_paths, command_name, update_commits):
    """Execute a Run."""
    wf, path = CWLConverter.convert(workflow, client)
    # Don't compute paths if storage is disabled.
    if client.check_external_storage():
        # Make sure all inputs are pulled from a storage.
        paths_ = (i.consumes.path for i in workflow.inputs)
        client.pull_paths_from_storage(*paths_)

    delete_indirect_files_list(client.path)

    # Execute the workflow and relocate all output files.
    # FIXME get new output paths for edited tools
    # output_paths = {path for _, path in workflow.iter_output_files()}
    execute(client, path, output_paths=output_paths)

    paths = [o.produces.path for o in workflow.outputs]

    client.repo.git.add(*paths)

    if client.repo.is_dirty():
        commit_msg = f"renku {command_name}: committing {len(paths)} newly added files"

        committer = Actor(f"renku {__version__}", version_url)

        client.repo.index.commit(
            commit_msg, committer=committer, skip_hooks=True,
        )

    workflow_name = f"{uuid.uuid4().hex}_{command_name}.yaml"

    path = client.workflow_path / workflow_name

    workflow.update_id_and_label_from_commit_path(client, client.repo.head.commit, path)

    if not workflow.subprocesses:  # Update parameters if there is only one step
        _update_run_parameters(run=workflow, working_dir=client.path)

    cls = WorkflowRun if workflow.subprocesses else ProcessRun
    run = cls.from_run(run=workflow, client=client, path=path, update_commits=update_commits)
    run.to_yaml(path=path)
    client.add_to_activity_index(run)


def _update_run_parameters(run, working_dir):

    default_parameters = {p.name: p for p in run.run_parameters}

    indirect_parameters = read_indirect_parameters(working_dir)
    for name, value in indirect_parameters.items():
        id_ = RunParameter.generate_id(run_id=run._id, name=name)
        parameter = RunParameter(id=id_, name=name, value=value)
        default_parameters[name] = parameter

    run.run_parameters = list(default_parameters.values())
