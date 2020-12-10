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
r"""Recreate files created by the "run" command.

Recreating files
~~~~~~~~~~~~~~~~

Assume you have run a step 2 that uses a stochastic algorithm, so each run
will be slightly different. The goal is to regenerate output ``C`` several
times to compare the output. In this situation it is not possible to simply
call :ref:`cli-update` since the input file ``A`` has not been modified
after the execution of step 2.

.. code-block:: text

    A-[step 1]-B-[step 2*]-C

Recreate a specific output file by running:

  .. code-block:: console

     $ renku rerun C

If you would like to recreate a file which was one of several produced by
a tool, then these files must be recreated as well. See the explanation in
:ref:`updating siblings <cli-update-with-siblings>`.
"""

import os
import sys
from pathlib import Path

import click

from renku.cli.update import execute_workflow
from renku.core import errors
from renku.core.commands.client import pass_local_client
from renku.core.commands.graph import Graph
from renku.core.commands.options import option_siblings


def show_inputs(client, workflow):
    """Show workflow inputs and exit."""
    for input_ in workflow.inputs:
        click.echo("{id}: {default}".format(id=input_._id, default=input_.consumes.path,))
    sys.exit(0)


def edit_inputs(client, workflow):
    """Edit workflow inputs."""
    for input_ in workflow.inputs:
        new_path = click.prompt("{0._id}".format(input_), default=input_.consumes.path,)
        input_.consumes.path = str(Path(os.path.abspath(new_path)).relative_to(client.path))

        try:
            input_.consumes.commit = client.find_previous_commit(input_.consumes.path)
        except KeyError:
            raise errors.DirtyRepository(f"Please commit {input_.consumes.path} before using it as an input.")

        input_.consumes._id = input_.consumes.default_id()
        input_.consumes._label = input_.consumes.default_label()

    for step in workflow.subprocesses:
        for argument in step.process.arguments:
            argument.value = click.prompt("{0._id}".format(argument), default=argument.value,)

    return workflow


@click.command()
@click.option("--revision", default="HEAD")
@click.option(
    "--from",
    "roots",
    type=click.Path(exists=True, dir_okay=False),
    multiple=True,
    help="Start an execution from this file.",
)
@option_siblings
@click.option(
    "--default-inputs", "inputs", default=True, flag_value=lambda _, workflow: workflow, help="Use default inputs.",
)
@click.option(
    "--show-inputs", "inputs", flag_value=show_inputs, help=show_inputs.__doc__,
)
@click.option(
    "--edit-inputs", "inputs", flag_value=edit_inputs, help=edit_inputs.__doc__,
)
@click.argument(
    "paths", type=click.Path(exists=True, dir_okay=True), nargs=-1, required=True,
)
@pass_local_client(
    clean=True, requires_migration=True, commit=True,
)
def rerun(client, revision, roots, siblings, inputs, paths):
    """Recreate files generated by a sequence of ``run`` commands."""
    graph = Graph(client)
    outputs = graph.build(paths=paths, revision=revision)

    # Check or extend siblings of outputs.
    outputs = siblings(graph, outputs)
    output_paths = {node.path for node in outputs}

    # Normalize and check all starting paths.
    roots = {graph.normalize_path(root) for root in roots}
    output_paths -= roots
    outputs = [o for o in outputs if o.path not in roots]

    # Generate workflow and check inputs.
    # NOTE The workflow creation is done before opening a new file.
    workflow = inputs(client, graph.as_workflow(input_paths=roots, output_paths=output_paths, outputs=outputs,))

    execute_workflow(
        client=client, workflow=workflow, output_paths=output_paths, command_name="rerun", update_commits=False
    )
