# -*- coding: utf-8 -*-
#
# Copyright 2018-2019- Swiss Data Science Center (SDSC)
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
import uuid
from pathlib import Path

import click

from renku.core.commands.client import pass_local_client
from renku.core.commands.cwl_runner import execute
from renku.core.commands.graph import Graph
from renku.core.commands.options import option_siblings
from renku.core.models.cwl.ascwl import ascwl
from renku.core.models.cwl.types import File


def _format_default(client, value):
    """Format default values."""
    if isinstance(value, File):
        return os.path.relpath(
            str((client.workflow_path / value.path).resolve())
        )
    return value


def show_inputs(client, workflow):
    """Show workflow inputs and exit."""
    for input_ in workflow.inputs:
        click.echo(
            '{id}: {default}'.format(
                id=input_.id,
                default=_format_default(client, input_.default),
            )
        )
    sys.exit(0)


def edit_inputs(client, workflow):
    """Edit workflow inputs."""
    types = {
        'int': int,
        'string': str,
        'File': lambda x: File(path=Path(x).resolve()),
    }
    for input_ in workflow.inputs:
        convert = types.get(input_.type, str)
        input_.default = convert(
            click.prompt(
                '{0.id} ({0.type})'.format(input_),
                default=_format_default(client, input_.default),
            )
        )
    return workflow


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
@click.option(
    '--default-inputs',
    'inputs',
    default=True,
    flag_value=lambda _, workflow: workflow,
    help='Use default inputs.',
)
@click.option(
    '--show-inputs',
    'inputs',
    flag_value=show_inputs,
    help=show_inputs.__doc__,
)
@click.option(
    '--edit-inputs',
    'inputs',
    flag_value=edit_inputs,
    help=edit_inputs.__doc__,
)
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=True),
    nargs=-1,
    required=True,
)
@pass_local_client(
    clean=True,
    commit=True,
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
    assert not roots & output_paths, '--from colides with output paths'

    # Generate workflow and check inputs.
    # NOTE The workflow creation is done before opening a new file.
    workflow = inputs(
        client,
        graph.ascwl(
            input_paths=roots,
            output_paths=output_paths,
            outputs=outputs,
        )
    )

    # Don't compute paths if storage is disabled.
    if client.has_external_storage:
        # Make sure all inputs are pulled from a storage.
        paths_ = (
            path
            for _, path in workflow.iter_input_files(client.workflow_path)
        )
        client.pull_paths_from_storage(*paths_)

    # Store the generated workflow used for updating paths.
    import yaml

    output_file = client.workflow_path / '{0}.cwl'.format(uuid.uuid4().hex)
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

    # Execute the workflow and relocate all output files.
    # FIXME get new output paths for edited tools
    # output_paths = {path for _, path in workflow.iter_output_files()}
    execute(
        client,
        output_file,
        output_paths=output_paths,
    )
