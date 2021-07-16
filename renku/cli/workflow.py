# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Manage the set of CWL files created by ``renku`` commands.

Manipulating workflows
~~~~~~~~~~~~~~~~~~~~~~

Listing workflows:

.. code-block:: console

    $ renku workflow ls
    26be2e8d66f74130a087642768f2cef0_rerun.yaml:
    199c4b9d462f4b27a4513e5e55f76eb2_cat.yaml:
    9bea2eccf9624de387d9b06e61eec0b6_rerun.yaml:
    b681b4e229764ceda161f6551370af12_update.yaml:
    25d0805243e3468d92a3786df782a2c4_rerun.yaml:

Each ``*.yaml`` file corresponds to a renku run/update/rerun execution.

Exporting workflows:

You can export the workflow to create a file as Common Workflow Language
by using:

.. code-block:: console

    $ renku workflow set-name create output_file
    baseCommand:
    - cat
    class: CommandLineTool
    cwlVersion: v1.0
    id: 22943eca-fa4c-4f3b-a92d-f6ac7badc0d2
    inputs:
    - default:
        class: File
        path: /home/user/project/intermediate
    id: inputs_1
    inputBinding:
        position: 1
    type: File
    - default:
        class: File
        path: /home/user/project/intermediate2
    id: inputs_2
    inputBinding:
        position: 2
    type: File
    outputs:
    - id: output_stdout
    streamable: false
    type: stdout
    requirements:
    InitialWorkDirRequirement:
        listing:
        - entry: $(inputs.inputs_1)
        entryname: intermediate
        writable: false
        - entry: $(inputs.inputs_2)
        entryname: intermediate2
        writable: false
    stdout: output_file

You can use ``--revision`` to specify the revision of the output file to
generate the workflow for. You can also export to a file directly with
``-o <path>``.


Composing workflows into larger workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more complex workflows consisting of several steps, you can use the
``renku workflow group`` command. This creates a new workflow that has
substeps.

The basic usage is:

.. code-block:: console

   $ renku run --name workflow1 -- command
   $ renku run --name workflow2 -- command
   $ renku workflow group my-grouped-workflow workflow1 workflow2

This would create a new workflow ``my-grouped-workflow`` that consists
of ``workflow1`` and ``workflow2`` as steps. This new workflow is just
like any other workflow in renku in that it can be executed, exported
or grouped with other workflows.

You can expose parameters of child steps on the parent workflow using
``--map``/``-m``  arguments followed by a mapping expression. Mapping expressions
take the form of ``<name>=<expression>`` where ``name`` is the name of the
property to be created on the parent workflow and expression points to one
or more fields on the child steps that should be mapped to this property.
The expressions come in two flavors, absolute references using the names
of workflows and properties, and relative references specifying the
position within a workflow.

An absolute expression in the example above could be ``workflow1.my_dataset``
to refer to the input, output or argument named ``my_dataset` on the step
``workflow1``. A relative expression could be ``@step2.@output1`` to refer
to the first output of the second step of the grouped workflow.

Valid relative expressions are ``@input<n>``, ``@output<n>`` and ``@param<n>``
for the n'th input, output or argument of a step, respectively. For referring
to steps inside a grouped workflow, you can use ``@step<n>``. For referencing
a mapping on a grouped workflow, you can use ``@mapping<n>``. Of course, the
names of the objects for all these cases also work.

The expressions can also be combined using ``,`` if a mapping should point
to more than one parameter of a child step.

You can mix absolute and relative reference in the same expression, as you see fit.

A full example of this would be:

.. code-block:: console

   $ renku workflow group --map input_file=workflow1.@input2 \
       --map output_file=@step1.my-output,@step2.step2s_output \
       my-grouped-workflow workflow1 workflow2

This would create a mapping called ``input_file`` on the parent workflow that
points to the second input of ``workflow1`` and a mapping called ``output_file``
that points to both the output ``my-output`` on ``workflow1`` and
``step2s_output`` on ``workflow2``.

You can also set default values for mappings, which override the default values
of the parameters they're pointing to by using the ``--set``/``-s`` parameter, for
instance:

.. code-block:: console

   $ renku workflow group --map input_file=workflow1.@input2 \
       --set input_file=data.csv
       my-grouped-workflow workflow1 workflow2


This would lead to ``data.csv`` being used for the second input of
``workflow1`` when ``my-grouped-workflow`` is executed (if it isn't overridden
at execution time).

You can add a description to the mappings to make them more human-readable
by using the ``--describe-param``/``-p`` parameter, as shown here:



.. code-block:: console

   $ renku workflow group --map input_file=workflow1.@input2 \
       -p input_file="The dataset to process"
       my-grouped-workflow workflow1 workflow2

You can also expose all inputs, outputs or parameters of child steps by
using ``--map-inputs``, ``--map-outputs`` or ``--map-params``, respectively.

"""


import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.workflow import (
    create_workflow_command,
    group_workflow_command,
    list_workflows_command,
    remove_workflow_command,
    rename_workflow_command,
    set_workflow_name_command,
)


@click.group()
def workflow():
    """Workflow commands."""
    pass


@workflow.command("ls")
def list_workflows():
    """List or manage workflows with subcommands."""
    communicator = ClickCallback()
    list_workflows_command().with_communicator(communicator).build().execute()


def validate_path(ctx, param, value):
    """Detect a workflow path if it is not passed."""
    client = ctx.obj

    if value is None:
        from renku.core.models.provenance.activities import ProcessRun

        activity = client.process_commit()

        if not isinstance(activity, ProcessRun):
            raise click.BadParameter("No tool was found.")

        return activity.path

    return value


@workflow.command("set-name")
@click.argument("name", metavar="<name>")
@click.argument(
    "path",
    metavar="<path>",
    type=click.Path(exists=True, dir_okay=False),
    callback=validate_path,
    default=None,
    required=False,
)
@click.option("--force", is_flag=True, help="Override the existence check.")
def set_name(name, path, force):
    """Sets the <name> for remote <path>."""
    set_workflow_name_command().build().execute(name=name, path=path, force=force)


@workflow.command()
@click.argument("old", metavar="<old>")
@click.argument("new", metavar="<new>")
@click.option("--force", is_flag=True, help="Override the existence check.")
def rename(old, new, force):
    """Rename the workflow named <old> to <new>."""
    rename_workflow_command().build().execute(old=old, new=new, force=force)


@workflow.command()
@click.argument("name", metavar="<name>")
def remove(name):
    """Remove the remote named <name>."""
    remove_workflow_command().build().execute(name=name)


@workflow.command()
@click.option("--revision", default="HEAD")
@click.option(
    "-o",
    "--output-file",
    metavar="FILE",
    type=click.Path(exists=False),
    default=None,
    help="Write workflow to the FILE.",
)
@click.argument("paths", type=click.Path(dir_okay=True), nargs=-1)
def create(output_file, revision, paths):
    """Create a workflow description for a file."""
    result = create_workflow_command().build().execute(output_file=output_file, revision=revision, paths=paths)

    if not output_file:
        click.echo(result.output)


@workflow.command()
@click.option("-d", "--description", help="Workflow step's description.")
@click.option("mappings", "-m", "--map", multiple=True, help="Mapping for a workflow parameter.")
@click.option("defaults", "-s", "--set", multiple=True, help="Default value for a workflow parameter.")
@click.option("links", "-l", "--link", multiple=True, help="Link source and sink parameters to connect steps.")
@click.option("-p", "--describe-param", multiple=True, help="Default value for a workflow parameter.")
@click.option("--map-inputs", is_flag=True, help="Exposes all child inputs as inputs on the GroupedRun.")
@click.option("--map-outputs", is_flag=True, help="Exposes all child outputs as inputs on the GroupedRun.")
@click.option("--map-params", is_flag=True, help="Exposes all child parameters as inputs on the GroupedRun.")
@click.option("--map-all", is_flag=True, help="Combination of --map-inputs, --map-outputs, --map-params.")
@click.option("--keyword", multiple=True, help="List of keywords for the workflow.")
@click.argument("name", required=True)
@click.argument("workflow", nargs=-1, required=True, type=click.UNPROCESSED)
def group(
    description,
    mappings,
    defaults,
    links,
    describe_param,
    map_inputs,
    map_outputs,
    map_params,
    map_all,
    keyword,
    name,
    workflow,
):
    """Create a grouped workflow consisting of multiple steps."""

    if map_all:
        map_inputs, map_outputs, map_params = True

    group_workflow_command().build().execute(
        name=name,
        description=description,
        mappings=mappings,
        defaults=defaults,
        links=links,
        param_descriptions=describe_param,
        map_inputs=map_inputs,
        map_outputs=map_outputs,
        map_params=map_params,
        keywords=keyword,
        workflows=workflow,
    )
