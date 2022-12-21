# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Manage the set of execution templates created by the ``renku run`` command.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.workflow:workflow
   :prog: renku workflow
   :nested: full

Description
~~~~~~~~~~~

Renku records two different kinds of metadata when a workflow is executed,
``Run`` and ``Plan``.
Plans describe a recipe for a command. They function as a template that
can be used directly or combined with other workflow templates to create more
complex recipes.
These Plans can be run in various ways, on creation with ``renku run``,
doing a ``renku rerun`` or ``renku update`` or manually using ``renku workflow
execute``.

Each time a ``Plan`` is run, we track that instance of it as a ``Run``.
Runs track workflow execution through time. They track which Plan was
run, at what time, with which specific values. This gives an insight into what
were the steps taken in a repository, how they were taken and what results they
produced.

The ``renku workflow`` group of commands contains most of the commands used
to interact with Plans and Runs

Working with Plans
~~~~~~~~~~~~~~~~~~

Listing Plans
*************

.. image:: ../../_static/asciicasts/list_plans.delay.gif
   :width: 850
   :alt: List Plans

.. code-block:: console

    $ renku workflow ls
    ID                                       NAME
    ---------------------------------------  ---------------
    /plans/11a3702184394b93ac422df760e40999  cp-B-C-ca4da
    /plans/96642cac86d9435e8abce2384f8618b9  cat-A-C-fa017
    /plans/96c70626575c41c5a13853b070eaaaf5  my-other-run
    /plans/9a0961844fcc46e1816fde00f57e24a8  my-run

Each entry corresponds to a recorded Plan/workflow template. You can also
show additional columns using the ``--columns`` parameter, which takes any
combination of values from ``id``, ``name``, ``keywords`` and ``description``.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow ls
   :description: List Plans (workflow templates).
   :target: rp

Showing Plan Details
********************

.. image:: ../../_static/asciicasts/show_plan.delay.gif
   :width: 850
   :alt: Show Plan

You can see the details of a plan by using ``renku workflow show``:

.. code-block:: console

    $ renku workflow show my-run
    Id: /plans/9a0961844fcc46e1816fde00f57e24a8
    Name: run1
    Command: cp A B
    Success Codes:
    Inputs:
            - input-1:
                    Default Value: A
                    Position: 1
    Outputs:
            - output-2:
                    Default Value: B
                    Position: 2

This shows the unique Id of the Plan, its name, the full command of the Plan
if it was run without any modifications (more on that later), which exit codes
should be considered successful executions (defaults to ``0``) as well as its
inputs, outputs and parameters.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow show <name>
   :description: Show details for Plan <name>.
   :target: rp

Executing Plans
***************

.. image:: ../../_static/asciicasts/execute_plan.delay.gif
   :width: 850
   :alt: Execute Plans

Plans can be executed using ``renku workflow execute``. They can be run as-is
or their parameters can be modified as needed. Renku has a plugin architecture
to allow execution using various execution backends.

.. code-block:: console

    $ renku workflow execute --provider cwltool --set input-1=file.txt my-run

Parameters can be set using the ``--set`` keyword or by specifying them in a
values YAML file and passing that using ``--values``. In case of passing a file,
the YAML should follow the this structure:

.. code-block:: yaml

    learning_rate: 0.9
    dataset_input: dataset.csv
    chart_output: chart.png
    my-workflow:
        lr: 0.8
        lookup-table: lookup.xml
        my-other-workflow:
            language: en

In addition to being passed on the command line and being available to
``renku.ui.api.*`` classes in Python scripts, parameters are also set as
environment variables when executing the command, in the form of
``RENKU_ENV_<parameter name>``.

Provider specific settings can be passed as file using the ``--config`` parameter.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow execute --provider <provider> [--set
             <param-name>=<value>...] <name>
   :description: Execute a Plan using <provider> as a backend, overriding
                 parameter <param-name>'s value.
   :target: rp

In some cases it may be desirable to avoid updating the renku metadata
and to avoid committing this and any other change in the repository when a workflow
is executed. If this is the case then you can pass the ``--skip-metadata-update``
flag to ``renku workflow execute``.

Iterate Plans
*************

.. image:: ../../_static/asciicasts/iterate_plan.gif
   :width: 850
   :alt: Iterate Plans

For executing a Plan with different parametrization ``renku workflow iterate``
could be used. This sub-command is basically conducting a 'grid search'-like
execution of a Plan, with parameter-sets provided by the user.

.. code-block:: console

    $ renku workflow iterate --map parameter-1=[1,2,3] \
            --map parameter-2=[10,20] my-run

The set of possible values for a parameter can be given by ``--map`` command
line argument or by specifying them in a values YAML file and passing that
using ``--mapping``. Content of the mapping file for the above example
should be:

.. code-block:: yaml

    parameter-1: [1,2,3]
    parameter-2: [10,20]

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow iterate [--map <param-name>=[value,value,...]]
             <name>
   :description: Repeatedly execute a Plan, taking values from the list
                 specified with --map.
   :target: rp

By default ``renku workflow iterate`` will execute all the combination of the
given parameters' list of possible values. Sometimes it is desired that instead
of all the combination of possible values, a specific tuple of values are
executed. This could be done by marking the parameters that should be bound
together with the ``@tag`` suffix in their names.

.. code-block:: console

    $ renku workflow iterate --map parameter-1@tag1=[1,2,3] \
            --map parameter-2@tag1=[10,5,30] my-run

This will result in only three distinct execution of the ``my-run`` Plan,
with the following parameter combinations: ``[(1,10), (2,5), (3,30)]``. It is
important to note that parameters that have the same tag, should have the same
number of possible values, i.e. the values list should have the same length.

There's a special template variable for parameter values ``{iter_index}``, which
can be used to mark each iteration's index in a value of a parameter. The template
variable is going to be substituted with the iteration index (0, 1, 2, ...).

.. code-block:: console

    $ renku workflow iterate --map parameter-1=[10,20,30] \
            --map output=output_{iter_index}.txt my-run

This would execute ``my-run`` three times, where ``parameter-1`` values would be
``10``, ``20`` and ``30`` and the producing output files ``output_0.txt``,
``output_1.txt`` and ``output_2.txt`` files in this order.

In some cases it may be desirable to avoid updating the renku metadata
and to avoid committing this and any other change in the repository when a workflow
is iterated through. If this is the case then you can pass the ``--skip-metadata-update``
flag to ``renku workflow iterate``.

Exporting Plans
***************

You can export a Plan to a number of different workflow languages, such as CWL
(Common Workflow Language) by using ``renku workflow export``:

.. code-block:: console

    $ renku workflow export --format cwl my-run
    baseCommand:
    - cp
    class: CommandLineTool
    cwlVersion: v1.0
    id: 63e3a2a8-5b40-49b2-a2f4-eecc37bc76b0
    inputs:
    - default: B
    id: _plans_9a0961844fcc46e1816fde00f57e24a8_outputs_2_arg
    inputBinding:
        position: 2
    type: string
    - default:
        class: File
        location: file:///home/user/my-project/A
    id: _plans_9a0961844fcc46e1816fde00f57e24a8_inputs_1
    inputBinding:
        position: 1
    type: File
    - default:
        class: Directory
        location: file:///home/user/my-project/.renku
    id: input_renku_metadata
    type: Directory
    - default:
        class: Directory
        location: file:///home/user/my-project/.git
    id: input_git_directory
    type: Directory
    outputs:
    - id: _plans_9a0961844fcc46e1816fde00f57e24a8_outputs_2
    outputBinding:
        glob: $(inputs._plans_9a0961844fcc46e1816fde00f57e24a8_outputs_2_arg)
    type: File
    requirements:
    InitialWorkDirRequirement:
        listing:
        - entry: $(inputs._plans_9a0961844fcc46e1816fde00f57e24a8_inputs_1)
        entryname: A
        writable: false
        - entry: $(inputs.input_renku_metadata)
        entryname: .renku
        writable: false
        - entry: $(inputs.input_git_directory)
        entryname: .git
        writable: false

You can export into a file directly with ``-o <path>``.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow export --format <format> <plan>
   :description: Export a Plan in a given format (e.g. 'cwl').
   :target: rp


Composing Plans into larger workflows
*************************************
.. image:: ../../_static/asciicasts/compose_plan.delay.gif
   :width: 850
   :alt: Composing Plans

For more complex workflows consisting of several steps, you can use the
``renku workflow compose`` command. This creates a new workflow that has
sub-steps.

The basic usage is:

.. code-block:: console

   $ renku run --name step1 -- cp input intermediate
   $ renku run --name step2 -- cp intermediate output
   $ renku workflow compose my-composed-workflow step1 step2

This would create a new workflow called ``my-composed-workflow`` that
consists of ``step1`` and ``step2`` as steps. This new workflow is just
like any other workflow in renku in that it can be executed, exported
or composed with other workflows.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow compose <composed-name> <plan> <plan>
   :description: Create a new Plan composed of child Plans.
   :target: rp

Workflows can also be composed based on past Runs and their
inputs/outputs, using the ``--from`` and ``--to`` parameters. This finds
chains of Runs from inputs to outputs and then adds them to the
composed plan, applying mappings (see below) where appropriate to make
sure the correct values for execution are used in the composite. This
also means that all the parameters in the used plans are exposed on the
composed plan directly.
In the example above, this would be:

.. code-block:: console

   $ renku workflow compose --from input --to output my-composed-workflow

You can expose parameters of child steps on the parent workflow using
``--map``/``-m``  arguments followed by a mapping expression. Mapping expressions
take the form of ``<name>=<expression>`` where ``name`` is the name of the
property to be created on the parent workflow and expression points to one
or more fields on the child steps that should be mapped to this property.
The expressions come in two flavors, absolute references using the names
of workflows and properties, and relative references specifying the
position within a workflow.

An absolute expression in the example above could be ``step1.my_dataset``
to refer to the input, output or argument named ``my_dataset`` on the step
``step1``. A relative expression could be ``@step2.@output1`` to refer
to the first output of the second step of the composed workflow.

Valid relative expressions are ``@input<n>``, ``@output<n>`` and ``@param<n>``
for the nth input, output or argument of a step, respectively. For referring
to steps inside a composed workflow, you can use ``@step<n>``. For referencing
a mapping on a composed workflow, you can use ``@mapping<n>``. Of course, the
names of the objects for all these cases also work.

The expressions can also be combined using ``,`` if a mapping should point
to more than one parameter of a child step.

You can mix absolute and relative reference in the same expression, as you see
fit.

A full example of this would be:

.. code-block:: console

   $ renku workflow compose --map input_file=step1.@input2 \
       --map output_file=@step1.my-output,@step2.step2s_output \
       my-composed-workflow step1 step2

This would create a mapping called ``input_file`` on the parent workflow that
points to the second input of ``step1`` and a mapping called ``output_file``
that points to both the output ``my-output`` on ``step1`` and
``step2s_output`` on ``step2``.

You can also set default values for mappings, which override the default values
of the parameters they're pointing to by using the ``--set``/``-s`` parameter, for
instance:

.. code-block:: console

   $ renku workflow compose --map input_file=step1.@input2 \
       --set input_file=data.csv
       my-composed-workflow step1 step2


This would lead to ``data.csv`` being used for the second input of
``step1`` when ``my-composed-workflow`` is executed (if it isn't overridden
at execution time).

You can add a description to the mappings to make them more human-readable
by using the ``--describe-param``/``-p`` parameter, as shown here:

.. code-block:: console

   $ renku workflow compose --map input_file=step1.@input2 \
       -p input_file="The dataset to process"
       my-composed-workflow step1 step2

You can also expose all inputs, outputs or parameters of child steps by
using ``--map-inputs``, ``--map-outputs`` or ``--map-params``, respectively.

On execution, renku will automatically detect links between steps, if an input
of one step uses the same path as an output of another step, and execute
them in the correct order. Since this depends on what values are passed
at runtime, you might want to enforce a certain order of steps by explicitly
mapping outputs to inputs.

You can do that using the ``--link <source>=<sink>`` parameters, e.g.
``--link step1.@output1=step2.@input1``. This gets recorded on the
workflow template and forces ``step2.@input1`` to always be set to the same
path as ``step1.@output1``, irrespective of which values are passed at
execution time.

This way, you can ensure that the steps in your workflow are always executed
in the correct order and that the dependencies between steps are modeled
correctly.

Renku can also add links for you automatically based on the default values
of inputs and outputs, where inputs/outputs that have the same path get
linked in the composed run. To do this, pass the ``--link-all`` flag.

.. warning:: Due to workflows having to be directed acyclic graphs, cycles
   in the dependencies are not allowed. E.g. step1 depending on step2
   depending on step1 is not allowed. Additionally, the flow of information
   has to be from outputs to inputs or parameters, so you cannot map an input
   to an output, only the other way around.

Values on inputs/outputs/parameters get set according to the following
order of precedence (lower precedence first):

- Default value on a input/output/parameter
- Default value on a mapping to the input/output/parameter
- Value passed to a mapping to the input/output/parameter
- Value passed to the input/output/parameter
- Value propagated to an input from the source of a workflow link

Editing Plans
*************

.. image:: ../../_static/asciicasts/edit_plan.delay.gif
   :width: 850
   :alt: Editing Plans

Plans can be edited in some limited fashion, but we do not allow structural
changes, as that might cause issues with the reproducibility and provenance of
the project. If you want to do structural changes (e.g. adding/removing
parameters), we recommend you record a new plan instead.

You can change the name and description of Plans and of their parameters, as
well as changing default values of the parameters using the ``renku workflow
edit`` command:

.. code-block:: console

   $ renku workflow edit my-run --name new-run --description "my description"
     --rename-param input-1=my-input --set my-input=other-file.txt
     --describe-param my-input="My input parameter" my-run

This would rename the Plan ``my-run`` to ``new-run``, change its description,
rename its parameter ``input-1`` to ``my-input`` and set the default of this
parameter to ``other-file.txt`` and set its description.

+-----------------------+------------------------------------------------------+
| Option                | Description                                          |
+=======================+======================================================+
| ``-n, --name``        | Plan's name                                          |
+-----------------------+------------------------------------------------------+
| ``-d, --description`` | Plan's description.                                  |
+-----------------------+------------------------------------------------------+
| ``-s, --set``         | Set default value for a parameter.                   |
|                       | Accepted format is '<name>=<value>'                  |
+-----------------------+------------------------------------------------------+
| ``-m, --map``         | Add a new mapping on the Plan.                       |
|                       | Accepted format is '<name>=<name or expression>'     |
+-----------------------+------------------------------------------------------+
| ``-r, --rename-param``| Rename a parameter.                                  |
|                       | Accepted format is '<name>="new name"'               |
+-----------------------+------------------------------------------------------+
| ``-d,``               | Add a description for a parameter.                   |
| ``--describe-param``  | Accepted format is '<name>="description"'            |
+-----------------------+------------------------------------------------------+
| ``-m, --metadata``    | Path to file containing custom JSON-LD metadata to   |
|                       | be added to the dataset.                             |
+-----------------------+------------------------------------------------------+


.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow edit <plan>
   :description: Create a new Plan composed of child Plans.
   :target: rp

Removing Plans
**************

Sometimes you might want to discard a recorded Plan or reuse its name with a
new Plan. In these cases, you can delete the old plan using ``renku workflow
remove <plan name>``. Once a Plan is removed, it doesn't show up in most renku
workflow commands.
``renku update`` ignores deleted Plans, but ``renku rerun`` will still rerun
them if needed, to ensure reproducibility.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow delete <plan>
   :description: Remove a Plan.
   :target: rp

Working with Runs
~~~~~~~~~~~~~~~~~

Listing Runs
************

To get a view of what commands have been execute in the project, you can use
the ``renku log --workflows`` command:

.. code-block:: console

    $ renku log --workflows
    DATE                 TYPE  DESCRIPTION
    -------------------  ----  -------------
    2021-09-21 15:46:02  Run   cp A C
    2021-09-21 10:52:51  Run   cp A B

Refer to the documentation of the :ref:`cli-log` command for more details.

Visualizing Executions
**********************

.. image:: ../../_static/asciicasts/visualize_runs.delay.gif
   :width: 850
   :alt: Visualizing Runs

You can visualize past Runs made with renku using the ``renku workflow
visualize`` command.
This will show a directed graph of executions and how they are connected. This
way you can see exactly how a file was generated and what steps it involved.
It also supports an interactive mode that lets you explore the graph in a more
detailed way.

.. code-block:: console

   $ renku run echo "input" > input
   $ renku run cp input intermediate
   $ renku run cp intermediate output
   $ renku workflow visualize
        ╔════════════╗
        ║echo > input║
        ╚════════════╝
                *
                *
                *
            ┌─────┐
            │input│
            └─────┘
                *
                *
                *
    ╔═════════════════════╗
    ║cp input intermediate║
    ╚═════════════════════╝
                *
                *
                *
        ┌────────────┐
        │intermediate│
        └────────────┘
                *
                *
                *
    ╔══════════════════════╗
    ║cp intermediate output║
    ╚══════════════════════╝
                *
                *
                *
            ┌──────┐
            │output│
            └──────┘

    $ renku workflow visualize intermediate
        ╔════════════╗
        ║echo > input║
        ╚════════════╝
            *
            *
            *
            ┌─────┐
            │input│
            └─────┘
            *
            *
            *
    ╔═════════════════════╗
    ║cp input intermediate║
    ╚═════════════════════╝
            *
            *
            *
        ┌────────────┐
        │intermediate│
        └────────────┘
    $ renku workflow visualize --from intermediate
        ┌────────────┐
        │intermediate│
        └────────────┘
                *
                *
                *
    ╔══════════════════════╗
    ║cp intermediate output║
    ╚══════════════════════╝
                *
                *
                *
            ┌──────┐
            │output│
            └──────┘

You can also run in interactive mode using the ``--interactive`` flag.

.. code-block:: console

   $ renku workflow visualize --interactive

This will allow you to navigate between workflow execution and see details
by pressing the <Enter> key.

If you prefer to elaborate the output graph further, or if you wish to export
it for any reason, you can use the ``--format`` option to specify an output
format.

The following example generates the graph using the `dot` format. It can
be stored in a file or piped directly to any compatible tool. Here we
use the ``dot`` command line tool from graphviz to generate an SVG file.

.. code-block:: console

   $ renku workflow visualize --format dot <path> | dot -Tsvg > graph.svg

Use ``renku workflow visualize -h`` to see all available options.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow visualize [--interactive]
   :description: Show linked workflows as a graph.
   :target: rp


Removing Runs
*************

Renku allows you to undo a Run in a project by using ``renku workflow revert
<activity ID>``. You can obtain <activity ID> from the ``renku log`` command.
If the deleted run generated some files, Renku either deletes these files (in
case there are no earlier versions of them and they are not used in other
activities) or revert them to their earlier versions. You can ask Renku to keep the
generated files and only delete the metadata by passing the ``--metadata-only``
option.

.. warning:: Renku only checks project's runs/plans to see if files are used.
   It doesn't check if files, that are going to be deleted, are added to a
   dataset for example. Make sure that the project doesn't use such files in
   other places or always use ``--metadata-only`` option when reverting a run.

If you want to delete a run along with its plan use the ``--plan`` option.
This only deletes the plan if it's not used by any other activity.

Renku won't remove a run if there are downstream runs that depend on it. The
reason is that removing a run will break the link between its upstream and
downstream runs. If this is not an issue for you or if you want to delete the
downstream runs later, then pass the ``--force`` option to make Renku delete
the run anyway.

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow revert <activity ID>
   :description: Undo a Run.
   :target: rp

.. cheatsheet::
   :group: Undo Renku Command
   :command: $ renku workflow revert <activity ID>
   :description: Undo a Renku workflow execution.
   :target: ui


Input and output files
~~~~~~~~~~~~~~~~~~~~~~

You can list input and output files generated in the repository by running
``renku workflow inputs`` and ``renku workflow outputs`` commands. Alternatively,
you can check if all paths specified as arguments are input or output files
respectively.

.. code-block:: console

   $ renku run wc < source.txt > result.wc
   $ renku workflow inputs
   source.txt
   $ renku workflow outputs
   result.wc
   $ renku workflow outputs source.txt
   $ echo $?  # last command finished with an error code
   1

.. cheatsheet::
   :group: Workflows
   :command: $ renku workflow inputs|||$ renku workflow outputs
   :description: Show input respectively output files used by workflows.
   :target: rp

"""

import json
import os
import shutil
import sys
from pathlib import Path

import click
from lazy_object_proxy import Proxy

import renku.ui.cli.utils.color as color
from renku.command.format.workflow import WORKFLOW_COLUMNS, WORKFLOW_FORMATS, WORKFLOW_VISUALIZE_FORMATS
from renku.command.util import ERROR
from renku.command.view_model.activity_graph import ACTIVITY_GRAPH_COLUMNS
from renku.core import errors
from renku.core.util.util import NO_VALUE
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.plugins import available_workflow_providers, get_supported_formats
from renku.ui.cli.utils.terminal import print_workflow_file, show_text_with_pager


def _complete_workflows(ctx, param, incomplete):
    from renku.command.workflow import search_workflows_command

    try:
        result = search_workflows_command().build().execute(name=incomplete)
        return list(filter(lambda x: x.startswith(incomplete), result.output))
    except Exception:
        return []


@click.group()
def workflow():
    """Workflow commands."""
    pass


@workflow.command("ls")
@click.option(
    "--format", type=click.Choice(list(WORKFLOW_FORMATS.keys())), default="tabular", help="Choose an output format."
)
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="id,name,command",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(WORKFLOW_COLUMNS.keys())),
    show_default=True,
)
def list_workflows(format, columns):
    """List or manage workflows with subcommands."""
    from renku.command.workflow import list_workflows_command

    result = list_workflows_command().build().execute(format=format, columns=columns)
    click.echo(result.output)


@workflow.command()
@click.argument("name_or_id_or_path", metavar="<name_or_id_or_path>", shell_complete=_complete_workflows)
def show(name_or_id_or_path):
    """Show details for workflow <name_or_id_or_path>."""
    from renku.command.view_model.plan import PlanViewModel
    from renku.command.view_model.workflow_file import WorkflowFileViewModel
    from renku.command.workflow import show_workflow_command
    from renku.ui.cli.utils.terminal import print_composite_plan, print_plan

    communicator = ClickCallback()

    plan = (
        show_workflow_command()
        .with_communicator(communicator)
        .build()
        .execute(name_or_id_or_path=name_or_id_or_path)
        .output
    )

    if plan:
        if isinstance(plan, WorkflowFileViewModel):
            print_workflow_file(plan)
        elif isinstance(plan, PlanViewModel):
            print_plan(plan)
        else:
            print_composite_plan(plan)
    else:
        click.secho(ERROR + f"Workflow '{name_or_id_or_path}' not found.")


@workflow.command()
@click.argument("name", metavar="<name>", shell_complete=_complete_workflows)
@click.option("--force", is_flag=True, help="Override the existence check.")
def remove(name, force):
    """Remove a workflow named <name>."""
    from renku.command.workflow import remove_plan_command

    communicator = ClickCallback()

    remove_plan_command().with_communicator(communicator).build().execute(name_or_id=name, force=force)


@workflow.command()
@click.option("-d", "--description", help="Workflow step's description.")
@click.option("mappings", "-m", "--map", multiple=True, help="Mapping for a workflow parameter.")
@click.option("defaults", "-s", "--set", multiple=True, help="Default value for a workflow parameter.")
@click.option("links", "-l", "--link", multiple=True, help="Link source and sink parameters to connect steps.")
@click.option("-p", "--describe-param", multiple=True, help="Add description for a workflow parameter.")
@click.option("--map-inputs", is_flag=True, help="Exposes all child inputs as inputs on the CompositePlan.")
@click.option("--map-outputs", is_flag=True, help="Exposes all child outputs as outputs on the CompositePlan.")
@click.option("--map-params", is_flag=True, help="Exposes all child parameters as parameters on the CompositePlan.")
@click.option("--map-all", is_flag=True, help="Combination of --map-inputs, --map-outputs, --map-params.")
@click.option("--link-all", is_flag=True, help="Automatically link steps based on default values.")
@click.option("--keyword", multiple=True, help="List of keywords for the workflow.")
@click.option(
    "--from",
    "sources",
    type=click.Path(exists=True, dir_okay=False),
    multiple=True,
    help="Start a composite plan from this file as input.",
)
@click.option(
    "--to",
    "sinks",
    type=click.Path(exists=True, dir_okay=True),
    multiple=True,
    help="End a composite plan at this file as output.",
)
@click.option(
    "--creator",
    "creators",
    default=None,
    multiple=True,
    type=click.UNPROCESSED,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.argument("name", required=True)
@click.argument("steps", nargs=-1, type=click.UNPROCESSED, shell_complete=_complete_workflows)
def compose(
    description,
    mappings,
    defaults,
    links,
    describe_param,
    map_inputs,
    map_outputs,
    map_params,
    map_all,
    link_all,
    keyword,
    sources,
    sinks,
    creators,
    name,
    steps,
):
    """Create a composite workflow consisting of multiple steps."""
    from renku.command.workflow import compose_workflow_command
    from renku.core.util.metadata import construct_creators
    from renku.ui.cli.utils.terminal import print_composite_plan

    if (sources or sinks) and steps:
        click.secho(ERROR + "--from/--to cannot be used at the same time as passing run/step names.")
        exit(1)
    elif not (sources or sinks or steps):
        click.secho(ERROR + "Either --from/--to passing run/step names is required.")
        exit(1)

    if creators:
        creators, _ = construct_creators(creators)

    if map_all:
        map_inputs = map_outputs = map_params = True

    result = (
        compose_workflow_command()
        .build()
        .execute(
            name=name,
            description=description,
            mappings=mappings,
            defaults=defaults,
            links=links,
            param_descriptions=describe_param,
            map_inputs=map_inputs,
            map_outputs=map_outputs,
            map_params=map_params,
            link_all=link_all,
            keywords=keyword,
            steps=steps,
            sources=sources,
            sinks=sinks,
            creators=creators,
        )
    )

    if not result.error:
        print_composite_plan(result.output)


@workflow.command()
@click.argument("workflow_name", metavar="<name or uuid>", shell_complete=_complete_workflows)
@click.option("-n", "--name", metavar="<new name>", help="New name of the workflow")
@click.option("-d", "--description", metavar="<new desc>", help="New description of the workflow")
@click.option(
    "-s",
    "--set",
    "set_params",
    multiple=True,
    metavar="<parameter>=<value>",
    help="Set default <value> for a <parameter>/add new parameter",
)
@click.option(
    "-m",
    "--map",
    "map_params",
    multiple=True,
    metavar="<parameter>=<parameter or expression>",
    help="New mapping on the workflow",
)
@click.option(
    "-r",
    "--rename-param",
    "rename_params",
    multiple=True,
    metavar='<parameter>="name"',
    help="New name for parameter",
)
@click.option(
    "--describe-param",
    "describe_params",
    multiple=True,
    metavar='<parameter>="description"',
    help="New description of the workflow",
)
@click.option(
    "-m",
    "--metadata",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Custom metadata to be associated with the workflow.",
)
@click.option(
    "--creator",
    "creators",
    default=[NO_VALUE],
    multiple=True,
    type=click.UNPROCESSED,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option(
    "--keyword",
    "keywords",
    default=[NO_VALUE],
    type=click.UNPROCESSED,
    multiple=True,
    help="List of keywords for the workflow.",
)
def edit(
    workflow_name,
    name,
    description,
    set_params,
    map_params,
    rename_params,
    describe_params,
    metadata,
    creators,
    keywords,
):
    """Edit workflow details."""
    from renku.command.view_model.plan import PlanViewModel
    from renku.command.workflow import edit_workflow_command
    from renku.core.util.metadata import construct_creators
    from renku.ui.cli.utils.terminal import print_composite_plan, print_plan

    if list(creators) == [NO_VALUE]:
        creators = NO_VALUE

    keywords = list(keywords)
    if keywords == [NO_VALUE]:
        keywords = NO_VALUE

    if creators and creators is not NO_VALUE:
        creators, _ = construct_creators(creators, ignore_email=True)

    custom_metadata = None

    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    result = (
        edit_workflow_command()
        .build()
        .execute(
            name=workflow_name,
            new_name=name,
            description=description,
            set_params=set_params,
            map_params=map_params,
            rename_params=rename_params,
            describe_params=describe_params,
            creators=creators,
            keywords=keywords,
            custom_metadata=custom_metadata,
        )
    )
    if not result.error:
        plan = result.output
        if isinstance(plan, PlanViewModel):
            print_plan(plan)
        else:
            print_composite_plan(plan)


@workflow.command()
@click.argument("workflow_name", metavar="<name or uuid>", shell_complete=_complete_workflows)
@click.option(
    "-f",
    "--format",
    default="cwl",
    type=click.Choice(Proxy(get_supported_formats), case_sensitive=False),
    show_default=True,
    help="Workflow language format.",
)
@click.option(
    "-o",
    "--output",
    metavar="<path>",
    type=click.Path(exists=False),
    default=None,
    help="Save to <path> instead of printing to terminal",
)
@click.option(
    "--values",
    metavar="<file>",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="YAML file containing parameter mappings to be used.",
)
def export(workflow_name, format, output, values):
    """Export workflow."""
    from renku.command.workflow import export_workflow_command
    from renku.core.util.os import safe_read_yaml

    communicator = ClickCallback()

    values_dict = None
    if values is not None:
        values_dict = safe_read_yaml(values)

    result = (
        export_workflow_command()
        .with_communicator(communicator)
        .build()
        .execute(
            name_or_id=workflow_name,
            format=format,
            output=output,
            values=values_dict,
            basedir=None,
            resolve_paths=True,
            nest_workflows=False,
        )
    )

    if not output:
        click.echo(result.output)


@workflow.command()
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.pass_context
def inputs(ctx, paths):
    """Show all inputs used by workflows.

    <PATHS>    Limit results to these paths.
    """
    from renku.command.workflow import workflow_inputs_command

    result = workflow_inputs_command().build().execute(paths=paths)

    input_paths = result.output

    click.echo("\n".join(input_paths))

    if paths:
        if not input_paths or any(
            p not in input_paths and all(Path(o) not in Path(p).parents for o in input_paths) for p in paths
        ):
            ctx.exit(1)


@workflow.command()
@click.argument("paths", type=click.Path(exists=True, dir_okay=True), nargs=-1)
@click.pass_context
def outputs(ctx, paths):
    """Show all outputs generated by workflows.

    <PATHS>    Limit results to these paths.
    """
    from renku.command.workflow import workflow_outputs_command

    result = workflow_outputs_command().build().execute(paths=paths)

    output_paths = result.output

    click.echo("\n".join(output_paths))

    if paths:
        if not output_paths or any(
            p not in output_paths and all(Path(o) not in Path(p).parents for o in output_paths) for p in paths
        ):
            ctx.exit(1)


@workflow.command()
@click.option(
    "provider",
    "-p",
    "--provider",
    default="toil",
    show_default=True,
    type=click.Choice(Proxy(available_workflow_providers), case_sensitive=False),
    help="The workflow engine to use.",
)
@click.option(
    "config", "-c", "--config", metavar="<config file>", help="YAML file containing configuration for the provider."
)
@click.option(
    "set_params",
    "-s",
    "--set",
    multiple=True,
    metavar="<parameter>=<value>",
    help="Set <value> for a <parameter> to be used in execution.",
)
@click.option(
    "--values",
    metavar="<values-file>",
    type=click.Path(exists=True, dir_okay=False),
    help="YAML file containing parameter mappings to be used.",
)
@click.option("--skip-metadata-update", is_flag=True, help="Do not update the metadata store for the execution.")
@click.argument("name_or_id", required=True, shell_complete=_complete_workflows)
def execute(
    provider,
    config,
    set_params,
    values,
    skip_metadata_update,
    name_or_id,
):
    """Execute a given workflow."""
    from renku.command.workflow import execute_workflow_command

    communicator = ClickCallback()

    result = (
        execute_workflow_command(skip_metadata_update=skip_metadata_update)
        .with_communicator(communicator)
        .build()
        .execute(
            name_or_id=name_or_id,
            provider=provider,
            config=config,
            values=values,
            set_params=set_params,
        )
    )

    if result.output:
        click.echo(
            "Unchanged files:\n\n\t{0}".format(
                "\n\t".join(click.style(path, fg=color.YELLOW) for path in result.output)
            )
        )


@workflow.command(no_args_is_help=True)
@click.option(
    "--from",
    "sources",
    type=click.Path(exists=False, dir_okay=False),
    multiple=True,
    help="Start drawing the graph from this file.",
)
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="command",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(ACTIVITY_GRAPH_COLUMNS.keys())),
    show_default=True,
)
@click.option("-x", "--exclude-files", is_flag=True, help="Hide file nodes, only show Runs.")
@click.option("-a", "--ascii", is_flag=True, help="Only use Ascii characters for formatting.")
@click.option(
    "--revision",
    type=click.STRING,
    help="Git revision to generate the graph for.",
)
@click.option(
    "--format",
    type=click.Choice(list(WORKFLOW_VISUALIZE_FORMATS.keys())),
    default="console",
    help="Choose an output format.",
)
@click.option(
    "-i", "--interactive", is_flag=True, help="Interactively explore run graph. Only available for console output"
)
@click.option("--no-color", is_flag=True, help="Don't colorize console output.")
@click.option("--pager", is_flag=True, help="Force use pager (less) for console output.")
@click.option("--no-pager", is_flag=True, help="Don't use pager (less) for console output.")
@click.argument("paths", type=click.Path(exists=False, dir_okay=True), nargs=-1)
def visualize(sources, columns, exclude_files, ascii, revision, format, interactive, no_color, pager, no_pager, paths):
    """Visualization of workflows that produced outputs at the specified paths.

    Either PATHS or --from need to be set.
    """
    from renku.command.workflow import visualize_graph_command

    if format == WORKFLOW_VISUALIZE_FORMATS["console"] and pager and no_pager:
        raise errors.ParameterError("Can't use both --pager and --no-pager.")
    if revision and not paths:
        raise errors.ParameterError("Can't use --revision without specifying PATHS.")

    result = (
        visualize_graph_command()
        .build()
        .execute(sources=sources, targets=paths, show_files=not exclude_files, revision=revision)
    )
    if format == WORKFLOW_VISUALIZE_FORMATS["dot"]:
        output = result.output.dot_representation(columns=columns)

        if not output:
            return

        click.echo(output)
        return
    else:
        text_output, navigation_data = result.output.text_representation(
            columns=columns, color=not no_color, ascii=ascii
        )

        if not text_output:
            return

        if not interactive:
            max_width = max(node[1].x for layer in navigation_data for node in layer)
            tty_size = shutil.get_terminal_size(fallback=(120, 120))

            if no_pager or not sys.stdout.isatty() or os.system(f"less 2>{os.devnull}") != 0:
                use_pager = False
            elif pager:
                use_pager = True
            elif max_width < tty_size.columns:
                use_pager = False
            else:
                use_pager = True

            if use_pager:
                show_text_with_pager(text_output)
            else:
                click.echo(text_output)
            return

        from renku.ui.cli.utils.curses import CursesActivityGraphViewer

        viewer = CursesActivityGraphViewer(
            text_output, navigation_data, result.output.vertical_space, use_color=not no_color
        )
        viewer.run()


@workflow.command()
@click.option("--skip-metadata-update", is_flag=True, help="Do not update the metadata store for the execution.")
@click.option(
    "mapping_path",
    "--mapping",
    metavar="<file>",
    type=click.Path(exists=True, dir_okay=False),
    help="YAML file containing parameter mappings to be used.",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    default=False,
    help="Print the generated plans with their parameters instead of executing.",
    show_default=True,
)
@click.option(
    "provider",
    "-p",
    "--provider",
    default="toil",
    show_default=True,
    type=click.Choice(Proxy(available_workflow_providers), case_sensitive=False),
    help="The workflow engine to use.",
)
@click.option("mappings", "-m", "--map", multiple=True, help="Mapping for a workflow parameter.")
@click.option("config", "-c", "--config", metavar="<config file>", help="YAML file containing config for the provider.")
@click.argument("name_or_id", required=True, shell_complete=_complete_workflows)
def iterate(name_or_id, mappings, mapping_path, dry_run, provider, config, skip_metadata_update):
    """Execute a workflow by iterating through a range of provided parameters."""
    from renku.command.view_model.plan import PlanViewModel
    from renku.command.workflow import iterate_workflow_command, show_workflow_command
    from renku.ui.cli.utils.terminal import print_composite_plan, print_plan

    if len(mappings) == 0 and mapping_path is None:
        raise errors.UsageError("No mapping has been given for the iteration!")

    plan = show_workflow_command().build().execute(name_or_id_or_path=name_or_id).output

    if plan:
        if isinstance(plan, PlanViewModel):
            print_plan(plan)
        else:
            print_composite_plan(plan)

    communicator = ClickCallback()
    iterate_workflow_command(skip_metadata_update=skip_metadata_update).with_communicator(communicator).build().execute(
        name_or_id=name_or_id,
        mapping_path=mapping_path,
        mappings=mappings,
        dry_run=dry_run,
        provider=provider,
        config=config,
    )


@workflow.command()
@click.option(
    "-m",
    "--metadata-only",
    default=False,
    help="Only undo metadata, leave generated outputs unchanged.",
    is_flag=True,
    show_default=True,
)
@click.option(
    "-f",
    "--force",
    default=False,
    help="Force-revert the activity, even if it breaks things.",
    is_flag=True,
    show_default=True,
)
@click.option(
    "-p",
    "--plan",
    default=False,
    help="Delete activity's plan if no other activity is using it.",
    is_flag=True,
    show_default=True,
)
@click.argument("activity_id", required=True)
def revert(metadata_only, force, plan, activity_id):
    """Revert activity metadata and generations."""
    from renku.command.workflow import revert_activity_command

    communicator = ClickCallback()
    try:
        revert_activity_command().with_communicator(communicator).build().execute(
            metadata_only=metadata_only, force=force, delete_plan=plan, activity_id=activity_id
        )
    except errors.ActivityDownstreamNotEmptyError:
        raise errors.ParameterError(
            "Activity has downstream dependent activities: Pass '--force' if you want to revert the activity anyways."
        )
