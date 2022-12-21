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
"""Track provenance of data created by executing programs.

Description
~~~~~~~~~~~

Track the execution of your command line scripts. This will enable detection of:

* arguments (flags),
* string and integer options,
* input files or directories if linked to existing paths in the repository,
* output files or directories if modified or created while running the command.

It will create a ``Plan`` (Workflow Template) that can be reused and a ``Run``
which is a record of a past workflow execution for provenance purposes. Refer
to the :ref:`cli-workflow` documentation for more details on this distinction.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.run:run
   :prog: renku run
   :nested: full

Examples
~~~~~~~~

.. code-block:: console

    $ renku run --name <plan name> -- <console command>

.. note:: If there were uncommitted changes in the repository, then the
   ``renku run`` command fails. See :program:`git status` for details.

.. warning:: If executed command/script has similar arguments to ``renku run``
    (e.g. ``--input``) they will be treated as ``renku run`` arguments. To
    avoid this, put a ``--`` separator between ``renku run`` and the
    command/script.

.. warning:: Input and output paths can only be detected if they are passed as
   arguments to ``renku run``.

.. warning:: Circular dependencies are not supported for ``renku run``. See
   :ref:`circular-dependencies` for more details.

.. warning:: When using output redirection in ``renku run`` on Windows (with
   `` > file`` or `` 2> file``), all Renku errors and messages are redirected
   as well and ``renku run`` produces no output on the terminal. On Linux,
   this is detected by renku and only the output of the command to be run is
   actually redirected. Renku specific messages such as errors get printed to
   the terminal as usual and don't get redirected.

.. cheatsheet::
   :group: Running
   :command: $ renku run --name <name> <command> [--input <in_file>...] [--output <out_file>...]
   :description: Execute a <command> with Renku tracking inputs and outputs. Input and output files
                 are automatically detected from the command string. Creates a workflow template
                 named <name>. With --input and/or --output: Manually specify input or output files to track.
   :target: rp

Detecting input paths
~~~~~~~~~~~~~~~~~~~~~

Any path passed as an argument to ``renku run``, which was not changed during
the execution, is identified as an input path. The identification only works if
the path associated with the argument matches an existing file or directory
in the repository.

The detection might not work as expected if:

* a file is **modified** during the execution. In this case it will be stored
  as an **output**;
* a path is not passed as an argument to ``renku run``.

.. topic:: Specifying auxiliary inputs (``--input``)

   You can specify extra inputs to your program explicitly by using the
   ``--input`` option. This is useful for specifying hidden dependencies
   that don't appear on the command line. Explicit inputs must exist before
   execution of ``renku run`` command. This option is not a replacement for
   the arguments that are passed on the command line. Files or directories
   specified with this option will not be passed as input arguments to the
   script.
   You can specify ``--input name=path`` or just ``--input path``, the former
   of which would also set the name of the input on the resulting Plan.

   For example, ``renku run --input infile=data.csv -- python script.py data.csv outfile``
   would force Renku to detect ``data.csv`` as an input file and set the name
   of the input to ``infile``.
   Similarly, ``renku run --input infile=data.csv -- python script.py``
   would let Renku know that ``script.py`` reads the file ``data.csv`` even
   though it does not show up on the command line.

.. topic:: Specifying auxiliary parameters (``--param``)

   You can specify extra parameters to your program explicitly by using the
   ``--param`` option. This is useful for getting Renku to consider a
   parameter as just a string even if it matches a file name in the project.
   This option is not a replacement for the arguments that are passed on the
   command line.
   You can specify ``--param name=value`` or just ``--param value``, the former
   of which would also set the name of the parameter on the resulting Plan.

   For example, ``renku run --param my-param=hello -- python script.py hello outfile``
   would force Renku to detect ``hello`` as the value of a string parameter
   with name ``my-param`` even if there is a file called ``hello`` present on the
   filesystem.

.. topic:: Disabling input detection (``--no-input-detection``)

    Input paths detection can be disabled by passing ``--no-input-detection``
    flag to ``renku run``. In this case, only the directories/files that are
    passed as explicit input are considered to be file inputs. Those passed via
    command arguments are ignored unless they are in the explicit inputs list.
    This only affects files and directories; command options and flags are
    still treated as inputs.

.. note:: ``renku run`` prints the generated plan after execution if you pass
    ``--verbose`` to it. You can check the generated plan to verify that the
    execution was done as you intended. The plan will always be printed to
    ``stderr`` even if it's directed to a file.

Detecting output paths
~~~~~~~~~~~~~~~~~~~~~~

Any path **modified** or **created** during the execution will be added as an
output.

Because the output path detection is based on the Git repository state after
the execution of ``renku run`` command, it is good to have a basic
understanding of the underlying principles and limitations of tracking
files in Git.

Git tracks not only the paths in a repository, but also the content stored in
those paths. Therefore:

* a recreated file with the same content is not considered an output file,
  but instead is kept as an input;
* file moves are detected based on their content and can cause problems;
* directories cannot be empty.

.. note:: When in doubt whether the outputs will be detected, remove all
  outputs using ``git rm <path>`` followed by ``git commit`` before running
  the ``renku run`` command.

.. topic:: Command does not produce any files (``--no-output``)

   If the program does not produce any outputs, the execution ends with an
   error:

   .. code-block:: text

      Error: There are not any detected outputs in the repository.

   You can specify the ``--no-output`` option to force tracking of such
   an execution.

.. cheatsheet::
   :group: Running
   :command: $ renku run --name <name> <command> --no-output
   :description: Run a <command> that produces no output.
   :target: rp

.. topic:: Specifying outputs explicitly (``--output``)

   You can specify expected outputs of your program explicitly by using the
   ``--output`` option. These output must exist after the execution of the
   ``renku run`` command. However, they do not need to be modified by
   the command.
   You can specify ``--output name=path`` or just `--output path``, the former
   of which would also set the name of the output on the resulting Plan.

   For instance, ``renku run --output result=result.txt -- python script.py -o result.txt``
   would force Renku to treat the file ``result.txt`` as an output of the
   workflow and set the name of the output to ``result``.
   Similarly, ``renku run --output result=result.txt -- python script.py``
   would let Renku know about ``result.txt`` created by ``script.py`` even
   though it does not show up on the command line command. Though Renku should
   automatically detect these cases under normal circumstances.

.. topic:: Disabling output detection (``--no-output-detection``)

    Output paths detection can be disabled by passing ``--no-output-detection``
    flag to ``renku run``. When disabled, only the directories/files that are
    passed as explicit output are considered to be outputs and those passed via
    command arguments are ignored.

.. cli-run-std

Detecting standard streams
~~~~~~~~~~~~~~~~~~~~~~~~~~

Often the program expect inputs as a standard input stream. This is detected
and recorded in the tool specification when invoked by ``renku run cat < A``.

Similarly, both redirects to standard output and standard error output can be
done when invoking a command:

.. code-block:: console

   $ renku run grep "test" B > C 2> D

.. warning:: Detecting inputs and outputs from pipes ``|`` is not supported.

Specifying inputs and outputs programmatically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes the list of inputs and outputs are not known before execution of the
program. For example, a program might accept a date range as input and access
all files within that range during its execution.

To address this issue, the program can dump a mapping of input and output files
that it is accessing in ``inputs.yml`` and ``outputs.yml``. This YAML file
should be of the format
.. code-block:: YAML

   name1: path1
   name2: path2

where name is the user-defined name of the input/output and path is the path.
When the program is finished, Renku will look for existence of these two files
and adds their content to the list of explicit inputs and outputs. Renku will
then delete these two files.

By default, Renku looks for these two files in ``.renku/tmp`` directory. One
can change this default location by setting ``RENKU_INDIRECT_PATH``
environment variable. When set, it points to a sub-directory within the
``.renku/tmp`` directory where ``inputs.yml`` and ``outputs.yml`` reside.

Exit codes
~~~~~~~~~~

All Unix commands return a number between 0 and 255 which is called an
"exit code". In case other numbers are returned, they are treated modulo 256
(-10 is equivalent to 246, 257 is equivalent to 1). The exit-code 0 represents
a *success* and non-zero exit-code indicates a *failure*.

Therefore the command specified after ``renku run`` is expected to return
exit-code 0. If the command returns different exit code, you can specify them
with ``--success-code=<INT>`` parameter.

.. code-block:: console

   $ renku run --success-code=1 --no-output fail

.. _circular-dependencies:

Circular Dependencies
~~~~~~~~~~~~~~~~~~~~~

Circular dependencies are not supported in ``renku run``. This means you cannot
use the same file or directory as both an input and an output in the same step,
for instance reading from a file as input and then appending to it is not
allowed. Since renku records all steps of an analysis workflow in a dependency
graph and it allows you to update outputs when an input changes, this would
lead to problems with circular dependencies. An update command would change the
input again, leading to renku seeing it as a changed input, which would run
update again, and so on, without ever stopping.

Due to this, the renku dependency graph has to be *acyclic*. So instead of
appending to an input file or writing an output file to the same directory
that was used as an input directory, create new files or write to other
directories, respectively.

.. _workflow-definition-file:

Workflow Definition File
~~~~~~~~~~~~~~~~~~~~~~~~

Instead of using ``renku run`` to track your workflows, you can pass a workflow
definition file to renku for execution and tracking. A workflow definition file
or workflow file contains definition of each individual command as execution
*steps*. A step's definition includes the command that will be executed along
with lists of all its inputs, outputs, and parameters that are used in the
command. The following shows a workflow file with one step:

.. code-block:: console

    name: workflow-file
    steps:
      head:
        command: head -n 10 data/collection/models.csv data/collection/colors.csv > intermediate
        inputs:
          - models:
              path: data/collection/models.csv
          - colors:
              path: data/collection/colors.csv
        outputs:
          temporary-result:
            path: intermediate
        parameters:
          n:
            prefix: -n
            value: 10

The step *head* in this workflow file, has two inputs, one output, and one
parameter. All these arguments are given a name for better understanding of
their purpose. The same workflow file can be simplified to the following
format:

.. code-block:: console

    name: workflow-file
    steps:
      head:
        command: head -n 10 data/collection/models.csv data/collection/colors.csv > intermediate
        inputs:
          - data/collection/models.csv
          - data/collection/colors.csv
        outputs:
          - intermediate
        parameters:
          - -n
          - 10

Although the latter format is more concise it's recommended to use the former
format since it's more readable and has a more clear definition. You can
provide a description for each of the elements in the workflow file. You can
also have a set of keywords for each step and for the workflow file. The
following listing shows a more complete definition of the same workflow file:

.. code-block:: console

    name: workflow-file
    description: A sample workflow file used for testing
    keywords:
      - workflow file
      - v1
    steps:
      head:
        command: head -n 10 data/collection/models.csv data/collection/colors.csv > intermediate
        description: first stage of the pipeline
        success_codes:
          - 0
          - 127
        keywords:
          - preprocessing
          - first step
        inputs:
          - models:
              description: all available model numbers
              path: data/collection/models.csv
          - colors:
              path: data/collection/colors.csv
        outputs:
          temporary-result:
            description: temporary intermediate result that won't be saved
            path: intermediate
        parameters:
          n:
            description: number of lines to print
            prefix: -n
            value: 10

.. topic:: Running a workflow file

    The ``renku run`` command can be used to execute a workflow file:

    .. code-block:: console

       $ renku run workflow-file.yml

    .. note:: It's recommended to use a ``.yml`` or ``.yaml`` extension for your
        workflow files. Renku uses this as a hint to guess that you want to run a
        workflow file (and not an executable script). You can force execution of
        a workflow file by passing a ``--file`` flag to the ``renku run`` command.

    Upon running, Renku executes each step of the workflow file and creates
    corresponding plans for them. The effect is exactly the same as running each
    step using ``renku run`` in the command line.

    .. note:: The order of execution of steps is determined by the dependencies
        between the steps and not by the order that they are defined in the
        workflow file.

    Automatic detection of inputs and outputs is disabled when running a workflow
    file. The reason is that the list of inputs and outputs are already given in
    the workflow file and there is no need for automatic detection. This also
    allows running workflow files in a dirty repository which is not possible when
    running individual commands. Moreover, once the execution is over, only the
    outputs that have been generated by the workflow file will be committed.

    You can also select a subset of steps for execution by passing their names
    after the workflow file. The following command runs only two steps named
    *step-2* and *step-7* in an example workflow files:

    .. code-block:: console

       $ renku run workflow-file.yml step-2 step-7

.. topic:: Argument templates in workflow files

    In the workflow definition file, each command's input, output, or parameter
    is defined in two places: One in the command and one in the list of arguments.
    The reason for this duplication is that Renku needs to know the type of each
    argument (whether it's an input, output, or parameter) as well as the order
    that each argument has in the command. This redundant definition of arguments
    is inconvenient for large workflow files and is error-prone.

    To address this issue, Renku supports argument templates for the workflow
    files. To use templates, you need to assign a name to each argument and then
    use this name to refer to each argument in the step's command. The following
    listing shows the first workflow file rewritten using templates:

    .. code-block:: console

        name: workflow-file
        steps:
          head:
            command: head $n $models $colors > $temporary-result
            inputs:
              - models:
                  path: data/collection/models.csv
              - colors:
                  path: data/collection/colors.csv
            outputs:
              temporary-result:
                path: intermediate
            parameters:
              n:
                prefix: -n
                value: 10

    As is shown, you can use the name of each argument (i.e. input, output, or
    parameter) prefixed with a ``$`` to refer to an argument.

    In addition, you can use ``$inputs``, ``$outputs``, or ``$parameters`` to refer
    to the list of all inputs, outputs, or parameters respectively:

    .. code-block:: console

        ...
            command: cmd $parameters $inputs $outputs
        ...

    .. note:: If input or output paths or parameter values contain a ``$`` you
        must replace them with ``$$`` in the command. This doesn't need to be
        done in the list of arguments.

.. topic:: ``--dry-run`` and ``--no-commit`` flags

    By passing the ``--dry-run`` flag to the ``renku run`` command, you can
    instruct Renku to only print the order of execution of the steps without
    actually running any of them. Note that this option is valid only for running
    workflow files and it is ignored when running a single command.

    The ``--no-commit`` flags causes Renku to run the workflow file but it won't
    create a commit after the execution. Renku also won't create any metadata in
    this case.

    .. code-block:: console

       $ renku run --no-commit workflow-file.yml
"""

import os
from pathlib import Path

import click
from lazy_object_proxy import Proxy

from renku.command.run import run_workflow_file_command
from renku.core import errors
from renku.core.plugin.workflow_file_parser import read_workflow_file
from renku.core.util.os import is_subpath
from renku.core.workflow.workflow_file import get_all_workflow_file_inputs_and_outputs
from renku.domain_model.project_context import project_context
from renku.ui.cli.utils.callback import ClickCallback
from renku.ui.cli.utils.plugins import available_workflow_providers
from renku.ui.cli.utils.terminal import print_workflow_file


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option("--name", help="A name for the workflow step.")
@click.option("--description", help="Workflow step's description.")
@click.option("--keyword", multiple=True, help="List of keywords for the workflow.")
@click.option("explicit_inputs", "--input", multiple=True, help="Force a path to be considered as an input.")
@click.option("explicit_outputs", "--output", multiple=True, help="Force a path to be considered an output.")
@click.option("explicit_parameters", "--param", multiple=True, help="Force a string to be considered a parameter.")
@click.option("--no-output", is_flag=True, default=False, help="Allow command without output files.")
@click.option("--no-input-detection", is_flag=True, default=False, help="Disable auto-detection of inputs.")
@click.option("--no-output-detection", is_flag=True, default=False, help="Disable auto-detection of outputs.")
@click.option(
    "--success-code",
    "success_codes",
    type=int,
    multiple=True,
    callback=lambda _, __, values: [int(value) % 256 for value in values],
    help="Allowed command exit-code.",
)
@click.option("--isolation", is_flag=True, default=False, help="Invoke the given command in isolation.")
@click.option("--file", is_flag=True, default=False, help="Force running of a workflow file.")
@click.option("--verbose", is_flag=True, default=False, help="Print generated plan after the execution.")
@click.option(
    "--creator",
    "creators",
    default=None,
    multiple=True,
    type=click.UNPROCESSED,
    help="Creator's name, email, and affiliation. Accepted format is 'Forename Surname <email> [affiliation]'.",
)
@click.option("--dry-run", is_flag=True, help="Show what would have been executed in a workflow file")
@click.option("--no-commit", is_flag=True, help="Don't update metadata after the execution and don't create a commit.")
@click.option(
    "--provider",
    default=None,
    show_default=False,
    type=click.Choice(Proxy(available_workflow_providers), case_sensitive=False),
    help="The workflow engine to use for executing workflow files.",
)
@click.argument("command_line", nargs=-1, metavar="<COMMAND> or <WORKFLOW FILE>", required=True, type=click.UNPROCESSED)
def run(
    name,
    description,
    keyword,
    explicit_inputs,
    explicit_outputs,
    explicit_parameters,
    no_output,
    no_input_detection,
    no_output_detection,
    success_codes,
    isolation,
    file,
    command_line,
    verbose,
    creators,
    dry_run,
    no_commit,
    provider,
):
    """Tracking work on a specific problem."""
    from renku.command.run import run_command_line_command
    from renku.core.util.metadata import construct_creators
    from renku.ui.cli.utils.terminal import print_plan

    communicator = ClickCallback()

    def is_workflow_file() -> bool:
        """Some heuristics to guess if the first argument is a workflow file or not."""
        if file:
            return True
        if not command_line:
            return False

        path = Path(command_line[0])
        if not path.is_file() or not is_subpath(path=path, base=project_context.path):
            return False
        if path.suffix.lower() in [".yml", ".yaml"] and not os.access(path, os.X_OK):
            return True

        try:
            content = read_workflow_file(path=path)
        except (errors.ParseError, errors.ParameterError):
            return False
        else:
            # NOTE: A single string is also a valid YAML, so, we check that the file contains a dict
            return isinstance(content, dict)

    if is_workflow_file():
        # NOTE: Check other flags and warn if they are set since they don't have any effect
        if (
            name
            or description
            or keyword
            or explicit_inputs
            or explicit_outputs
            or explicit_parameters
            or no_output
            or no_input_detection
            or no_output_detection
            or success_codes
            or isolation
            or creators
        ):
            communicator.warn("All flags other than '--file', '--verbose', '--dry-run', and 'no-commit' are ignored")

        path = command_line[0]
        no_commit = no_commit or dry_run

        # NOTE: Read the workflow file to get list of generated files that should be committed
        if no_commit:
            workflow_file = None
            commit_only = None
        else:
            workflow_file = read_workflow_file(path=path, parser="renku")
            commit_only = (
                [path] + get_all_workflow_file_inputs_and_outputs(workflow_file) + [str(project_context.metadata_path)]
            )

        provider = provider or "local"

        result = (
            run_workflow_file_command(no_commit=no_commit, commit_only=commit_only)
            .with_communicator(communicator)
            .build()
            .execute(path=path, steps=command_line[1:], dry_run=dry_run, workflow_file=workflow_file, provider=provider)
        )

        if dry_run:
            for line in result.output[1]:
                communicator.echo(line)
        elif verbose:
            print_workflow_file(result.output[0])
    else:
        # NOTE: ``dry-run`` and ``no-commit`` should be passed only with WFF
        if dry_run:
            communicator.warn("'--dry-run' flag is valid only when running workflow files")
        if no_commit:
            communicator.warn("'--no-commit' flag is valid only when running workflow files")
        if provider:
            communicator.warn("'--provider' flag is valid only when running workflow files")

        command = run_command_line_command()

        if isolation:
            command = command.with_git_isolation()

        if creators:
            creators, _ = construct_creators(creators)

        result = (
            command.with_communicator(communicator)
            .build()
            .execute(
                name=name,
                description=description,
                keyword=keyword,
                explicit_inputs=explicit_inputs,
                explicit_outputs=explicit_outputs,
                explicit_parameters=explicit_parameters,
                no_output=no_output,
                no_input_detection=no_input_detection,
                no_output_detection=no_output_detection,
                success_codes=success_codes,
                command_line=command_line,
                creators=creators,
            )
        )

        if verbose:
            plan = result.output
            print_plan(plan, err=True)
