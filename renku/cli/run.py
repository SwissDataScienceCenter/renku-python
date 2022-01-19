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
"""Track provenance of data created by executing programs.

Capture command line execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tracking execution of your command line script is done by simply adding the
``renku run`` command before the actual command. This will enable detection of:

* arguments (flags),
* string and integer options,
* input files or directories if linked to existing paths in the repository,
* output files or directories if modified or created while running the command.

It will create a ``Plan`` (Workflow Template) that can be reused and a ``Run``
which is a record of a past workflow execution for provenance purposes. Refer
to the :ref:`cli-workflow` documentation for more details on this distinction.

Basic usage is:

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
   :extended:

Detecting input paths
~~~~~~~~~~~~~~~~~~~~~

Any path passed as an argument to ``renku run``, which was not changed during
the execution, is identified as an input path. The identification only works if
the path associated with the argument matches an existing file or directory
in the repository.

The detection might not work as expected
if:

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
   You can specify ``--input name=path`` or just `--input path``, the former
   of which would also set the name of the input on the resulting Plan.

.. topic:: Specifying auxiliary parameters (``--param``)

   You can specify extra parameters to your program explicitly by using the
   ``--param`` option. This is useful for getting Renku to consider a
   parameter as just a string even if it matches a file name in the project.
   This option is not a replacement for the arguments that are passed on the
   command line.
   You can specify ``--param name=value`` or just `--param value``, the former
   of which would also set the name of the parameter on the resulting Plan.

.. topic:: Disabling input detection (``--no-input-detection``)

    Input paths detection can be disabled by passing ``--no-input-detection``
    flag to ``renku run``. In this case, only the directories/files that are
    passed as explicit input are considered to be file inputs. Those passed via
    command arguments are ignored unless they are in the explicit inputs list.
    This only affects files and directories; command options and flags are
    still treated as inputs.

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
   :extended:

.. topic:: Specifying outputs explicitly (``--output``)

   You can specify expected outputs of your program explicitly by using the
   ``--output`` option. These output must exist after the execution of the
   ``renku run`` command. However, they do not need to be modified by
   the command.
   You can specify ``--output name=path`` or just `--output path``, the former
   of which would also set the name of the output on the resulting Plan.

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

To address this issue, the program can dump a list of input and output files
that it is accessing in ``inputs.txt`` and ``outputs.txt``. Each line in these
files is expected to be the path to an input or output file within the
project's directory. When the program is finished, Renku will look for
existence of these two files and adds their content to the list of explicit
inputs and outputs. Renku will then delete these two files.

By default, Renku looks for these two files in ``.renku/tmp`` directory. One
can change this default location by setting ``RENKU_INDIRECT_PATH``
environment variable. When set, it points to a sub-directory within the
``.renku/tmp`` directory where ``inputs.txt`` and ``outputs.txt`` reside.

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
"""

import click

from renku.cli.utils.callback import ClickCallback
from renku.core.commands.options import option_isolation


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
@option_isolation
@click.argument("command_line", nargs=-1, required=True, type=click.UNPROCESSED)
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
    command_line,
):
    """Tracking work on a specific problem."""
    from renku.core.commands.run import run_command

    communicator = ClickCallback()
    command = run_command()

    if isolation:
        command = command.with_git_isolation()

    command.with_communicator(communicator).build().execute(
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
    )
