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
"""Track provenance of data created by executing programs.

Capture command line execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tracking execution of your command line script is done by simply adding the
``renku run`` command before the actual command. This will enable detection of:

* arguments (flags),
* string and integer options,
* input files or directories if linked to existing paths in the repository,
* output files or directories if modified or created while running the command.

.. note:: If there were uncommitted changes in the repository, then the
   ``renku run`` command fails. See :program:`git status` for details.

.. warning:: Input and output paths can only be detected if they are passed as
   arguments to ``renku run``.

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
   that don't appear on the command line. These input file must exist before
   execution of ``renku run`` command. This option is not a replacement for
   the arguments that are passed on the command line. Files or directories
   specified with this option will not be passed as input arguments to the
   script.

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

.. topic:: Specifying outputs explicitly (``--output``)

   You can specify expected outputs of your program explicitly by using the
   ``--output`` option. These output must exist after the execution of the
   ``renku run`` command. However, they do not need to be modified by
   the command.

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
can change this default location by setting ``RENKU_FILELIST_PATH``
environment variable. When set, it points to the directory within the
project's directory where ``inputs.txt`` and ``outputs.txt`` reside.

Exit codes
~~~~~~~~~~

All Unix commands return a number between 0 and 255 which is called
"exit code". In case other numbers are returned, they are treaded module 256
(-10 is equivalent to 246, 257 is equivalent to 1). The exit-code 0 represents
a *success* and non-zero exit-code indicates a *failure*.

Therefore the command specified after ``renku run`` is expected to return
exit-code 0. If the command returns different exit code, you can specify them
with ``--success-code=<INT>`` parameter.

.. code-block:: console

   $ renku run --success-code=1 --no-output fail

"""

import os
import sys
from subprocess import call

import click

from renku.core import errors
from renku.core.commands.client import pass_local_client
from renku.core.commands.options import option_isolation
from renku.core.management.git import _mapped_std_streams
from renku.core.models.cwl.command_line_tool import CommandLineToolFactory


@click.command(context_settings=dict(ignore_unknown_options=True, ))
@click.option(
    'inputs',
    '--input',
    multiple=True,
    help='Force a path to be considered as an input.',
)
@click.option(
    'outputs',
    '--output',
    multiple=True,
    help='Force a path to be considered an output.',
)
@click.option(
    '--no-output',
    is_flag=True,
    default=False,
    help='Allow command without output files.',
)
@click.option(
    '--success-code',
    'success_codes',
    type=int,
    multiple=True,
    callback=lambda _, __, values: [int(value) % 256 for value in values],
    help='Allowed command exit-code.',
)
@option_isolation
@click.argument('command_line', nargs=-1, type=click.UNPROCESSED)
@pass_local_client(
    clean=True,
    up_to_date=True,
    commit=True,
    ignore_std_streams=True,
)
def run(
    client, inputs, outputs, no_output, success_codes, isolation, command_line
):
    """Tracking work on a specific problem."""
    working_dir = client.repo.working_dir
    mapped_std = _mapped_std_streams(client.candidate_paths)
    factory = CommandLineToolFactory(
        command_line=command_line,
        explicit_inputs=inputs,
        explicit_outputs=outputs,
        directory=os.getcwd(),
        working_dir=working_dir,
        successCodes=success_codes,
        **{
            name: os.path.relpath(path, working_dir)
            for name, path in mapped_std.items()
        }
    )
    with client.with_workflow_storage() as wf:
        with factory.watch(client, no_output=no_output) as tool:
            # Don't compute paths if storage is disabled.
            if client.has_external_storage:
                # Make sure all inputs are pulled from a storage.
                paths_ = (
                    path
                    for _, path in tool.iter_input_files(client.workflow_path)
                )
                client.pull_paths_from_storage(*paths_)

            return_code = call(
                factory.command_line,
                cwd=os.getcwd(),
                **{key: getattr(sys, key)
                   for key in mapped_std.keys()},
            )

            if return_code not in (success_codes or {0}):
                raise errors.InvalidSuccessCode(
                    return_code, success_codes=success_codes
                )

            sys.stdout.flush()
            sys.stderr.flush()

            wf.add_step(run=tool)
