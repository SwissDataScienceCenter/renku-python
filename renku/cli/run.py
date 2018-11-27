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

Detecting output paths
~~~~~~~~~~~~~~~~~~~~~~

Any path **modified** or **created** during the execution will be added as an
output.

Because the output path detection is based on the Git repository state after
the execution of ``renku run`` command, it is good to have a basic understading
of the underlying principles and limitations of tracking files in Git.

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

Exit codes
~~~~~~~~~~

All Unix commands return a number between 0 and 255 which is called
"exit code". In case other numbers are returned, they are treaded module 256
(-10 is equivalent to 246, 257 is equivalent to 1). The exit-code 0 represents
a *success* and non-zero exit-code indicates a *failure*.

Therefore the command speficied after ``renku run`` is expected to return
exit-code 0. If the command returns different exit code, you can speficy them
with ``--success-code=<INT>`` parameter.

.. code-block:: console

   $ renku run --success-code=1 --no-output fail

"""

import os
import sys
from subprocess import call

import click

from renku import errors
from renku.api._git import _mapped_std_streams
from renku.models.cwl.command_line_tool import CommandLineToolFactory

from ._client import pass_local_client
from ._git import set_git_isolation


@click.command(context_settings=dict(ignore_unknown_options=True, ))
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
@click.option(
    '--isolation',
    is_flag=True,
    default=False,
    callback=lambda ctx, param, value: set_git_isolation(value),
    help='Set up the isolation for invoking of the given command.',
)
@click.argument('command_line', nargs=-1, type=click.UNPROCESSED)
@pass_local_client(
    clean=True, up_to_date=True, commit=True, ignore_std_streams=True
)
def run(client, no_output, success_codes, isolation, command_line):
    """Tracking work on a specific problem."""
    working_dir = client.repo.working_dir
    mapped_std = _mapped_std_streams(client.candidate_paths)
    factory = CommandLineToolFactory(
        command_line=command_line,
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
            returncode = call(
                factory.command_line,
                cwd=os.getcwd(),
                **{key: getattr(sys, key)
                   for key in mapped_std.keys()},
            )

            if returncode not in (success_codes or {0}):
                raise errors.InvalidSuccessCode(
                    returncode, success_codes=success_codes
                )

            sys.stdout.flush()
            sys.stderr.flush()

            wf.add_step(run=tool)
