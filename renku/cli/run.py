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
"""Track provenance of data created by executing programs."""

import os
import sys
from subprocess import call

import click

from renku.models.cwl.command_line_tool import CommandLineToolFactory

from ._client import pass_local_client
from ._git import _mapped_std_streams, with_git


@click.command(context_settings=dict(ignore_unknown_options=True, ))
@click.option(
    '--no-output',
    is_flag=True,
    default=False,
    help='Allow commands without output files.'
)
@click.argument('command_line', nargs=-1, type=click.UNPROCESSED)
@pass_local_client
@with_git(clean=True, up_to_date=True, commit=True, ignore_std_streams=True)
def run(client, no_output, command_line):
    """Tracking work on a specific problem."""
    candidates = [x[0] for x in client.git.index.entries] + \
        client.git.untracked_files
    mapped_std = _mapped_std_streams(candidates)
    factory = CommandLineToolFactory(command_line=command_line, **mapped_std)

    with client.with_workflow_storage() as wf:
        with factory.watch(client, no_output=no_output) as tool:
            call(
                factory.command_line,
                cwd=os.getcwd(),
                **{key: getattr(sys, key)
                   for key in mapped_std.keys()},
            )

            sys.stdout.flush()
            sys.stderr.flush()

            wf.add_step(run=tool)
