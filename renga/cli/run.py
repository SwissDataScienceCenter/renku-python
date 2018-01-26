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
from werkzeug.utils import secure_filename

from ._git import _dirty_paths, _mapped_std_streams, _safe_issue_checkout, \
    with_git
from ._repo import pass_repo


@click.command(context_settings=dict(ignore_unknown_options=True, ))
@click.argument('cmd_args', nargs=-1, type=click.UNPROCESSED)
@pass_repo
@with_git(clean=True, up_to_date=True, commit=True, ignore_std_streams=True)
def run(repo, cmd_args):
    """Activate environment for tracking work on a specific problem."""
    click.echo('Command: ' + str(cmd_args[0]))
    click.echo('Args: ' + str(cmd_args[1:]))
    click.echo('Mapped streams: ' + str(
        _mapped_std_streams(
            [x[0]
             for x in repo.git.index.entries] + repo.git.untracked_files)))

    call(cmd_args, cwd=os.getcwd())

    outputs = repo.git.untracked_files
    outputs += [item.a_path for item in repo.git.index.diff(None)]

    click.echo('Outputs' + str(outputs))
