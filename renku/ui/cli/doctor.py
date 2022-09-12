# -*- coding: utf-8 -*-
#
# Copyright 2020-2022 - Swiss Data Science Center (SDSC)
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
"""Check your system and repository for potential problems.

.. cheatsheet::
   :group: Misc
   :command: $ renku doctor
   :description: Check your system and repository for potential problems.
   :target: rp

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.doctor:doctor
   :prog: renku doctor
   :nested: full
"""

import textwrap

import click

from renku.core import errors


@click.command()
@click.pass_context
@click.option("--fix", is_flag=True, help="Fix issues when possible.")
@click.option("-f", "--force", is_flag=True, help="Do possible fixes even though no problem is reported.")
def doctor(ctx, fix, force):
    """Check your system and repository for potential problems."""
    import renku.ui.cli.utils.color as color
    from renku.command.doctor import DOCTOR_INFO, doctor_check_command
    from renku.ui.cli.utils.callback import ClickCallback

    if force and not fix:
        raise errors.ParameterError("Cannot use '-f/--force' without '--fix'")

    click.secho("\n".join(textwrap.wrap(DOCTOR_INFO)) + "\n", bold=True)

    communicator = ClickCallback()
    command = doctor_check_command(with_fix=fix)
    if fix:
        command = command.with_communicator(communicator)
    is_ok, problems = command.build().execute(fix=fix, force=force).output

    if is_ok:
        click.secho("Everything seems to be ok.", fg=color.GREEN)
        ctx.exit(0)

    click.echo(problems)
    ctx.exit(1)
