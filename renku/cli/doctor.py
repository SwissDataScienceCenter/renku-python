# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Check your system and repository for potential problems."""
import textwrap

import click

from renku.core.commands.doctor import DOCTOR_INFO, doctor_check


@click.command()
@click.pass_context
def doctor(ctx):
    """Check your system and repository for potential problems."""
    click.secho('\n'.join(textwrap.wrap(DOCTOR_INFO)) + '\n', bold=True)
    is_ok, problems = doctor_check()

    if is_ok:
        click.secho('Everything seems to be ok.', fg='green')
        ctx.exit(0)

    click.secho(problems)
    ctx.exit(1)
