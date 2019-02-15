# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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

from ._client import pass_local_client

DOCTOR_INFO = """\
Please note that the diagnosis report is used to help Renku maintainers with
debugging if you file an issue. Use all proposed solutions with maximal care
and if in doubt ask an expert around or file an issue. Thanks!
"""


@click.command()
@pass_local_client
@click.pass_context
def doctor(ctx, client):
    """Check your system and repository for potential problems."""
    click.secho('\n'.join(textwrap.wrap(DOCTOR_INFO)) + '\n', bold=True)

    from . import _checks

    is_ok = True
    for attr in _checks.__all__:
        is_ok &= getattr(_checks, attr)(client)

    if is_ok:
        click.secho('Everything seems to be ok.', fg='green')

    ctx.exit(0 if is_ok else 1)
