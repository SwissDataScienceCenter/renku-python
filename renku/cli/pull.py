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
"""Pull latest data from server."""

import click


@click.group()
def pull():
    """Pull latest data from server."""


@pull.command()
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=True),
    nargs=-1,
    required=True,
)
@click.pass_context
def path(ctx, paths):
    """DEPRECATED: use 'renku storage pull'."""
    click.secho('Use "renku storage pull" instead.', fg='red', err=True)
    ctx.exit(2)
