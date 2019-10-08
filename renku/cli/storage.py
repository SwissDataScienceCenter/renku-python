# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Manage an external storage."""

import click

from renku.core.commands.client import pass_local_client


@click.group()
def storage():
    """Manage an external storage."""


@storage.command()
@click.argument(
    'paths',
    type=click.Path(exists=True, dir_okay=True),
    nargs=-1,
    required=True,
)
@pass_local_client
def pull(client, paths):
    """Pull the specified paths from external storage."""
    client.pull_paths_from_storage(*paths)
