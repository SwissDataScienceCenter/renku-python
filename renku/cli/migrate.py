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
"""Migrate files and metadata to the latest Renku version.

Datasets
~~~~~~~~

The location of dataset metadata files has been changed from the
``data/<name>/metadata.yml`` to ``.renku/datasets/<UUID>/metadata.yml``.
All file paths inside a metadata file are relative to itself and the
``renku migrate datasets`` command will take care of it.
"""

import click

from renku.cli._checks.migrate_datasets import STRUCTURE_MIGRATIONS

from ._client import pass_local_client


@click.group()
def migrate():
    """Migrate to latest Renku version."""


@migrate.command()
@pass_local_client(
    clean=True,
    commit=True,
    allow_empty=False,
)
@click.pass_context
def datasets(ctx, client):
    """Migrate dataset metadata."""
    results = [
        migration(client) is not False for migration in STRUCTURE_MIGRATIONS
    ]

    if all(results) and client.repo.index.diff(None):
        click.secho('OK', fg='green')
