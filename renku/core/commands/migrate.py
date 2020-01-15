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
"""Migrate files and metadata to the latest Renku version."""
from renku.core.commands.checks.migration import STRUCTURE_MIGRATIONS

from .client import pass_local_client


@pass_local_client(
    clean=True, commit=True, commit_empty=False, raise_if_empty=True
)
def migrate_datasets(
    client,
    commit_message=None,
):
    """Migrate dataset metadata."""
    results = [
        migration(client) is not False for migration in STRUCTURE_MIGRATIONS
    ]

    if all(results) and client.repo.index.diff(None):
        return results
