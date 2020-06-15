# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Migrate project to the latest Renku version."""

from renku.core.management.migrate import migrate

from .client import pass_local_client


@pass_local_client(clean=True, commit=True, commit_empty=False)
def migrate_project(client, progress_callback=None, commit_message=None):
    """Migrate all project's entities."""
    return migrate(client=client, progress_callback=progress_callback)


@pass_local_client(clean=True, commit=False)
def migrate_project_no_commit(client, progress_callback=None):
    """Migrate all project's entities but do not commit changes."""
    return migrate(client=client, progress_callback=progress_callback)
