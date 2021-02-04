# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku storage command."""

from renku.core.incubation.command import Command


def _check_lfs(client, everything=False):
    """Check if large files are not in lfs."""
    return client.check_lfs_migrate_info(everything)


def check_lfs():
    """Check lfs command."""
    return Command().command(_check_lfs)


def _fix_lfs(client, paths):
    """Migrate large files into lfs."""
    client.migrate_files_to_lfs(paths)


def fix_lfs():
    """Fix lfs command."""
    return Command().command(_fix_lfs).require_clean().with_commit(commit_if_empty=False)
