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
"""Warn if migration is required."""

from renku.command.util import ERROR, WARNING
from renku.core.migration.migrate import is_migration_required, is_project_unsupported


def check_migration(**_):
    """Check for project version.

    Args:
        _: keyword arguments.

    Returns:
        Tuple of whether project metadata is up to date and string of found problems.
    """
    if is_migration_required():
        problems = WARNING + "Project requires migration.\n" + '  (use "renku migrate" to fix this issue)\n'
    elif is_project_unsupported():
        problems = (
            ERROR + "Project version is not supported by your version of Renku.\n" + "  (upgrade your Renku version)\n"
        )
    else:
        return True, None

    return False, problems
