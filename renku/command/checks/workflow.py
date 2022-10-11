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
"""Checks needed to determine integrity of workflows."""

from typing import Optional, Tuple

from renku.command.util import WARNING
from renku.core.util import communication
from renku.domain_model.project_context import project_context
from renku.infrastructure.gateway.activity_gateway import reindex_catalog


def check_activity_catalog(fix, force, **_) -> Tuple[bool, Optional[str]]:
    """Check if the activity-catalog needs to be rebuilt.

    Args:
        fix: Whether to fix found issues.
        force: Whether to force rebuild the activity catalog.
        _: keyword arguments.

    Returns:
        Tuple of whether the activity-catalog needs to be rebuilt and a string of found problems.
    """
    database = project_context.database
    activity_catalog = database["activity-catalog"]
    relations = database["_downstream_relations"]

    # NOTE: If len(activity_catalog) > 0 then either the project is fixed or it used a fixed Renku version but still has
    # broken metadata. ``force`` allows to rebuild the metadata in the latter case.
    if (len(relations) == 0 or len(activity_catalog) > 0) and not (force and fix):
        return True, None

    if not fix:
        problems = (
            WARNING + "The project's workflow metadata needs to be rebuilt (use 'renku doctor --fix' to rebuild it).\n"
        )

        return False, problems

    with communication.busy("Rebuilding workflow metadata ..."):
        reindex_catalog(database=database)

    communication.info("Workflow metadata was rebuilt")

    return True, None
