# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Serializers for activities."""

from datetime import datetime
from typing import List, NamedTuple, Set

from renku.command.format.tabulate import tabulate
from renku.core.util.os import are_paths_related
from renku.domain_model.provenance.activity import Activity


def tabulate_activities(activities: List[Activity], modified_inputs: Set[str]):
    """Return some info about the activities in a tabular form.

    Args:
        activities(List[Activity]): Activities to convert.
        modified_inputs(Set[str]): Set of modified inputs for activities.

    Returns:
        String of activities in tabular representation.
    """
    collection = []
    fields = "plan,execution_date,modified_inputs,outputs,command"
    ActivityDisplay = NamedTuple(
        "ActivityDisplay",
        [("plan", str), ("execution_date", datetime), ("modified_inputs", str), ("outputs", str), ("command", str)],
    )

    for activity in activities:
        modified_usages = {
            u.entity.path for u in activity.usages if any(are_paths_related(u.entity.path, m) for m in modified_inputs)
        }
        generations = {g.entity.path for g in activity.generations}
        modified_inputs |= generations
        plan = activity.association.plan.name

        collection.append(
            ActivityDisplay(
                plan,
                activity.ended_at_time,
                ", ".join(sorted(modified_usages)),
                ", ".join(sorted(generations)),
                " ".join(activity.plan_with_values.to_argv(with_streams=True)),
            )
        )

    return tabulate(collection=collection, columns=fields, columns_mapping=ACTIVITY_DISPLAY_COLUMNS, sort=False)


ACTIVITY_DISPLAY_COLUMNS = {
    "plan": ("plan", None),
    "execution_date": ("execution_date", "date executed"),
    "modified_inputs": ("modified_inputs", "modified inputs"),
    "outputs": ("outputs", None),
    "command": ("command", None),
}
