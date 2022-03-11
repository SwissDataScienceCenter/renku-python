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
"""Checks needed to determine integrity of datasets."""

from itertools import chain

import click

from renku.core.commands.echo import WARNING
from renku.core.management.command_builder import inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.utils import communication


@inject.autoparams()
def check_migrated_activity_ids(
    client, fix, activity_gateway: IActivityGateway, database_dispatcher: IDatabaseDispatcher
):
    """Check that activity ids were correctly migrated in the past."""
    activities = activity_gateway.get_all_activities()

    wrong_activities = [a for a in activities if not a.id.startswith("/activities/")]

    if fix:
        current_database = database_dispatcher.current_database
        for activity in wrong_activities:
            communication.info(f"Fixing activity '{activity.id}'")

            old_id = activity.id

            # NOTE: Remove activity relations
            tok = current_database["activity-catalog"].tokenizeQuery
            relations = chain(
                list(current_database["activity-catalog"].findRelationChains(tok(downstream=activity))),
                list(current_database["activity-catalog"].findRelationChains(tok(upstream=activity))),
            )
            for rel_collection in relations:
                for r in list(rel_collection):
                    current_database["activity-catalog"].unindex(r)

            current_database["activities"].pop(old_id)

            # NOTE: Modify id on activity and children
            activity.unfreeze()
            activity.id = f"/activities/{activity.id}"
            activity._p_oid = current_database.hash_id(activity.id)
            activity.freeze()

            for usage in activity.usages:
                current_database["activities-by-usage"][usage.entity.path] = [
                    a for a in current_database["activities-by-usage"][usage.entity.path] if a != activity
                ]
                object.__setattr__(usage, "id", f"/activities/{usage.id}")

            for generation in activity.generations:
                current_database["activities-by-generation"][generation.entity.path] = [
                    a for a in current_database["activities-by-generation"][generation.entity.path] if a != activity
                ]
                object.__setattr__(generation, "id", f"/activities/{generation.id}")

            for parameter in activity.parameters:
                object.__setattr__(parameter, "id", f"/activities/{parameter.id}")

            activity.association.id = f"/activities/{activity.association.id}"

            activity_gateway.add(activity)

        wrong_activities = []

    if not wrong_activities:
        return True, None

    problems = (
        WARNING
        + "There are invalid activity ids in the project (use 'renku doctor --fix' to fix them):"
        + "\n\n\t"
        + "\n\t".join(click.style(a.id, fg="yellow") for a in wrong_activities)
        + "\n"
    )

    return False, problems
