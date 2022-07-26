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

import itertools

import click

from renku.command.command_builder import inject
from renku.command.echo import WARNING
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.util import communication


@inject.autoparams("activity_gateway", "database_dispatcher")
def check_migrated_activity_ids(
    client, fix, activity_gateway: IActivityGateway, database_dispatcher: IDatabaseDispatcher, **kwargs
):
    """Check that activity ids were correctly migrated in the past."""
    activities = activity_gateway.get_all_activities(include_deleted=True)

    wrong_activities = [a for a in activities if not a.id.startswith("/activities/")]

    if fix:
        current_database = database_dispatcher.current_database
        for activity in wrong_activities:
            communication.info(f"Fixing activity '{activity.id}'")

            activity_gateway.remove(activity, keep_reference=False)

            # NOTE: Modify id on activity and children
            activity.unfreeze()
            activity.id = f"/activities/{activity.id}"
            activity._p_oid = current_database.hash_id(activity.id)
            activity.freeze()

            for attribute in itertools.chain(activity.usages, activity.generations, activity.parameters):
                object.__setattr__(attribute, "id", f"/activities/{attribute.id}")  # type: ignore

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
