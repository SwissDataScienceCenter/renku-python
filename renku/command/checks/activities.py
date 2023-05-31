# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
from renku.command.util import WARNING
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.util import communication
from renku.domain_model.project_context import project_context


@inject.autoparams("activity_gateway")
def check_migrated_activity_ids(fix, activity_gateway: IActivityGateway, **_):
    """Check that activity ids were correctly migrated in the past."""
    activities = activity_gateway.get_all_activities(include_deleted=True)

    wrong_activities = [a for a in activities if not a.id.startswith("/activities/")]

    if fix:
        current_database = project_context.database
        for activity in wrong_activities:
            communication.info(f"Fixing activity '{activity.id}'")

            activity_gateway.remove(activity, keep_reference=False)

            # NOTE: Modify id on activity and children
            activity.unfreeze()
            activity.id = f"/activities/{activity.id}"
            activity._p_oid = current_database.hash_id(activity.id)
            activity.freeze()

            for attribute in itertools.chain(
                activity.usages, activity.hidden_usages, activity.generations, activity.parameters
            ):
                object.__setattr__(attribute, "id", f"/activities/{attribute.id}")  # type: ignore

            activity.association.id = f"/activities/{activity.association.id}"

            activity_gateway.add(activity)

        wrong_activities = []

    if not wrong_activities:
        return True, False, None

    problems = (
        WARNING
        + "There are invalid activity ids in the project (use 'renku doctor --fix' to fix them):"
        + "\n\n\t"
        + "\n\t".join(click.style(a.id, fg="yellow") for a in wrong_activities)
        + "\n"
    )

    return False, True, problems


@inject.autoparams("activity_gateway")
def check_activity_dates(fix, activity_gateway: IActivityGateway, **_):
    """Check activities have correct start/end/delete dates.

    Args:
        fix(bool): Whether to fix found issues.
        activity_gateway(IActivityGateway): Injected ActivityGateway.
        _: keyword arguments.

    Returns:
        Tuple[bool, Optional[str]]: Tuple of whether there are activities with invalid dates, if they can be
            automatically fixed and a string of the problem.
    """
    invalid_activities = []

    for activity in activity_gateway.get_all_activities(include_deleted=True):
        plan = activity.association.plan
        if (
            activity.started_at_time < plan.date_created
            or activity.ended_at_time < activity.started_at_time
            or (activity.invalidated_at and activity.invalidated_at < activity.ended_at_time)
        ):
            invalid_activities.append(activity)

    if not invalid_activities:
        return True, False, None
    if not fix:
        ids = [a.id for a in invalid_activities]
        message = (
            WARNING
            + "The following activity have incorrect start, end, or delete date (use 'renku doctor --fix' to fix them):"
            + "\n\t"
            + "\n\t".join(ids)
        )
        return False, True, message

    fix_activity_dates(activities=invalid_activities)
    project_context.database.commit()
    communication.info("Activity dates were fixed")

    return True, False, None


def fix_activity_dates(activities):
    """Fix activities' start/end/delete dates."""
    for activity in activities:
        plan = activity.association.plan
        activity.unfreeze()
        if activity.started_at_time < plan.date_created:
            activity.started_at_time = plan.date_created

        if activity.ended_at_time < activity.started_at_time:
            activity.ended_at_time = activity.started_at_time

        if activity.invalidated_at and activity.invalidated_at < activity.ended_at_time:
            activity.invalidated_at = activity.ended_at_time
        activity.freeze()
