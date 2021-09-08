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
"""Renku ``rerun`` command."""
from collections import defaultdict
from typing import List

from renku.core.commands.update import execute_workflow
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication
from renku.core.utils.os import get_relative_paths


def rerun_command():
    """Recreate files generated by a sequence of ``run`` commands."""
    return (
        Command()
        .command(_rerun)
        .require_migration()
        .require_clean()
        .require_nodejs()
        .with_database(write=True)
        .with_commit()
    )


@inject.autoparams()
def _rerun(
    sources: List[str], paths: List[str], client_dispatcher: IClientDispatcher, activity_gateway: IActivityGateway
):
    client = client_dispatcher.current_client

    sources = sources or []
    sources = get_relative_paths(base=client.path, paths=sources)
    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    activities = _get_activities(paths, sources, activity_gateway)

    if not activities:
        exit(1)

    plans = [a.plan_with_values for a in activities]

    execute_workflow(plans=plans, command_name="rerun")


def _get_activities(paths: List[str], sources: List[str], activity_gateway: IActivityGateway):
    all_activities = defaultdict(set)

    def include_newest_activity(activity):
        existing_activities = all_activities[activity.association.plan.id]

        if activity in existing_activities:
            return

        for existing_activity in existing_activities:
            if activity.has_identical_inputs_and_outputs_as(existing_activity):
                if activity.ended_at_time > existing_activity.ended_at_time:  # activity is newer
                    existing_activities.remove(existing_activity)
                    existing_activities.add(activity)
                return

        # No similar activity was found
        existing_activities.add(activity)

    for path in paths:
        activities = activity_gateway.get_activities_by_generation(path)

        if len(activities) == 0:
            communication.warn(f"Path '{path}' is not generated by any workflows.")
            continue

        latest_activity = max(activities, key=lambda a: a.ended_at_time)

        upstream_chains = activity_gateway.get_upstream_activity_chains(latest_activity)

        if sources:
            # NOTE: Add the activity to check if it also matches the condition
            upstream_chains.append((latest_activity,))
            # NOTE: Only include paths that is using at least one of the sources
            upstream_chains = [c for c in upstream_chains if any(u.entity.path in sources for u in c[0].usages)]

            # NOTE: Include activity only if any of its upstream match the condition
            if upstream_chains:
                include_newest_activity(latest_activity)
        else:
            include_newest_activity(latest_activity)

        for chain in upstream_chains:
            for activity in chain:
                include_newest_activity(activity)

    return {a for activities in all_activities.values() for a in activities}
