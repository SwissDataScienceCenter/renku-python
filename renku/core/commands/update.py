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
"""Renku ``update`` command."""

from collections import defaultdict
from pathlib import Path
from typing import List, Set, Tuple

from renku.core import errors
from renku.core.commands.workflow import execute_workflow
from renku.core.errors import ParameterError
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.workflow import sort_activities
from renku.core.models.provenance.activity import Activity
from renku.core.utils.metadata import add_activity_if_recent, get_modified_activities
from renku.core.utils.os import get_relative_paths


def update_command():
    """Update existing files by rerunning their outdated workflow."""
    return (
        Command()
        .command(_update)
        .require_migration()
        .require_clean()
        .require_nodejs()
        .with_database(write=True)
        .with_commit()
    )


@inject.autoparams()
def _update(update_all, dry_run, client_dispatcher: IClientDispatcher, activity_gateway: IActivityGateway, paths=None):
    if not paths and not update_all and not dry_run:
        raise ParameterError("Either PATHS, --all/-a, or --dry-run/-n should be specified.")
    if paths and update_all:
        raise ParameterError("Cannot use PATHS and --all/-a at the same time.")

    client = client_dispatcher.current_client

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    modified_activities, modified_paths = _get_modified_activities_and_paths(client.repo, activity_gateway)
    activities = _get_downstream_activities(modified_activities, activity_gateway, paths)

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    # NOTE: When updating we only want to eliminate activities that are overridden, not their parents
    activities = sort_activities(activities, remove_overridden_parents=False)
    if dry_run:
        return activities, modified_paths

    plans = [a.plan_with_values for a in activities]

    execute_workflow(plans=plans, command_name="update")


def _get_modified_activities_and_paths(repo, activity_gateway) -> Tuple[Set[Activity], Set[str]]:
    """Return latest activities that one of their inputs is modified."""
    latest_activities = activity_gateway.get_latest_activity_per_plan().values()
    modified, _ = get_modified_activities(activities=latest_activities, repo=repo)

    return {a for a, _ in modified}, {e.path for _, e in modified}


def _get_downstream_activities(
    starting_activities: Set[Activity], activity_gateway: IActivityGateway, paths: List[str]
) -> Set[Activity]:
    """Return an ordered list of activities so that an activities comes before all its downstream activities."""
    all_activities = defaultdict(set)

    def include_newest_activity(activity):
        existing_activities = all_activities[activity.association.plan.id]
        add_activity_if_recent(activity=activity, activities=existing_activities)

    def does_activity_generate_any_paths(activity):
        is_same = any(g.entity.path in paths for g in activity.generations)
        is_parent = any(Path(p) in Path(g.entity.path).parents for p in paths for g in activity.generations)

        return is_same or is_parent

    for activity in starting_activities:
        downstream_chains = activity_gateway.get_downstream_activity_chains(activity)

        if paths:
            # NOTE: Add the activity to check if it also matches the condition
            downstream_chains.append((activity,))
            downstream_chains = [c for c in downstream_chains if does_activity_generate_any_paths(c[-1])]

            # NOTE: Include activity only if any of its downstream match the condition
            if downstream_chains:
                include_newest_activity(activity)
        else:
            include_newest_activity(activity)

        for chain in downstream_chains:
            for activity in chain:
                include_newest_activity(activity)

    return {a for activities in all_activities.values() for a in activities}
