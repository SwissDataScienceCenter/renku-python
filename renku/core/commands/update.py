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
from typing import List, Optional, Set, Tuple

from renku.core import errors
from renku.core.commands.workflow import execute_workflow
from renku.core.errors import ParameterError
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.workflow.activity import sort_activities
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.models.provenance.activity import Activity
from renku.core.utils.metadata import add_activity_if_recent, filter_overridden_activities, get_modified_activities
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
def _update(
    update_all,
    dry_run,
    client_dispatcher: IClientDispatcher,
    activity_gateway: IActivityGateway,
    provider: str,
    config: Optional[str],
    paths=None,
):
    if not paths and not update_all and not dry_run:
        raise ParameterError("Either PATHS, --all/-a, or --dry-run/-n should be specified.")
    if paths and update_all:
        raise ParameterError("Cannot use PATHS and --all/-a at the same time.")

    client = client_dispatcher.current_client

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    modified_activities, modified_paths = _get_modified_activities_and_paths(client.repository, activity_gateway)
    activities = _get_downstream_activities(modified_activities, activity_gateway, paths)

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    # NOTE: When updating we only want to eliminate activities that are overridden, not their parents
    activities = sort_activities(activities, remove_overridden_parents=False)
    if dry_run:
        return activities, modified_paths

    graph = ExecutionGraph([a.plan_with_values for a in activities], virtual_links=True)
    execute_workflow(dag=graph.workflow_graph, command_name="update", provider=provider, config=config)


@inject.autoparams()
def _is_activity_valid(activity: Activity, plan_gateway: IPlanGateway, client_dispatcher: IClientDispatcher) -> bool:
    """Return whether this plan has been deleted."""
    client = client_dispatcher.current_client

    for usage in activity.usages:
        if not (client.path / usage.entity.path).exists():
            return False

    plan = activity.association.plan

    if plan.invalidated_at is not None:
        return False

    # get newest with same name
    newest_plan = plan_gateway.get_by_name(plan.name)

    if newest_plan.invalidated_at is not None:
        return False

    all_plans = plan_gateway.get_all_plans()

    derived = plan
    while derived:
        plan = derived
        derived = next((p for p in all_plans if p.derived_from and p.derived_from == plan.id), None)

    return plan.invalidated_at is None


def _get_modified_activities_and_paths(repository, activity_gateway) -> Tuple[Set[Activity], Set[str]]:
    """Return latest activities that one of their inputs is modified."""
    all_activities = activity_gateway.get_all_activities()
    relevant_activities = filter_overridden_activities(all_activities)
    modified, _ = get_modified_activities(activities=list(relevant_activities), repository=repository)
    return {a for a, _ in modified if _is_activity_valid(a)}, {e.path for _, e in modified}


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
                if not _is_activity_valid(activity):
                    # don't process further downstream activities as the plan in question was deleted
                    break
                include_newest_activity(activity)

    return {a for activities in all_activities.values() for a in activities}
