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

import uuid
from collections import defaultdict
from pathlib import Path
from typing import List, Set

from git import Actor

from renku.core import errors
from renku.core.errors import ParameterError
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.workflow.plan_factory import delete_indirect_files_list
from renku.core.models.provenance.activity import Activity, ActivityCollection
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import Plan
from renku.core.plugins.provider import execute
from renku.core.utils.datetime8601 import local_now
from renku.core.utils.git import add_to_git
from renku.core.utils.metadata import add_activity_if_recent, get_modified_activities
from renku.core.utils.os import get_relative_paths
from renku.version import __version__, version_url


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
def _update(update_all, client_dispatcher: IClientDispatcher, activity_gateway: IActivityGateway, paths=None):
    if not paths and not update_all:
        raise ParameterError("Either PATHS or --all/-a should be specified.")
    if paths and update_all:
        raise ParameterError("Cannot use PATHS and --all/-a at the same time.")

    client = client_dispatcher.current_client

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    modified_activities = _get_modified_activities(client.repo, activity_gateway)
    activities = _get_downstream_activities(modified_activities, activity_gateway, paths)

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    plans = [a.plan_with_values for a in activities]

    execute_workflow(plans=plans, command_name="update")


def _get_modified_activities(repo, activity_gateway) -> Set[Activity]:
    """Return latest activities that one of their inputs is modified."""
    latest_activities = activity_gateway.get_latest_activity_per_plan().values()
    modified, _ = get_modified_activities(activities=latest_activities, repo=repo)

    return {a for a, _ in modified}


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


@inject.autoparams()
def execute_workflow(
    plans: List[Plan],
    command_name,
    client_dispatcher: IClientDispatcher,
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
    provider="cwltool",
    config=None,
):
    """Execute a Run with/without subprocesses."""
    client = client_dispatcher.current_client

    # NOTE: Pull inputs from Git LFS or other storage backends
    if client.check_external_storage():
        inputs = [i.actual_value for p in plans for i in p.inputs]
        client.pull_paths_from_storage(*inputs)

    delete_indirect_files_list(client.path)

    started_at_time = local_now()

    # NOTE: Create a ``CompositePlan`` because ``workflow_covert`` expects it
    workflow = CompositePlan(id=CompositePlan.generate_id(), plans=plans, name=f"plan-collection-{uuid.uuid4().hex}")
    modified_outputs = execute(workflow=workflow, basedir=client.path, provider=provider, config=config)

    ended_at_time = local_now()

    add_to_git(client.repo.git, *modified_outputs)

    if client.repo.is_dirty():
        postfix = "s" if len(modified_outputs) > 1 else ""
        commit_msg = f"renku {command_name}: committing {len(modified_outputs)} modified file{postfix}"
        committer = Actor(f"renku {__version__}", version_url)
        client.repo.index.commit(commit_msg, committer=committer, skip_hooks=True)

    activities = []

    for plan in plans:
        # NOTE: Update plans are copies of Plan objects. We need to use the original Plan objects to avoid duplicates.
        original_plan = plan_gateway.get_by_id(plan.id)
        activity = Activity.from_plan(plan=original_plan, started_at_time=started_at_time, ended_at_time=ended_at_time)
        activity_gateway.add(activity)
        activities.append(activity)

    if len(activities) > 1:
        activity_collection = ActivityCollection(activities=activities)
        activity_gateway.add_activity_collection(activity_collection)
