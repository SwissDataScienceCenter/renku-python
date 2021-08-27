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
from typing import Generator, List, Union

import networkx
from git import Actor

from renku.core.errors import ParameterError
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.workflow.plan_factory import delete_indirect_files_list
from renku.core.models.provenance.activity import Activity
from renku.core.models.workflow.composite_plan import CompositePlan, PlanCollection
from renku.core.models.workflow.plan import Plan
from renku.core.utils.datetime8601 import local_now
from renku.core.utils.git import add_to_git, get_modified_entities
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

    modified_activities = _get_modified_activities(client, activity_gateway)
    ordered_activities = _get_ordered_downstream_activities(modified_activities, activity_gateway, paths)

    update_plan = _create_plan_from_activity_list(ordered_activities)

    execute_workflow(workflow=update_plan, command_name="update")


def _get_modified_activities(client, activity_gateway) -> Generator[Activity, None, None]:
    """Return latest activities that one of their inputs is modified."""
    latest_activities = activity_gateway.get_latest_activity_per_plan().values()

    used_entities = (u.entity for a in latest_activities for u in a.usages)
    modified, _ = get_modified_entities(entities=used_entities, repo=client.repo)
    return (a for a in latest_activities if any(u.entity in modified for u in a.usages))


def _get_ordered_downstream_activities(
    starting_activities: Generator[Activity, None, None], activity_gateway: IActivityGateway, paths: List[str]
):
    """Return an ordered list of activities so that an activities comes before all its downstream activities."""
    graph = networkx.DiGraph()

    activities = set(starting_activities)
    while activities:
        activity = activities.pop()
        child_activities = activity_gateway.get_downstream_activities(activity, max_depth=1)

        if len(child_activities) > 0:
            activities |= child_activities
            for child in child_activities:
                graph.add_edge(activity, child)
        elif activity not in graph:
            graph.add_node(activity)

    if paths:
        tail_activities = {activity for activity in graph if any(g.entity.path in paths for g in activity.generations)}

        # NOTE: Add tail nodes and their ancestors are required for an update
        required_activities = tail_activities.copy()
        for activity in tail_activities:
            parents = networkx.ancestors(graph, activity)
            required_activities.update(parents)

        original_graph = graph.copy()
        # Exclude non-required activities
        for activity in original_graph:
            if activity not in required_activities:
                graph.remove_node(activity)

    return list(networkx.algorithms.dag.topological_sort(graph))


def _create_plan_from_activity_list(activities: List[Activity]) -> Union[Plan, PlanCollection]:
    """Create a CompositePlan by using Plans from an activity list."""
    plans = [a.to_plan() for a in activities]

    if len(plans) == 1:
        return plans[0]

    return PlanCollection(id=PlanCollection.generate_id(), plans=plans, name=f"plan-collection-{uuid.uuid4().hex}")


@inject.autoparams()
def execute_workflow(
    workflow: Union[CompositePlan, Plan, PlanCollection],
    command_name,
    client_dispatcher: IClientDispatcher,
    activity_gateway: IActivityGateway,
):
    """Execute a Run with/without subprocesses."""
    client = client_dispatcher.current_client

    # NOTE: Pull inputs from Git LFS or other storage backends
    if client.check_external_storage():
        inputs = [i.actual_value for i in workflow.inputs]
        client.pull_paths_from_storage(*inputs)

    delete_indirect_files_list(client.path)

    started_at_time = local_now()

    modified_outputs = _execute_workflow_helper(workflow=workflow, client=client)

    ended_at_time = local_now()

    add_to_git(client.repo.git, *modified_outputs)

    if client.repo.is_dirty():
        postfix = "s" if len(modified_outputs) > 1 else ""
        commit_msg = f"renku {command_name}: committing {len(modified_outputs)} modified file{postfix}"
        committer = Actor(f"renku {__version__}", version_url)
        client.repo.index.commit(commit_msg, committer=committer, skip_hooks=True)

    activity = Activity.from_plan(plan=workflow, started_at_time=started_at_time, ended_at_time=ended_at_time)

    activity_gateway.add(activity)


# TODO: This function is created as a patch from renku/core/commands/workflow.py::_execute_workflow and
# renku/core/management/workflow/providers/cwltool_provider.py::CWLToolProvider::workflow_execute in the ``workflow
# execute`` PR (renku-python/pull/2273). Once the PR is merged remove this function and refactor
# renku/core/commands/workflow.py::_execute_workflow to accept a Plan and use it here.
def _execute_workflow_helper(workflow: Union[Plan, PlanCollection], client):
    """Executes a given workflow using cwltool."""
    import os
    import shutil
    import sys
    import tempfile
    from pathlib import Path
    from urllib.parse import unquote

    import cwltool.factory
    from cwltool.context import LoadingContext, RuntimeContext

    from renku.core import errors
    from renku.core.commands.echo import progressbar

    basedir = client.path

    with tempfile.NamedTemporaryFile() as f:
        # export Plan to cwl
        from renku.core.management.workflow.converters.cwl import CWLExporter

        converter = CWLExporter()
        converter.workflow_convert(workflow=workflow, basedir=basedir, output=Path(f.name), output_format=None)

        # run cwl with cwltool
        argv = sys.argv
        sys.argv = ["cwltool"]

        runtime_args = {"rm_tmpdir": False, "move_outputs": "leave", "preserve_entire_environment": True}
        loading_args = {"relax_path_checks": True}

        # Keep all environment variables.
        runtime_context = RuntimeContext(kwargs=runtime_args)
        loading_context = LoadingContext(kwargs=loading_args)

        factory = cwltool.factory.Factory(loading_context=loading_context, runtime_context=runtime_context)
        process = factory.make(os.path.relpath(str(f.name)))

        try:
            outputs = process()
        except cwltool.factory.WorkflowStatus:
            raise errors.RenkuException("WorkflowExecuteError()")

        sys.argv = argv

        # Move outputs to correct location in the repository.
        output_dirs = process.factory.executor.output_dirs

        def remove_prefix(location, prefix="file://"):
            if location.startswith(prefix):
                return unquote(location[len(prefix) :])
            return unquote(location)

        locations = {remove_prefix(output["location"]) for output in outputs.values()}
        # make sure to not move an output if it's containing directory gets moved
        locations = {
            location for location in locations if not any(location.startswith(d) for d in locations if location != d)
        }

        output_paths = []
        with progressbar(locations, label="Moving outputs") as bar:
            for location in bar:
                for output_dir in output_dirs:
                    if location.startswith(output_dir):
                        output_path = location[len(output_dir) :].lstrip(os.path.sep)
                        destination = basedir / output_path
                        output_paths.append(destination)
                        if destination.is_dir():
                            shutil.rmtree(str(destination))
                            destination = destination.parent
                        shutil.move(location, str(destination))
                        continue

        return client.remove_unmodified(output_paths)
