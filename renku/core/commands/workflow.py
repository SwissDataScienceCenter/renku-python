# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Renku workflow commands."""


import uuid
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Optional

from git import Actor

from renku.core import errors
from renku.core.commands.format.workflow import WORKFLOW_FORMATS
from renku.core.commands.view_model import plan_view
from renku.core.commands.view_model.activity_graph import ActivityGraphViewModel
from renku.core.commands.view_model.composite_plan import CompositePlanViewModel
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.workflow.activity import get_activities_until_paths, sort_activities, create_activity_graph
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.plan_factory import delete_indirect_files_list
from renku.core.management.workflow.value_resolution import CompositePlanValueResolver, ValueResolver
from renku.core.models.provenance.activity import Activity, ActivityCollection
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import AbstractPlan, Plan
from renku.core.plugins.provider import execute
from renku.core.utils import communication
from renku.core.utils.datetime8601 import local_now
from renku.core.utils.git import add_to_git
from renku.core.utils.os import are_paths_related, get_relative_paths
from renku.version import __version__, version_url


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


def _safe_read_yaml(file: str) -> Dict[str, Any]:
    try:
        from renku.core.models import jsonld as jsonld

        return jsonld.read_yaml(file)
    except Exception as e:
        raise errors.ParameterError(e)


@inject.autoparams()
def _find_workflow(name_or_id: str, plan_gateway: IPlanGateway) -> AbstractPlan:
    workflow = plan_gateway.get_by_id(name_or_id) or plan_gateway.get_by_name(name_or_id)

    if not workflow:
        raise errors.ParameterError(f'The specified workflow "{name_or_id}" cannot be found.')
    return workflow


@inject.autoparams()
def _list_workflows(plan_gateway: IPlanGateway, format: str, columns: List[str]):
    """List or manage workflows with subcommands."""
    workflows = plan_gateway.get_newest_plans_by_names()

    if format not in WORKFLOW_FORMATS:
        raise errors.UsageError(f'Provided format "{format}" is not supported ({", ".join(WORKFLOW_FORMATS.keys())})"')

    if format == "json-ld":
        return WORKFLOW_FORMATS[format](list(workflows.values()), columns=columns)

    return WORKFLOW_FORMATS[format](list(map(lambda x: plan_view(x), workflows.values())), columns=columns)


def list_workflows_command():
    """Command to list or manage workflows with subcommands."""
    return Command().command(_list_workflows).require_migration().with_database(write=False)


@inject.autoparams()
def _remove_workflow(name, force, plan_gateway: IPlanGateway):
    """Remove the remote named <name>."""
    workflows = plan_gateway.get_newest_plans_by_names()
    plan = None
    if name.startswith("/plans/"):
        plan = next(filter(lambda x: x.id == name, workflows.values()), None)
    if not plan and name not in workflows:
        raise errors.ParameterError(f'The specified workflow is "{name}" is not an active workflow.')

    if not force:
        prompt_text = f'You are about to remove the following workflow "{name}".' + "\n" + "\nDo you wish to continue?"
        communication.confirm(prompt_text, abort=True, warning=True)

    plan = plan or workflows[name]
    plan._v_immutable = False
    plan.invalidated_at = datetime.utcnow()
    plan.freeze()


def remove_workflow_command():
    """Command that removes the workflow named <name>."""
    return Command().command(_remove_workflow).require_clean().with_database(write=True).with_commit()


def _show_workflow(name_or_id: str):
    """Show the details of a workflow."""
    workflow = _find_workflow(name_or_id)
    return plan_view(workflow)


def show_workflow_command():
    """Command that the details of a workflow."""
    return Command().command(_show_workflow).require_migration().with_database(write=False)


@inject.autoparams()
def _compose_workflow(
    name: str,
    description: str,
    mappings: List[str],
    defaults: List[str],
    links: List[str],
    param_descriptions: List[str],
    map_inputs: bool,
    map_outputs: bool,
    map_params: bool,
    link_all: bool,
    keywords: List[str],
    steps: List[str],
    sources: List[str],
    sinks: List[str],
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
    project_gateway: IProjectGateway,
    client_dispatcher: IClientDispatcher,
) -> CompositePlan:
    """Compose workflows into a CompositePlan."""

    if plan_gateway.get_by_name(name):
        raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    child_workflows = []
    plan_activities = []

    if steps:
        for workflow_name_or_id in steps:
            child_workflow = plan_gateway.get_by_id(workflow_name_or_id)

            if not child_workflow:
                child_workflow = plan_gateway.get_by_name(workflow_name_or_id)

            if not child_workflow:
                raise errors.ObjectNotFoundError(workflow_name_or_id)

            child_workflows.append(child_workflow)
    else:
        client = client_dispatcher.current_client
        sources = sources or []
        sources = get_relative_paths(base=client.path, paths=sources)

        if not sinks:
            usages = activity_gateway.get_all_usage_paths()
            generations = activity_gateway.get_all_generation_paths()

            sinks = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]

        sinks = get_relative_paths(base=client.path, paths=sinks)

        activities = list(get_activities_until_paths(sinks, sources, activity_gateway))
        activities = sort_activities(activities)

        # we need to get the actual plans from the DB as plan_with_values returns a copy
        for i, activity in enumerate(activities, 1):
            child_workflow = activity.association.plan
            child_workflows.append(child_workflow)
            plan_activities.append((i, activity.plan_with_values))

    plan = CompositePlan(
        description=description,
        id=CompositePlan.generate_id(),
        keywords=keywords,
        name=name,
        plans=child_workflows,
        project_id=project_gateway.get_project().id,
    )

    if mappings:
        plan.set_mappings_from_strings(mappings)

    if defaults:
        plan.set_mapping_defaults(defaults)

    if plan_activities:
        # Since composite is created from activities, we need to add mappings to set defaults to the values of
        # the activities, to ensure values from the involved activities are preserved.
        # If the user supplies their own mappings, those overrule the automatically added ones.

        for i, child_plan in plan_activities:
            for param in chain(child_plan.inputs, child_plan.outputs, child_plan.parameters):
                try:
                    mapping_name = f"{i}-{param.name}"
                    plan.set_mappings_from_strings([f"{mapping_name}=@step{i}.{param.name}"])
                except errors.MappingExistsError:
                    continue

                plan.set_mapping_defaults([f"{mapping_name}={param.actual_value}"])

    if links:
        plan.set_links_from_strings(links)
        graph = ExecutionGraph(plan)
        cycles = graph.cycles
        if cycles:
            cycles = [map(lambda x: x.name, cycle) for cycle in cycles]
            raise errors.GraphCycleError(cycles)

    if param_descriptions:
        plan.set_mapping_descriptions(param_descriptions)

    if map_inputs:
        plan.map_all_inputs()

    if map_outputs:
        plan.map_all_outputs()

    if map_params:
        plan.map_all_parameters()

    if link_all:
        # NOTE: propagate values to for linking to use
        rv = CompositePlanValueResolver(plan, None)
        plan = rv.apply()

        graph = ExecutionGraph(plan, virtual_links=True)

        cycles = graph.cycles
        if cycles:
            cycles = [map(lambda x: x.name, cycle) for cycle in cycles]
            raise errors.GraphCycleError(cycles)

        for virtual_link in graph.virtual_links:
            plan.add_link(virtual_link[0], [virtual_link[1]])

    plan_gateway.add(plan)

    return CompositePlanViewModel.from_composite_plan(plan)


def compose_workflow_command():
    """Command that creates a composite of several workflows."""
    return (
        Command().command(_compose_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )


@inject.autoparams()
def _edit_workflow(
    name,
    new_name: Optional[str],
    description: Optional[str],
    set_params: List[str],
    map_params: List[str],
    rename_params: List[str],
    describe_params: List[str],
    plan_gateway: IPlanGateway,
):
    """Edits a workflow details."""
    derived_from = _find_workflow(name)
    workflow = derived_from.derive()
    if new_name:
        workflow.name = new_name

    if description:
        workflow.description = description

    if isinstance(workflow, Plan):
        workflow.set_parameters_from_strings(set_params)

        def _kv_extract(kv_string):
            k, v = kv_string.split("=", maxsplit=1)
            v = v.strip(' "')
            return k, v

        for param_string in rename_params:
            name, new_name = _kv_extract(param_string)
            for param in workflow.inputs + workflow.outputs + workflow.parameters:
                if param.name == name:
                    param.name = new_name
                    break
            else:
                raise errors.ParameterNotFoundError(parameter=name, workflow=workflow.name)

        for description_string in describe_params:
            name, description = _kv_extract(description_string)
            for param in workflow.inputs + workflow.outputs + workflow.parameters:
                if param.name == name:
                    param.description = description
                    break
            else:
                raise errors.ParameterNotFoundError(parameter=name, workflow=workflow.name)
    elif isinstance(workflow, CompositePlan) and len(map_params):
        workflow.set_mappings_from_strings(map_params)

    plan_gateway.add(workflow)
    return plan_view(workflow)


def edit_workflow_command():
    """Command that edits the properties of a given workflow."""
    return Command().command(_edit_workflow).require_clean().with_database(write=True).with_commit()


@inject.autoparams()
def _export_workflow(
    name_or_id, client_dispatcher: IClientDispatcher, format: str, output: Optional[str], values: Optional[str]
):
    """Export a workflow to a given format."""
    client = client_dispatcher.current_client

    workflow = _find_workflow(name_or_id)
    if output:
        output = Path(output)

    if values:
        values = _safe_read_yaml(values)
        rv = ValueResolver.get(workflow, values)
        workflow = rv.apply()
        if rv.missing_parameters:
            communication.warn(
                f'Could not resolve the following parameters in "{workflow.name}" workflow: '
                f'{",".join(rv.missing_parameters)}'
            )

    from renku.core.plugins.workflow import workflow_converter

    converter = workflow_converter(format)
    return converter(workflow=workflow, basedir=client.path, output=output, output_format=format)


def export_workflow_command():
    """Command that exports a workflow into a given format."""
    return Command().command(_export_workflow).require_clean().with_database(write=False)


@inject.autoparams()
def _lookup_paths_in_paths(client_dispatcher: IClientDispatcher, lookup_paths: List[str], target_paths: List[str]):
    """Return all lookup_paths that are in or under target_paths."""
    client = client_dispatcher.current_client

    dirs = []
    files = set()

    for p in lookup_paths:
        path = Path(get_relative_paths(client.path, [p])[0])
        if path.is_dir():
            dirs.append(path)
        else:
            files.add(path)

    target_dirs = []
    target_files = set()

    for p in target_paths:
        path = Path(p)
        if path.is_dir():
            target_dirs.append(path)
        else:
            target_files.add(path)

    result = set()

    for target_file in target_files:
        if target_file in files or any(d in target_file.parents for d in dirs):
            result.add(str(target_file))

    for target_dir in target_dirs:
        if target_dir in dirs or any(target_dir in f.parents for f in files):
            result.add(str(target_dir))

    return result


@inject.autoparams()
def _workflow_inputs(activity_gateway: IActivityGateway, paths: List[str] = None):
    """Get inputs used by workflows."""
    usage_paths = activity_gateway.get_all_usage_paths()

    if not paths:
        return usage_paths

    return _lookup_paths_in_paths(lookup_paths=paths, target_paths=usage_paths)


def workflow_inputs_command():
    """Command that shows inputs used by workflows."""
    return Command().command(_workflow_inputs).require_migration().with_database(write=False)


@inject.autoparams()
def _workflow_outputs(activity_gateway: IActivityGateway, paths: List[str] = None):
    """Get inputs used by workflows."""
    generation_paths = activity_gateway.get_all_generation_paths()

    if not paths:
        return generation_paths

    return _lookup_paths_in_paths(lookup_paths=paths, target_paths=generation_paths)


def workflow_outputs_command():
    """Command that shows inputs used by workflows."""
    return Command().command(_workflow_outputs).require_migration().with_database(write=False)


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


def _execute_workflow(
    name_or_id: str, set_params: List[str], provider: str, config: Optional[str], values: Optional[str]
):
    workflow = _find_workflow(name_or_id)

    # apply the provided parameter settings provided by user
    override_params = dict()
    if values:
        override_params.update(_safe_read_yaml(values))

    if set_params:
        for param in set_params:
            name, value = param.split("=", maxsplit=1)
            override_params[name] = value

    if override_params:
        rv = ValueResolver.get(workflow, override_params)
        workflow = rv.apply()

        if rv.missing_parameters:
            communication.warn(
                f'Could not resolve the following parameters in "{workflow.name}" workflow: '
                f'{",".join(rv.missing_parameters)}'
            )

    if config:
        config = _safe_read_yaml(config)

    if isinstance(workflow, CompositePlan):
        import networkx as nx

        graph = ExecutionGraph(workflow=workflow, virtual_links=True)
        plans = list(nx.topological_sort(graph.workflow_graph))
    else:
        plans = [workflow]

    execute_workflow(plans=plans, command_name="execute", provider=provider, config=config)


def execute_workflow_command():
    """Command that executes a workflow."""
    return (
        Command().command(_execute_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )


@inject.autoparams()
def _visualize_graph(
    sources,
    targets,
    show_files,
    activity_gateway: IActivityGateway,
    client_dispatcher: IClientDispatcher,
):
    """Visualize an activity graph."""
    client = client_dispatcher.current_client

    sources = sources or []
    sources = get_relative_paths(base=client.path, paths=sources)

    if not targets:
        usages = activity_gateway.get_all_usage_paths()
        generations = activity_gateway.get_all_generation_paths()

        targets = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]
    targets = get_relative_paths(base=client.path, paths=targets)

    activities = get_activities_until_paths(targets, sources, activity_gateway)
    graph = create_activity_graph(activities, with_inputs_outputs=show_files)
    return ActivityGraphViewModel(graph)


def visualize_graph_command():
    """Execute the graph visualization command."""
    return Command().command(_visualize_graph).require_migration().with_database(write=False)
