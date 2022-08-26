# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Plan management."""

import itertools
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Union, cast

from renku.command.command_builder import inject
from renku.command.format.workflow import WORKFLOW_FORMATS
from renku.command.view_model.activity_graph import ActivityGraphViewModel
from renku.command.view_model.composite_plan import CompositePlanViewModel
from renku.command.view_model.plan import plan_view
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.os import are_paths_related, get_relative_paths, safe_read_yaml
from renku.core.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.value_resolution import CompositePlanValueResolver, ValueResolver
from renku.domain_model.provenance.activity import Activity
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.plan import AbstractPlan, Plan


@inject.autoparams()
def get_latest_plan(plan: Optional[AbstractPlan], plan_gateway: IPlanGateway) -> Optional[AbstractPlan]:
    """Return the latest version of a given plan in its derivative chain."""
    if plan is None:
        return None

    all_plans = plan_gateway.get_all_plans()

    child_plan: Optional[AbstractPlan] = plan
    while child_plan is not None:
        plan = child_plan
        child_plan = next((p for p in all_plans if p.derived_from == plan.id), None)

    return plan


@inject.autoparams()
def get_derivative_chain(
    plan: Optional[AbstractPlan], plan_gateway: IPlanGateway
) -> Generator[AbstractPlan, None, None]:
    """Return all plans in the derivative chain of a given plan including its parents/children and the plan itself."""
    if plan is None:
        return

    plan = get_latest_plan(plan)

    while plan is not None:
        yield plan
        plan = plan_gateway.get_by_id(plan.derived_from)


@inject.autoparams()
def search_workflows(name: str, plan_gateway: IPlanGateway) -> List[str]:
    """Get all the workflows whose Plan.name start with the given name.

    Args:
        name(str): The name to search for.
        plan_gateway(IPlanGateway): Injected Plan gateway.

    Returns:
        All Plans whose name starts with ``name``.
    """
    return plan_gateway.list_by_name(starts_with=name)


@inject.autoparams()
def list_workflows(plan_gateway: IPlanGateway, format: str, columns: List[str]):
    """List or manage workflows with subcommands.

    Args:
        plan_gateway(IPlanGateway): The injected Plan gateway.
        format(str): The output format.
        columns(List[str]): The columns to show for tabular output.

    Returns:
        List of workflows formatted by ``format``.
    """
    workflows = plan_gateway.get_newest_plans_by_names()

    if format not in WORKFLOW_FORMATS:
        raise errors.UsageError(f'Provided format "{format}" is not supported ({", ".join(WORKFLOW_FORMATS.keys())})"')

    if format == "json-ld":
        return WORKFLOW_FORMATS[format](list(workflows.values()), columns=columns)

    return WORKFLOW_FORMATS[format](list(map(lambda x: plan_view(x), workflows.values())), columns=columns)


@inject.autoparams()
def show_workflow(name_or_id: str, plan_gateway: IPlanGateway):
    """Show the details of a workflow.

    Args:
        name_or_id(str): Name or id of the Plan to show.
        plan_gateway(IPlanGateway): The injected Plan gateway.
    Returns:
        Details of the Plan.
    """
    workflow = plan_gateway.get_by_name_or_id(name_or_id)

    if is_plan_removed(workflow):
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    return plan_view(workflow)


@inject.autoparams("plan_gateway")
def remove_plan(name_or_id: str, force: bool, plan_gateway: IPlanGateway, when: datetime = local_now()):
    """Remove the workflow by its name or id.

    Args:
        name_or_id (str): The name of the Plan to remove.
        force (bool): Whether to force removal or not.
        plan_gateway(IPlanGateway): The injected Plan gateway.
        when(datetime): Time of deletion (Default value = current local date/time).
    Raises:
        errors.ParameterError: If the Plan doesn't exist or was already deleted.
    """
    plan: Optional[AbstractPlan] = plan_gateway.get_by_name(name_or_id) or plan_gateway.get_by_id(name_or_id)

    if not plan:
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    latest_version = get_latest_plan(plan)

    if latest_version.deleted:
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' is already deleted.")

    composites_containing_child = get_composite_plans_by_child(plan)

    if composites_containing_child:
        composite_names = "\n\t".join([c.name for c in composites_containing_child])

        if not force:
            raise errors.ParameterError(
                f"The specified workflow '{name_or_id}' is part of the following composite workflows and won't be "
                f"removed (use '--force' to remove anyways):\n\t{composite_names}"
            )
        else:
            communication.warn(f"Removing '{name_or_id}', which is still used in these workflows:\n\t{composite_names}")

    if not force:
        prompt_text = f"You are about to remove the following workflow '{name_or_id}'.\n\nDo you wish to continue?"
        communication.confirm(prompt_text, abort=True, warning=True)

    derived_plan = latest_version.derive()
    derived_plan.delete(when=when)

    plan_gateway.add(derived_plan)


@inject.autoparams()
def edit_workflow(
    name: str,
    new_name: Optional[str],
    description: Optional[str],
    set_params: List[str],
    map_params: List[str],
    rename_params: List[str],
    describe_params: List[str],
    plan_gateway: IPlanGateway,
    custom_metadata: Optional[Dict] = None,
):
    """Edits a workflow details.

    Args:
        name (str): Name of the Plan to edit.
        new_name(Optional[str]): New name of the Plan.
        description(Optional[str]): New description of the Plan.
        set_params(List[str]): New default values for parameters.
        map_params(List[str]): New mappings for Plan.
        rename_params(List[str]): New names for parameters.
        describe_params(List[str]): New descriptions for parameters.
        plan_gateway(IPlanGateway): Injected plan gateway.
        custom_metadata(Dict, optional): Custom JSON-LD metadata (Default value = None).

    Returns:
        Details of the modified Plan.
    """

    derived_from = plan_gateway.get_by_name_or_id(name)

    if is_plan_removed(derived_from):
        raise errors.ParameterError(f"The specified workflow '{name}' cannot be found.")

    if (
        not new_name
        and not description
        and not set_params
        and not map_params
        and not rename_params
        and not describe_params
        and not custom_metadata
    ):
        # NOTE: Nothing to do
        return plan_view(derived_from)

    workflow = derived_from.derive()
    if new_name:
        workflow.name = new_name

    if description:
        workflow.description = description

    if isinstance(workflow, Plan):
        if custom_metadata:
            existing_metadata = [a for a in workflow.annotations if a.source != "renku"]

            existing_metadata.append(Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku"))

            workflow.annotations = existing_metadata

        workflow.set_parameters_from_strings(set_params)

        def _mod_params(workflow, changed_params, attr):
            for param_string in changed_params:
                name, new_value = param_string.split("=", maxsplit=1)
                new_value = new_value.strip(' "')

                found = False
                for collection in [workflow.inputs, workflow.outputs, workflow.parameters]:
                    for i, param in enumerate(collection):
                        if param.name == name:
                            new_param = param.derive(plan_id=workflow.id)
                            setattr(new_param, attr, new_value)
                            collection[i] = new_param
                            found = True
                            break
                    if found:
                        break
                else:
                    raise errors.ParameterNotFoundError(parameter=name, workflow=workflow.name)

        _mod_params(workflow, rename_params, "name")
        _mod_params(workflow, describe_params, "description")
    elif isinstance(workflow, CompositePlan) and len(map_params):
        workflow.set_mappings_from_strings(map_params)

    plan_gateway.add(workflow)
    return plan_view(workflow)


@inject.autoparams()
def compose_workflow(
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
    """Compose workflows into a CompositePlan.

    Args:
        name(str): Name of the new composed Plan.
        description(str): Description for the Plan.
        mappings(List[str]): Mappings between parameters of this and child Plans.
        defaults(List[str]): Default values for parameters.
        links(List[str]): Links between parameters of child Plans.
        param_descriptions(List[str]): Descriptions of parameters.
        map_inputs(bool): Whether or not to automatically expose child inputs.
        map_outputs(bool): Whether or not to automatically expose child outputs.
        map_params(bool): Whether or not to automatically expose child parameters.
        link_all(bool): Whether or not to automatically link child steps' parameters.
        keywords(List[str]): Keywords for the Plan.
        steps(List[str]): Child steps to include.
        sources(List[str]): Starting files when automatically detecting child Plans.
        sinks(List[str]): Ending files when automatically detecting child Plans.
        activity_gateway(IActivityGateway): Injected activity gateway.
        plan_gateway(IPlanGateway): Injected plan gateway.
        project_gateway(IProjectGateway): Injected project gateway.
        client_dispatcher(IClientDispatcher): Injected client dispatcher.

    Returns:
        The newly created ``CompositePlan``.
    """
    from renku.core.workflow.activity import get_activities_until_paths, sort_activities

    if plan_gateway.get_by_name(name):
        raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    child_workflows = []
    plan_activities = []

    if steps:
        for workflow_name_or_id in steps:
            child_workflow = plan_gateway.get_by_id(workflow_name_or_id)

            if not child_workflow:
                child_workflow = plan_gateway.get_by_name(workflow_name_or_id)

            if not child_workflow or is_plan_removed(child_workflow):
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

        activities = list(
            get_activities_until_paths(
                sinks, sources, activity_gateway=activity_gateway, client_dispatcher=client_dispatcher
            )
        )
        activities = sort_activities(activities)

        # we need to get the actual plans from the DB as plan_with_values returns a copy
        for i, activity in enumerate(activities, 1):
            child_workflow = activity.association.plan

            if is_plan_removed(child_workflow):
                raise errors.ParameterError(
                    f"The workflow '{child_workflow.name}' on activity '{activity.id}' is deleted."
                )
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
            for param in itertools.chain(child_plan.inputs, child_plan.outputs, child_plan.parameters):
                try:
                    mapping_name = f"{i}-{param.name}"
                    plan.set_mappings_from_strings([f"{mapping_name}=@step{i}.{param.name}"])
                except errors.MappingExistsError:
                    continue

                plan.set_mapping_defaults([f"{mapping_name}={param.actual_value}"])

    if links:
        plan.set_links_from_strings(links)
        graph = ExecutionGraph([plan])
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
        rv = CompositePlanValueResolver(plan)
        plan = rv.apply()

        graph = ExecutionGraph([plan], virtual_links=True)

        cycles = graph.cycles
        if cycles:
            cycles = [map(lambda x: x.name, cycle) for cycle in cycles]
            raise errors.GraphCycleError(cycles)

        for virtual_link in graph.virtual_links:
            plan.add_link(virtual_link[0], [virtual_link[1]])

    plan_gateway.add(plan)

    return CompositePlanViewModel.from_composite_plan(plan)


@inject.autoparams()
def export_workflow(
    name_or_id,
    client_dispatcher: IClientDispatcher,
    plan_gateway: IPlanGateway,
    format: str,
    output: Optional[Union[str, Path]],
    values: Optional[str],
):
    """Export a workflow to a given format.

    Args:
        name_or_id: name or id of the Plan to export
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        plan_gateway(IPlanGateway): The injected Plan gateway.
        format(str): Format to export to.
        output(Optional[str]): Output path to store result at.
        values(Optional[str]): Path to values file to apply before export.
    Returns:
        The exported workflow as string.
    """
    client = client_dispatcher.current_client

    workflow = plan_gateway.get_by_name_or_id(name_or_id)

    if is_plan_removed(workflow):
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    output_path: Optional[Path] = None
    if output and isinstance(output, str):
        output_path = Path(output)
    elif output:
        output_path = cast(Path, output)

    if values:
        parsed_values = safe_read_yaml(values)
        rv = ValueResolver.get(workflow, parsed_values)
        workflow = rv.apply()
        if rv.missing_parameters:
            communication.warn(
                f'Could not resolve the following parameters in "{workflow.name}" workflow: '
                f'{",".join(rv.missing_parameters)}'
            )

    from renku.core.plugin.workflow import workflow_converter

    converter = workflow_converter(format)
    return converter(workflow=workflow, basedir=client.path, output=output_path, output_format=format)


@inject.autoparams()
def _lookup_paths_in_paths(client_dispatcher: IClientDispatcher, lookup_paths: List[str], target_paths: List[str]):
    """Return all lookup_paths that are in or under target_paths."""
    client = client_dispatcher.current_client

    dirs = []
    files = set()

    for p in lookup_paths:
        path = Path(get_relative_paths(base=client.path, paths=[p])[0])
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
def visualize_graph(
    sources: List[str],
    targets: List[str],
    show_files: bool,
    activity_gateway: IActivityGateway,
    client_dispatcher: IClientDispatcher,
    revision: Optional[str] = None,
):
    """Visualize an activity graph.

    Args:
        sources(List[str]): Input paths to start the visualized graph at.
        targets(List[str]): Output paths to end the visualized graph at.
        show_files(bool): Whether or not to show file nodes.
        activity_gateway(IActivityGateway): The injected activity gateway.
        client_dispatcher(IClientDispatcher): The injected client dispatcher.
        revision(Optional[str], optional): Revision or revision range to show
            the graph for  (Default value = None)

    Returns:
        Graph visualization view model.
    """
    from renku.core.workflow.activity import create_activity_graph, get_activities_until_paths

    client = client_dispatcher.current_client

    sources = sources or []
    sources = get_relative_paths(base=client.path, paths=[Path.cwd() / p for p in sources])

    if not targets:
        usages = activity_gateway.get_all_usage_paths()
        generations = activity_gateway.get_all_generation_paths()

        targets = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]
    else:
        targets = get_relative_paths(base=client.path, paths=[Path.cwd() / p for p in targets])

    activities = get_activities_until_paths(
        paths=targets,
        sources=sources,
        revision=revision,
        activity_gateway=activity_gateway,
        client_dispatcher=client_dispatcher,
    )
    graph = create_activity_graph(list(activities), with_inputs_outputs=show_files)
    return ActivityGraphViewModel(graph)


@inject.autoparams()
def workflow_inputs(activity_gateway: IActivityGateway, paths: List[str] = None):
    """Get inputs used by workflows.

    Args:
        activity_gateway(IActivityGateway): The injected activity gateway.
        paths(List[str], optional): List of paths to consider as inputs (Default value = None).

    Returns:
        Set[str]: Set of input file paths.
    """
    usage_paths = activity_gateway.get_all_usage_paths()

    if not paths:
        return usage_paths

    return _lookup_paths_in_paths(lookup_paths=paths, target_paths=usage_paths)


@inject.autoparams()
def workflow_outputs(activity_gateway: IActivityGateway, paths: List[str] = None):
    """Get inputs used by workflows.

    Args:
        activity_gateway(IActivityGateway): The injected activity gateway.
        paths(List[str], optional): List of paths to consider as outputs (Default value = None).

    Returns:
        Set[str]: Set of output file paths.
    """
    generation_paths = activity_gateway.get_all_generation_paths()

    if not paths:
        return generation_paths

    return _lookup_paths_in_paths(lookup_paths=paths, target_paths=generation_paths)


@inject.autoparams()
def get_initial_id(plan: Optional[AbstractPlan], plan_gateway: IPlanGateway) -> Optional[str]:
    """Return the id of the first plan in the derivative chain."""
    if plan is None:
        return None

    if not plan.derived_from:
        return plan.id

    parent = plan_gateway.get_by_id(plan.derived_from)
    if parent is None:
        raise errors.ParameterError(f"Cannot find parent plan with id '{plan.derived_from}'")

    return get_initial_id(parent)


@inject.autoparams()
def get_activities(plan: Optional[AbstractPlan], activity_gateway: IActivityGateway) -> Generator[Activity, None, None]:
    """Return all valid activities that use the plan or one of its parent/child derivatives."""
    if plan is None:
        return

    derivative_ids = [p.id for p in get_derivative_chain(plan=plan)]

    for activity in activity_gateway.get_all_activities():
        if activity.deleted:
            continue

        if activity.association.plan.id in derivative_ids:
            yield activity


def is_plan_removed(plan: AbstractPlan) -> bool:
    """Return true if the plan or any plan in its derivative chain is deleted."""
    for derived_plan in get_derivative_chain(plan):
        if derived_plan.deleted:
            return True

    return False


@inject.autoparams()
def get_composite_plans_by_child(plan: AbstractPlan, plan_gateway: IPlanGateway) -> List[CompositePlan]:
    """Return all composite plans that contain a child plan."""

    derivatives = {p.id for p in get_derivative_chain(plan=plan)}

    composites = (p for p in plan_gateway.get_newest_plans_by_names().values() if isinstance(p, CompositePlan))

    composites_containing_child = [c for c in composites if {p.id for p in c.plans}.intersection(derivatives)]

    return composites_containing_child
