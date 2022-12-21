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
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union, cast, overload

from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.command.format.workflow import WORKFLOW_FORMATS
from renku.command.view_model.activity_graph import ActivityGraphViewModel
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.plugin.workflow_file_parser import read_workflow_file
from renku.core.util import communication
from renku.core.util.git import get_git_user
from renku.core.util.os import are_paths_related, get_relative_paths
from renku.core.util.util import NO_VALUE, NoValueType
from renku.core.workflow.model.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.value_resolution import CompositePlanValueResolver, ValueResolver
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.activity import Activity
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.plan import AbstractPlan, Plan
from renku.infrastructure.immutable import DynamicProxy


@inject.autoparams("plan_gateway")
def get_plan(
    plan_gateway: IPlanGateway, name_or_id_or_path: Optional[str] = None, workflow_file: Optional[str] = None
) -> Union[AbstractPlan, str]:
    """Return the latest version of a given plan in its derivative chain."""
    plan = None

    # NOTE: At first look for a plan with the given name or ID. If not found, look for a workflow file with that name.

    if name_or_id_or_path:
        try:
            wf = plan_gateway.get_by_name_or_id(name_or_id_or_path)
        except errors.WorkflowNotFoundError:
            plan = None
        else:
            plan = None if is_plan_removed(wf) else wf
    elif not workflow_file:
        raise errors.ParameterError("Either 'name_or_id_or_path' or 'workflow_file' must be passed")

    if plan:
        return plan

    path = cast(str, name_or_id_or_path or workflow_file)

    if not os.path.exists(path):
        raise errors.WorkflowNotFoundError(path)

    return path


@overload
def get_latest_plan(plan: None = ..., plan_gateway: IPlanGateway = ...) -> None:  # noqa: D103
    ...


@overload
def get_latest_plan(plan: AbstractPlan, plan_gateway: IPlanGateway = ...) -> AbstractPlan:  # noqa: D103
    ...


@inject.autoparams("plan_gateway")
def get_latest_plan(
    plan: Optional[AbstractPlan], plan_gateway: IPlanGateway
) -> Union[Optional[AbstractPlan], AbstractPlan]:
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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def list_workflows(plan_gateway: IPlanGateway, format: str, columns: str):
    """List or manage workflows with subcommands.

    Args:
        plan_gateway(IPlanGateway): The injected Plan gateway.
        format(str): The output format.
        columns(List[str]): The columns to show for tabular output.

    Returns:
        List of workflows formatted by ``format``.
    """
    from renku.command.view_model.plan import plan_view

    workflows = plan_gateway.get_newest_plans_by_names()

    if format not in WORKFLOW_FORMATS:
        raise errors.UsageError(f'Provided format "{format}" is not supported ({", ".join(WORKFLOW_FORMATS.keys())})"')

    if format == "json-ld":
        return WORKFLOW_FORMATS[format](list(workflows.values()), columns=columns)

    return WORKFLOW_FORMATS[format](list(map(lambda x: plan_view(x), workflows.values())), columns=columns)


@inject.autoparams("activity_gateway")
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def show_workflow(name_or_id_or_path: str, activity_gateway: IActivityGateway, with_metadata: bool = False):
    """Show the details of a workflow.

    Args:
        name_or_id_or_path(str): Name or id of the Plan to show or path to a workflow file.
        activity_gateway(IActivityGateway): The injected Activity gateway.
        with_metadata(bool): Whether to get additional calculated metadata for the plan.
    Returns:
        Details of the Plan.
    """
    from renku.command.view_model.plan import plan_view
    from renku.command.view_model.workflow_file import WorkflowFileViewModel

    plan = get_plan(name_or_id_or_path=name_or_id_or_path)

    if isinstance(plan, str):  # A workflow file
        workflow_file = read_workflow_file(path=plan)
        return WorkflowFileViewModel.from_workflow_file(workflow_file)
    else:
        workflow = plan

        if with_metadata:
            activities = activity_gateway.get_all_activities()
            activity_map = _reverse_activity_plan_map(activities)
            plan_chain = list(get_derivative_chain(workflow))
            relevant_activities = [a for p in plan_chain for a in activity_map.get(p.id, [])]
            touches_files_cache: Dict[str, bool] = {}
            duration_cache: Dict[str, Optional[timedelta]] = {}
            touches_existing_files = _check_workflow_touches_existing_files(workflow, touches_files_cache, activity_map)

            if isinstance(workflow, Plan):

                num_executions = 0
                last_execution = None

                for activity in relevant_activities:
                    num_executions += 1

                    if not last_execution or last_execution < activity.ended_at_time:
                        last_execution = activity.ended_at_time

                workflow = cast(Plan, DynamicProxy(workflow))
                workflow.number_of_executions = num_executions
                workflow.last_executed = last_execution
                workflow.touches_existing_files = touches_existing_files
                workflow.latest = plan_chain[0].id
                workflow.duration = _get_plan_duration(workflow, duration_cache, activity_map)
            else:
                workflow = cast(CompositePlan, DynamicProxy(workflow))
                workflow.touches_existing_files = touches_existing_files
                workflow.latest = plan_chain[0].id
                workflow.duration = _get_plan_duration(workflow, duration_cache, activity_map)
                workflow.newest_plans = [get_latest_plan(p) for p in workflow.plans]

        return plan_view(cast(AbstractPlan, workflow), latest=with_metadata)


@inject.autoparams("plan_gateway")
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def remove_plan(name_or_id: str, force: bool, plan_gateway: IPlanGateway):
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
    derived_plan.delete()

    plan_gateway.add(derived_plan)


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def edit_workflow(
    name: str,
    new_name: Optional[str],
    description: Optional[str],
    set_params: List[str],
    map_params: List[str],
    rename_params: List[str],
    describe_params: List[str],
    creators: Union[List[Person], NoValueType],
    keywords: Union[List[str], NoValueType],
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
        creators(Union[List[Person], NoValueType]): Creators of the workflow.
        keywords(Union[List[str], NoValueType]): New keywords for the workflow.
        plan_gateway(IPlanGateway): Injected plan gateway.
        custom_metadata(Dict, optional): Custom JSON-LD metadata (Default value = None).

    Returns:
        Details of the modified Plan.
    """
    from renku.command.view_model.plan import plan_view

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
        and creators == NO_VALUE
        and keywords == NO_VALUE
    ):
        # NOTE: Nothing to do
        return plan_view(derived_from)

    git_creator = cast(Person, get_git_user(project_context.repository))
    workflow = derived_from.derive(creator=git_creator)
    if new_name:
        workflow.name = new_name

    if description:
        workflow.description = description

    if creators != NO_VALUE:
        workflow.creators = cast(List[Person], creators)

        if all(c.email != git_creator.email for c in workflow.creators):
            workflow.creators.append(git_creator)

    if keywords != NO_VALUE:
        workflow.keywords = cast(List[str], keywords)

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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def compose_workflow(
    name: str,
    description: Optional[str],
    mappings: Optional[List[str]],
    defaults: Optional[List[str]],
    links: Optional[List[str]],
    param_descriptions: Optional[List[str]],
    map_inputs: bool,
    map_outputs: bool,
    map_params: bool,
    link_all: bool,
    keywords: Optional[List[str]],
    steps: Optional[List[str]],
    sources: Optional[List[str]],
    sinks: Optional[List[str]],
    creators: Optional[List[Person]],
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
    project_gateway: IProjectGateway,
) -> CompositePlan:
    """Compose workflows into a CompositePlan.

    Args:
        name(str): Name of the new composed Plan.
        description(Optional[str]): Description for the Plan.
        mappings(Optional[List[str]]): Mappings between parameters of this and child Plans.
        defaults(Optional[List[str]]): Default values for parameters.
        links(Optional[List[str]]): Links between parameters of child Plans.
        param_descriptions(Optional[List[str]]): Descriptions of parameters.
        map_inputs(bool): Whether or not to automatically expose child inputs.
        map_outputs(bool): Whether or not to automatically expose child outputs.
        map_params(bool): Whether or not to automatically expose child parameters.
        link_all(bool): Whether or not to automatically link child steps' parameters.
        keywords(Optional[List[str]]): Keywords for the Plan.
        steps(Optional[List[str]]): Child steps to include.
        sources(Optional[List[str]]): Starting files when automatically detecting child Plans.
        sinks(Optional[List[str]]): Ending files when automatically detecting child Plans.
        creators(Optional[List[Person]]): Creator(s) of the composite plan.
        activity_gateway(IActivityGateway): Injected activity gateway.
        plan_gateway(IPlanGateway): Injected plan gateway.
        project_gateway(IProjectGateway): Injected project gateway.

    Returns:
        The newly created ``CompositePlan``.
    """
    from renku.command.view_model.composite_plan import CompositePlanViewModel
    from renku.core.workflow.activity import get_activities_until_paths, sort_activities

    if plan_gateway.get_by_name(name):
        raise errors.DuplicateWorkflowNameError(f"Duplicate workflow name: Workflow '{name}' already exists.")

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
        sources = sources or []
        sources = get_relative_paths(base=project_context.path, paths=sources)

        if not sinks:
            usages = activity_gateway.get_all_usage_paths()
            generations = activity_gateway.get_all_generation_paths()

            sinks = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]

        sinks = get_relative_paths(base=project_context.path, paths=sinks)

        activities = list(get_activities_until_paths(sinks, sources, activity_gateway=activity_gateway))
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

    if not creators:
        creators = [cast(Person, get_git_user(project_context.repository))]

    plan = CompositePlan(
        description=description,
        id=CompositePlan.generate_id(),
        keywords=keywords,
        name=name,
        plans=child_workflows,
        creators=creators,
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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def export_workflow(
    name_or_id,
    plan_gateway: IPlanGateway,
    format: str,
    output: Optional[Union[str, Path]],
    values: Optional[Dict[str, Any]],
    basedir: Optional[str],
    resolve_paths: Optional[bool],
    nest_workflows: Optional[bool],
):
    """Export a workflow to a given format.

    Args:
        name_or_id: name or id of the Plan to export
        plan_gateway(IPlanGateway): The injected Plan gateway.
        format(str): Format to export to.
        output(Optional[str]): Output path to store result at.
        values(Optional[Dict[str,Any]]): Parameter names and values to apply before export.
        basedir(Optional[str]): The base path prepended to all paths in the exported workflow,
            if None it defaults to the absolute path of the renku project.
        resolve_paths(Optional[bool]): Resolve all symlinks and make paths absolute, defaults to True.
        nest_workflows(Optional[bool]): Whether to try to nest all workflows into one specification and file or not,
            defaults to False.
    Returns:
        The exported workflow as string.
    """

    if resolve_paths is None:
        resolve_paths = True

    if basedir is None:
        basedir_path = project_context.path
    elif isinstance(basedir, str):
        basedir_path = Path(basedir)

    workflow = plan_gateway.get_by_name_or_id(name_or_id)

    if is_plan_removed(workflow):
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    output_path: Optional[Path] = None
    if output and isinstance(output, str):
        output_path = Path(output)
    elif output:
        output_path = cast(Path, output)

    if values:
        rv = ValueResolver.get(workflow, values)
        workflow = rv.apply()
        if rv.missing_parameters:
            communication.warn(
                f'Could not resolve the following parameters in "{workflow.name}" workflow: '
                f'{",".join(rv.missing_parameters)}'
            )

    from renku.core.plugin.workflow import workflow_converter

    converter = workflow_converter(format)
    return converter(
        workflow=workflow,
        basedir=basedir_path,
        output=output_path,
        output_format=format,
        resolve_paths=resolve_paths,
        nest_workflows=nest_workflows,
    )


def _lookup_paths_in_paths(lookup_paths: List[str], target_paths: List[str]):
    """Return all lookup_paths that are in or under target_paths."""

    dirs = []
    files = set()

    for p in lookup_paths:
        path = Path(get_relative_paths(base=project_context.path, paths=[p])[0])
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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def visualize_graph(
    sources: List[str],
    targets: List[str],
    show_files: bool,
    activity_gateway: IActivityGateway,
    revision: Optional[str] = None,
):
    """Visualize an activity graph.

    Args:
        sources(List[str]): Input paths to start the visualized graph at.
        targets(List[str]): Output paths to end the visualized graph at.
        show_files(bool): Whether or not to show file nodes.
        activity_gateway(IActivityGateway): The injected activity gateway.
        revision(Optional[str], optional): Revision or revision range to show
            the graph for  (Default value = None)

    Returns:
        Graph visualization view model.
    """
    from renku.core.workflow.activity import create_activity_graph, get_activities_until_paths

    sources = sources or []
    sources = get_relative_paths(base=project_context.path, paths=[Path.cwd() / p for p in sources])

    if not targets:
        usages = activity_gateway.get_all_usage_paths()
        generations = activity_gateway.get_all_generation_paths()

        targets = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]
    else:
        targets = get_relative_paths(base=project_context.path, paths=[Path.cwd() / p for p in targets])

    activities = get_activities_until_paths(
        paths=targets, sources=sources, revision=revision, activity_gateway=activity_gateway
    )
    graph = create_activity_graph(list(activities), with_inputs_outputs=show_files, with_hidden_dependencies=True)
    return ActivityGraphViewModel(graph)


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def workflow_inputs(activity_gateway: IActivityGateway, paths: Optional[List[str]] = None):
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
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def workflow_outputs(activity_gateway: IActivityGateway, paths: Optional[List[str]] = None):
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


@inject.autoparams("activity_gateway", "plan_gateway")
def get_plans_with_metadata(activity_gateway: IActivityGateway, plan_gateway: IPlanGateway) -> List[AbstractPlan]:
    """Get all plans in the project with additional metadata.

    Adds information about last execution, number of executions and whether the plan was used to create files
    currently existing in the project.
    """

    all_activities = activity_gateway.get_all_activities()
    activity_map = _reverse_activity_plan_map(list(all_activities))
    latest_plan_chains: Set[Tuple[AbstractPlan]] = set(
        cast(Tuple[AbstractPlan], tuple(get_derivative_chain(p)))
        for p in plan_gateway.get_newest_plans_by_names().values()
    )

    result: Dict[str, Union[Plan, CompositePlan]] = {}
    touches_file_cache: Dict[str, bool] = {}
    duration_cache: Dict[str, Optional[timedelta]] = {}

    # check which plans where involved in using/creating existing files
    for plan_chain in latest_plan_chains:
        latest_plan = cast(Union[Plan, CompositePlan], DynamicProxy(plan_chain[0]))
        latest_plan.touches_existing_files = False
        latest_plan.number_of_executions = 0
        latest_plan.created = latest_plan.date_created
        latest_plan.last_executed = None
        latest_plan.children = []
        duration = _get_plan_duration(latest_plan, duration_cache, activity_map)

        if duration is not None:
            latest_plan.duration = duration.seconds

        if isinstance(plan_chain[0], Plan):
            for activity in activity_map.get(latest_plan.id, []):
                if not latest_plan.last_executed or latest_plan.last_executed < activity.ended_at_time:
                    latest_plan.last_executed = activity.ended_at_time

                latest_plan.number_of_executions += 1

                latest_plan.touches_existing_files = _check_workflow_touches_existing_files(
                    latest_plan, touches_file_cache, activity_map
                )
            latest_plan.type = "Plan"
        else:
            latest_plan.number_of_executions = None
            latest_plan.type = "CompositePlan"
            latest_plan.children = [get_latest_plan(p).id for p in latest_plan.plans]
            latest_plan.touches_existing_files = _check_workflow_touches_existing_files(
                latest_plan, touches_file_cache, activity_map
            )

        result[latest_plan.id] = latest_plan

    return list(result.values())  # type: ignore


def _reverse_activity_plan_map(activities: List[Activity], latest: bool = True) -> Dict[str, Set[Activity]]:
    """Create a map from plan id to relevant activities."""
    result: Dict[str, Set[Activity]] = {}

    for activity in activities:
        plan = activity.association.plan

        if latest:
            plan = get_latest_plan(plan)

        if plan.id not in result:
            result[plan.id] = {activity}
        else:
            result[plan.id].add(activity)

    return result


def _check_workflow_touches_existing_files(
    workflow: Union[Plan, CompositePlan],
    cache: Dict[str, bool],
    activity_map: Dict[str, Set[Activity]],
    latest: bool = True,
) -> bool:
    """Check if a workflow or one of its children touches existing files."""
    if latest:
        workflow = get_latest_plan(workflow)

    if workflow.id in cache:
        return cache[workflow.id]

    if isinstance(workflow, Plan):
        for activity in activity_map.get(workflow.id, []):
            for output in activity.generations:
                if (project_context.path / output.entity.path).exists():
                    return cache.setdefault(workflow.id, True)
    else:
        for child in workflow.plans:
            if _check_workflow_touches_existing_files(child, cache, activity_map):
                return cache.setdefault(workflow.id, True)

    return cache.setdefault(workflow.id, False)


def _get_plan_duration(
    workflow: Union[Plan, CompositePlan],
    cache: Dict[str, Optional[timedelta]],
    activity_map: Dict[str, Set[Activity]],
    latest: bool = True,
) -> Optional[timedelta]:
    if latest:
        workflow = get_latest_plan(workflow)

    if workflow.id in cache:
        return cache[workflow.id]

    if isinstance(workflow, Plan):
        times = []
        for activity in activity_map.get(workflow.id, []):
            times.append(activity.ended_at_time - activity.started_at_time)

        if not times:
            return cache.setdefault(workflow.id, None)

        return cache.setdefault(workflow.id, sum(times, timedelta(0)) / len(times))

    else:
        total = timedelta(0)
        found = False
        for child in workflow.plans:
            child_time = _get_plan_duration(child, cache, activity_map)

            if child_time is not None:
                total += child_time
                found = True

        if found:
            return cache.setdefault(workflow.id, total)

    return cache.setdefault(workflow.id, None)
