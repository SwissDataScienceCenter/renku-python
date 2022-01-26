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


import itertools
import re
from collections import defaultdict
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from networkx import DiGraph

from renku.core import errors
from renku.core.commands.format.workflow import WORKFLOW_FORMATS
from renku.core.commands.view_model.activity_graph import ActivityGraphViewModel
from renku.core.commands.view_model.composite_plan import CompositePlanViewModel
from renku.core.commands.view_model.plan import plan_view
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.workflow.activity import create_activity_graph, get_activities_until_paths, sort_activities
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.plan_factory import delete_indirect_files_list
from renku.core.management.workflow.value_resolution import CompositePlanValueResolver, ValueResolver
from renku.core.models.provenance.activity import Activity, ActivityCollection
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import AbstractPlan, Plan
from renku.core.plugins.provider import execute
from renku.core.utils import communication
from renku.core.utils.datetime8601 import local_now
from renku.core.utils.os import are_paths_related, get_relative_paths


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
def _search_workflows(name: str, plan_gateway: IPlanGateway) -> List[str]:
    """Get all the workflows whose Plan.name are greater than or equal to the given name."""
    return plan_gateway.list_by_name(starts_with=name)


def search_workflows_command():
    """Command to get all the workflows whose Plan.name are greater than or equal to the given name."""
    return Command().command(_search_workflows).require_migration().with_database(write=False)


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
    plan.unfreeze()
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

        activities = list(
            get_activities_until_paths(
                sinks, sources, activity_gateway=activity_gateway, client_dispatcher=client_dispatcher
            )
        )
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
        rv = CompositePlanValueResolver(plan, None)
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


@inject.params(client_dispatcher=IClientDispatcher, activity_gateway=IActivityGateway, plan_gateway=IPlanGateway)
def execute_workflow(
    dag: "DiGraph",
    command_name: str,
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
        inputs = [i.actual_value for p in dag.nodes for i in p.inputs]
        client.pull_paths_from_storage(*inputs)

    delete_indirect_files_list(client.path)

    if config:
        config = _safe_read_yaml(config)

    started_at_time = local_now()

    execute(dag=dag, basedir=client.path, provider=provider, config=config)

    ended_at_time = local_now()

    activities = []

    for plan in dag.nodes:
        # NOTE: Update plans are copies of Plan objects. We need to use the original Plan objects to avoid duplicates.
        original_plan = plan_gateway.get_by_id(plan.id)
        activity = Activity.from_plan(plan=plan, started_at_time=started_at_time, ended_at_time=ended_at_time)
        activity.association.plan = original_plan
        activity_gateway.add(activity)
        activities.append(activity)

    if len(activities) > 1:
        activity_collection = ActivityCollection(activities=activities)
        activity_gateway.add_activity_collection(activity_collection)


def _execute_workflow(
    name_or_id: str, set_params: List[str], provider: str, config: Optional[str], values: Optional[str]
):
    def _nested_dict():
        return defaultdict(_nested_dict)

    workflow = _find_workflow(name_or_id)

    # apply the provided parameter settings provided by user
    override_params = dict()
    if values:
        override_params.update(_safe_read_yaml(values))

    if set_params:
        from deepmerge import always_merger

        for param in set_params:
            name, value = param.split("=", maxsplit=1)
            keys = name.split(".")

            set_param = reduce(lambda x, y: {y: x}, reversed(keys), value)
            override_params = always_merger.merge(override_params, set_param)

    rv = ValueResolver.get(workflow, override_params)

    workflow = rv.apply()

    if rv.missing_parameters:
        communication.warn(
            f'Could not resolve the following parameters in "{workflow.name}" workflow: '
            f'{",".join(rv.missing_parameters)}'
        )

    graph = ExecutionGraph([workflow], virtual_links=True)
    execute_workflow(dag=graph.workflow_graph, command_name="execute", provider=provider, config=config)


def execute_workflow_command():
    """Command that executes a workflow."""
    return (
        Command().command(_execute_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )


@inject.autoparams()
def _visualize_graph(
    sources: List[str],
    targets: List[str],
    show_files: bool,
    activity_gateway: IActivityGateway,
    client_dispatcher: IClientDispatcher,
    revision: Optional[str] = None,
):
    """Visualize an activity graph."""
    client = client_dispatcher.current_client

    sources = sources or []
    sources = get_relative_paths(base=client.path, paths=sources)

    if not targets:
        usages = activity_gateway.get_all_usage_paths()
        generations = activity_gateway.get_all_generation_paths()

        targets = [g for g in generations if all(not are_paths_related(g, u) for u in usages)]

    activities = get_activities_until_paths(
        paths=targets,
        sources=sources,
        revision=revision,
        activity_gateway=activity_gateway,
        client_dispatcher=client_dispatcher,
    )
    graph = create_activity_graph(activities, with_inputs_outputs=show_files)
    return ActivityGraphViewModel(graph)


def visualize_graph_command():
    """Execute the graph visualization command."""
    return Command().command(_visualize_graph).require_migration().with_database(write=False)


def _extract_iterate_parameters(values: Dict[str, Any], index_pattern: re.Pattern, tag_separator: str = "@"):
    """Recursively extracts the iteration paramaters from the workflow values given by the user."""
    iter_params = {"indexed": {}, "params": {}, "tagged": {}}
    params = {}
    for param_name, param_value in values.items():
        if isinstance(param_value, str) and index_pattern.search(param_value):
            iter_params["indexed"][param_name] = param_value
            params[param_name] = param_value
        elif isinstance(param_value, list):
            if len(param_value) == 1:
                communication.warn(
                    f"The parameter '{param_name}' has only one element '{param_value}', "
                    "changing it to be a fixed parameter!"
                )
                params[param_name] = param_value[0]
                continue

            if tag_separator in param_name:
                name, tag = param_name.split(tag_separator, maxsplit=1)
                if tag in iter_params["tagged"]:
                    iter_params["tagged"][tag][name] = param_value
                else:
                    iter_params["tagged"][tag] = {name: param_value}

                params[name] = param_value
            else:
                iter_params["params"][param_name] = param_value
                params[param_name] = param_value
        elif isinstance(param_value, dict):
            inner_iter_params, inner_params = _extract_iterate_parameters(param_value, index_pattern, tag_separator)
            iter_params["params"].update([(f"{param_name}.{ik}", iv) for ik, iv in inner_iter_params["params"].items()])
            iter_params["indexed"].update(
                [(f"{param_name}.{ik}", iv) for ik, iv in inner_iter_params["indexed"].items()]
            )
            for tag, param in inner_iter_params["tagged"].items():
                if tag in iter_params["tagged"]:
                    iter_params["tagged"][tag].update([(f"{param_name}.{ik}", iv) for ik, iv in param.items()])
                else:
                    iter_params["tagged"][tag] = dict([(f"{param_name}.{ik}", iv) for ik, iv in param.items()])
            params[param_name] = inner_params
        else:
            params[param_name] = param_value
    return iter_params, params


def _validate_iterate_parameters(
    workflow: AbstractPlan, workflow_params: Dict[str, Any], iter_params: Dict[str, Any], mapping_path: str
) -> Dict[str, Any]:
    """Validates the user provided iteration parameters."""
    import copy

    rv = ValueResolver.get(copy.deepcopy(workflow), workflow_params)
    rv.apply()

    mp_paths = [mp.split(".") for mp in rv.missing_parameters]
    for collection in [iter_params["indexed"], iter_params["params"], *iter_params["tagged"].values()]:
        remove_keys = []
        for p in collection.keys():
            parameter_path = p.split(".")
            if any(parameter_path[: len(mp)] == mp for mp in mp_paths):
                remove_keys.append(p)

        for rk in remove_keys:
            collection.pop(rk)

    # validate tagged
    empty_tags = []
    for k, tagged_params in iter_params["tagged"].items():
        if len(tagged_params) == 0:
            empty_tags.append(k)
        else:
            tagged_params_values = list(tagged_params.values())
            tag_size = len(tagged_params_values[0])
            for p in tagged_params_values[1:]:
                num_params = len(p)
                if tag_size != num_params:
                    communication.error(
                        f"'{k}' tagged parameters '{tagged_params}' has different number of possible values!"
                    )
                    return None

    for et in empty_tags:
        iter_params["tagged"].pop(et)

    if (len(iter_params["indexed"]) == 0) and (len(iter_params["params"]) == 0) and (len(iter_params["tagged"]) == 0):
        raise errors.UsageError(
            "Please check the provided mappings as none of the "
            f"parameters are present in the '{workflow.name}' workflow"
        )

    if rv.missing_parameters:
        communication.confirm(
            f'Could not resolve the following parameters in "{workflow.name}" workflow: '
            f'{", ".join(rv.missing_parameters)}. Resume the execution?',
            abort=True,
        )

    return iter_params


def _build_iterations(
    workflow: AbstractPlan, workflow_params: Dict[str, Any], iter_params: Dict[str, Any], index_pattern: re.Pattern
) -> List[AbstractPlan]:
    """Instantiate the workflows for each iteration."""
    import copy

    from deepmerge import always_merger

    plans = []
    execute_plan = []

    columns = list(iter_params["params"].keys())
    tagged_values = []
    for tag in iter_params["tagged"].values():
        columns.extend(tag.keys())
        tagged_values.append(zip(*tag.values()))

    def _flatten(values):
        for i in values:
            if isinstance(i, (list, tuple)):
                for k in i:
                    yield k
            else:
                yield i

    for i, values in enumerate(itertools.product(*iter_params["params"].values(), *tagged_values)):
        plan_params = copy.deepcopy(workflow_params)
        iteration_values = {}
        for k, v in iter_params["indexed"].items():
            value = index_pattern.sub(str(i), v)
            set_param = reduce(lambda x, y: {y: x}, reversed(k.split(".")), value)
            plan_params = always_merger.merge(plan_params, set_param)
            iteration_values[k] = value

        for param_key, param_value in zip(columns, _flatten(values)):
            set_param = reduce(lambda x, y: {y: x}, reversed(param_key.split(".")), param_value)
            plan_params = always_merger.merge(plan_params, set_param)
            iteration_values[param_key] = param_value

        execute_plan.append(iteration_values)
        rv = ValueResolver.get(copy.deepcopy(workflow), plan_params)
        plans.append(rv.apply())

    return plans, execute_plan


def _iterate_workflow(
    name_or_id: str,
    mapping_path: str,
    mappings: List[str],
    dry_run: bool,
    provider: str,
    config: Optional[str],
):
    import ast

    from deepmerge import always_merger

    from renku.core.models.tabulate import tabulate

    if mapping_path is None and len(mappings) == 0:
        raise errors.UsageError("No mapping has been given for the iteration!")

    workflow = _find_workflow(name_or_id)
    TAG_SEPARATOR = "@"
    index_pattern = re.compile(r"{iter_index}")

    iter_params = {"indexed": {}, "params": {}, "tagged": {}}
    workflow_params = {}
    if mapping_path:
        mapping = _safe_read_yaml(mapping_path)
        iter_params, workflow_params = _extract_iterate_parameters(mapping, index_pattern, tag_separator=TAG_SEPARATOR)

    for m in mappings:
        param_name, param_value = m.split("=", maxsplit=1)
        if index_pattern.search(param_value):
            iter_params["indexed"][param_name] = param_value
        else:
            try:
                param_value = ast.literal_eval(param_value)
            except Exception:
                raise errors.ParameterError(
                    f"The value of '{param_name}' parameter is neither a list nor templated variable!"
                )

            if len(param_value) == 1:
                communication.warn(
                    f"The parameter '{param_name}' has only one element '{param_value}', "
                    "changing it to be a fixed parameter!"
                )
                workflow_params[param_name] = param_value[0]
                continue

            if TAG_SEPARATOR in param_name:
                name, tag = param_name.split(TAG_SEPARATOR, maxsplit=1)
                if tag in iter_params["tagged"]:
                    iter_params["tagged"][tag][name] = param_value
                else:
                    iter_params["tagged"][tag] = {name: param_value}

                param_name = name
            else:
                iter_params["params"][param_name] = param_value

        set_param = reduce(lambda x, y: {y: x}, reversed(param_name.split(".")), param_value)
        workflow_params = always_merger.merge(workflow_params, set_param)

    iter_params = _validate_iterate_parameters(workflow, workflow_params, iter_params, mapping_path)
    if iter_params is None:
        return

    plans, execute_plan = _build_iterations(workflow, workflow_params, iter_params, index_pattern)

    communication.echo(f"\n\n{tabulate(execute_plan, execute_plan[0].keys())}")
    if not dry_run:
        graph = ExecutionGraph(workflows=plans, virtual_links=True)
        execute_workflow(dag=graph.workflow_graph, command_name="iterate", provider=provider, config=config)


def iterate_workflow_command():
    """Command that executes several workflows given a set of variables."""
    return (
        Command().command(_iterate_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )
