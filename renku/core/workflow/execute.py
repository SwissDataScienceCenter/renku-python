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
"""Plan execution."""

import itertools
import re
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.plugin.provider import execute
from renku.core.storage import check_external_storage, pull_paths_from_storage
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.util.os import is_subpath, safe_read_yaml
from renku.core.workflow.model.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.plan import is_plan_removed
from renku.core.workflow.plan_factory import delete_indirect_files_list
from renku.core.workflow.value_resolution import ValueResolver
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.activity import Activity, ActivityCollection, WorkflowFileActivityCollection
from renku.domain_model.workflow.plan import AbstractPlan
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan

if TYPE_CHECKING:
    from networkx import DiGraph


@inject.params(activity_gateway=IActivityGateway, plan_gateway=IPlanGateway)
def execute_workflow_graph(
    dag: "DiGraph",
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
    provider="toil",
    config=None,
    workflow_file_plan: Optional[WorkflowFileCompositePlan] = None,
):
    """Execute a Run with/without subprocesses.

    Args:
        dag(DiGraph): The workflow graph to execute.
        activity_gateway(IActivityGateway): The injected activity gateway.
        plan_gateway(IPlanGateway): The injected plan gateway.
        provider: Provider to run the workflow with (Default value = "toil").
        config: Path to config for the workflow provider (Default value = None).
        workflow_file_plan (Optional[WorkflowFileCompositePlan): If passed, a workflow file is executed, so, store
            related metadata.
    """
    inputs = {i.actual_value for p in dag.nodes for i in p.inputs}
    # NOTE: Pull inputs from Git LFS or other storage backends
    if check_external_storage():
        pull_paths_from_storage(project_context.repository, *inputs)

    # check whether the none generated inputs of workflows are available
    outputs = {o.actual_value for p in dag.nodes for o in p.outputs}
    inputs = {i for i in inputs if i not in outputs and not any(is_subpath(path=i, base=o) for o in outputs)}
    for i in inputs:
        if not Path(i).exists():
            raise errors.ParameterError(f"Invalid input value: Input '{i}' does not exist!", show_prefix=False)

    delete_indirect_files_list(project_context.path)

    if config:
        config = safe_read_yaml(config)

    started_at_time = local_now()

    execute(dag=dag, basedir=project_context.path, provider=provider, config=config)

    ended_at_time = local_now()

    activities = []

    for plan in dag.nodes:
        # NOTE: Update plans are copies of Plan objects. We need to use the original Plan objects to avoid duplicates.
        original_plan = plan_gateway.get_by_id(plan.id)

        # NOTE: Workflow files don't have an original plan
        if not original_plan:
            original_plan = plan

        activity = Activity.from_plan(
            plan=plan,
            repository=project_context.repository,
            started_at_time=started_at_time,
            ended_at_time=ended_at_time,
        )
        activity.association.plan = original_plan
        activity_gateway.add(activity)
        activities.append(activity)

    if workflow_file_plan:
        activity_collection = WorkflowFileActivityCollection.from_activities(
            activities=activities, plan=workflow_file_plan
        )
        activity_gateway.add_activity_collection(activity_collection)
    elif len(activities) > 1:
        activity_collection = ActivityCollection(activities=activities)
        activity_gateway.add_activity_collection(activity_collection)


def check_for_cycles(graph: ExecutionGraph):
    """Check for cycles in the graph and raises an error if there are any."""
    if not graph.cycles:
        return

    cycles_str = []
    for cycle in graph.cycles:
        nodes = []
        for node in cycle:
            if isinstance(node, AbstractPlan):
                nodes.append(f"[{node.name}]")
            else:
                cls = node.__class__.__name__.replace("Command", "")
                nodes.append(f"{cls}: {node.actual_value}")
        cycles_str.append(" -> ".join(nodes))

    message = "Circular workflows are not supported in Renku. Please remove these cycles:\n\t"
    message += "\n\t".join(cycles_str)
    raise errors.GraphCycleError(message=message, cycles=[])


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def execute_workflow(
    name_or_id: str,
    set_params: List[str],
    provider: str,
    config: Optional[str],
    values: Optional[str],
    plan_gateway: IPlanGateway,
):
    """Execute a plan with specified values.

    Args:
        name_or_id(str): Name or id of the Plan to iterate.
        set_params(List[str]): List of values specified for workflow parameters.
        provider(str): Name of the workflow provider backend to use for execution.
        config(Optional[str]): Path to config for the workflow provider.
        values(Optional[str]): Path to YAMl file containing values specified for workflow parameters.
        plan_gateway(IPlanGateway): The plan gateway.
    """
    workflow = plan_gateway.get_by_name_or_id(name_or_id)

    if is_plan_removed(workflow):
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    # apply the provided parameter settings provided by user
    override_params = dict()
    if values:
        override_params.update(safe_read_yaml(values))

    if set_params:
        from deepmerge import always_merger

        for param in set_params:
            name, value = param.split("=", maxsplit=1)
            keys = name.split(".")

            set_param = reduce(lambda x, y: {y: x}, reversed(keys), value)  # type: ignore
            override_params = always_merger.merge(override_params, set_param)

    rv = ValueResolver.get(workflow, override_params)

    workflow = rv.apply()

    if rv.missing_parameters:
        communication.warn(
            f'Could not resolve the following parameters in "{workflow.name}" workflow: '
            f'{",".join(rv.missing_parameters)}'
        )

    graph = ExecutionGraph([workflow], virtual_links=True)
    execute_workflow_graph(dag=graph.workflow_graph, provider=provider, config=config)


def _extract_iterate_parameters(values: Dict[str, Any], index_pattern: re.Pattern, tag_separator: str = "@"):
    """Recursively extracts the iteration parameters from the workflow values given by the user.

    Args:
        values(Dict[str, Any]): Plan values to iterate over.
        index_pattern(re.Pattern): Pattern for parameter indexes.
        tag_separator(str, optional): Separator for tagged values (Default value = "@").

    Returns:
        Tuple of ``(iter_params, params)`` where ``params`` are regular parameters
        and ``iter_params`` are parameters with iteration values.
    """
    iter_params: Dict[str, Any] = {"indexed": {}, "params": {}, "tagged": {}}
    params: Dict[str, Any] = {}
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
    workflow: AbstractPlan, workflow_params: Dict[str, Any], iter_params: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Validates the user provided iteration parameters.

    Args:
        workflow(AbstractPlan): The Plan to validate parameters against.
        workflow_params(Dict[str, Any]): The plain parameters to check.
        iter_params(Dict[str, Any]): The iterative parameters to check.

    Returns:
        Dictionary of validated iteration parameters.
    """
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
) -> Tuple[List[AbstractPlan], List[Dict]]:
    """Instantiate the workflows for each iteration.

    Args:
        workflow(AbstractPlan): The base workflow to use as a template.
        workflow_params(Dict[str, Any]): The plain parameters to use.
        iter_params(Dict[str, Any]): The iterative parameters to use.
        index_pattern(re.Pattern): The pattern for the index placeholder.

    Returns:
        Tuple of ``(plans, itervalues)`` with ``plans`` being a list of all
        plans for each iteration and ``itervalues`` being a list of all values
        for each iteration.
    """
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
            set_param = reduce(lambda x, y: {y: x}, reversed(k.split(".")), value)  # type: ignore
            plan_params = always_merger.merge(plan_params, set_param)
            iteration_values[k] = value

        for param_key, param_value in zip(columns, _flatten(values)):
            set_param = reduce(lambda x, y: {y: x}, reversed(param_key.split(".")), param_value)  # type: ignore
            plan_params = always_merger.merge(plan_params, set_param)
            iteration_values[param_key] = param_value

        execute_plan.append(iteration_values)
        rv = ValueResolver.get(copy.deepcopy(workflow), plan_params)
        plans.append(rv.apply())

    return plans, execute_plan


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def iterate_workflow(
    name_or_id: str,
    mapping_path: Optional[str],
    mappings: List[str],
    dry_run: bool,
    provider: str,
    config: Optional[str],
    plan_gateway: IPlanGateway,
):
    """Iterate a workflow repeatedly with differing values.

    Args:
        name_or_id(str): Name or id of the Plan to iterate.
        mapping_path(str): Path to file defining workflow mappings.
        mappings(List[str]): List of workflow mappings.
        dry_run(bool): Whether to preview execution or actually run it.
        provider(str): Name of the workflow provider backend to use for execution.
        config(Optional[str]): Path to config for the workflow provider.
        plan_gateway(IPlanGateway): The plan gateway.
    """
    import ast

    from deepmerge import always_merger

    from renku.core.util.tabulate import tabulate

    if mapping_path is None and len(mappings) == 0:
        raise errors.UsageError("No mapping has been given for the iteration!")

    workflow = plan_gateway.get_by_name_or_id(name_or_id)

    if is_plan_removed(workflow):
        raise errors.ParameterError(f"The specified workflow '{name_or_id}' cannot be found.")

    tag_separator = "@"
    index_pattern = re.compile(r"{iter_index}")

    iter_params: Optional[Dict[str, Any]] = {"indexed": {}, "params": {}, "tagged": {}}
    workflow_params = {}
    if mapping_path:
        mapping = safe_read_yaml(mapping_path)
        iter_params, workflow_params = _extract_iterate_parameters(mapping, index_pattern, tag_separator=tag_separator)

    for m in mappings:
        param_name, param_value = m.split("=", maxsplit=1)
        if index_pattern.search(param_value):
            iter_params["indexed"][param_name] = param_value  # type: ignore
        else:
            try:
                param_value = ast.literal_eval(param_value)
            except Exception:
                raise errors.ParameterError(
                    f"The value of '{param_name}' parameter is neither a list nor templated variable!"
                )

            if isinstance(param_value, list) and len(param_value) == 1:
                communication.warn(
                    f"The parameter '{param_name}' has only one element '{param_value}', "
                    "changing it to be a fixed parameter!"
                )
                workflow_params[param_name] = param_value[0]
                continue
            elif not isinstance(param_value, list):
                workflow_params[param_name] = param_value
                continue

            if tag_separator in param_name:
                name, tag = param_name.split(tag_separator, maxsplit=1)
                if tag in iter_params["tagged"]:
                    iter_params["tagged"][tag][name] = param_value
                else:
                    iter_params["tagged"][tag] = {name: param_value}

                param_name = name
            else:
                iter_params["params"][param_name] = param_value

        set_param = reduce(lambda x, y: {y: x}, reversed(param_name.split(".")), param_value)  # type: ignore
        workflow_params = always_merger.merge(workflow_params, set_param)

    iter_params = _validate_iterate_parameters(workflow, workflow_params, cast(Dict[str, Any], iter_params))
    if iter_params is None:
        return

    plans, execute_plan = _build_iterations(workflow, workflow_params, iter_params, index_pattern)

    communication.echo(f"\n\n{tabulate(execute_plan, execute_plan[0].keys())}")
    if not dry_run:
        graph = ExecutionGraph(workflows=plans, virtual_links=True)
        execute_workflow_graph(dag=graph.workflow_graph, provider=provider, config=config)
