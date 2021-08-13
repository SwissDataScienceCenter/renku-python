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


from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from renku.core import errors
from renku.core.commands.format.workflow import WORKFLOW_FORMATS
from renku.core.commands.view_model import plan_view
from renku.core.commands.view_model.composite_plan import CompositePlanViewModel
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.value_resolution import apply_run_values
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import AbstractPlan, Plan
from renku.core.utils import communication


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


@inject.autoparams()
def _find_workflow(name_or_id: str, plan_gateway: IPlanGateway) -> AbstractPlan:
    workflow = plan_gateway.get_by_id(name_or_id)

    if not workflow:
        workflow = plan_gateway.get_by_name(name_or_id)

    if not workflow:
        raise errors.ParameterError(f'The specified workflow "{name_or_id}" cannot be found.')
    return workflow


@inject.autoparams()
def _list_workflows(plan_gateway: IPlanGateway, format: str, columns: List[str]):
    """List or manage workflows with subcommands."""
    workflows = plan_gateway.get_newest_plans_by_names()

    if format not in WORKFLOW_FORMATS:
        raise errors.UsageError(f'Provided format "{format}" is not supported ({", ".join(WORKFLOW_FORMATS.keys())})"')

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
def _group_workflow(
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
    plan_gateway: IPlanGateway,
) -> CompositePlan:
    """Group workflows into a CompositePlan."""

    if plan_gateway.get_by_name(name):
        raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    child_workflows = []

    for workflow_name_or_id in steps:
        child_workflow = plan_gateway.get_by_id(workflow_name_or_id)

        if not child_workflow:
            child_workflow = plan_gateway.get_by_name(workflow_name_or_id)

        if not child_workflow:
            raise errors.ObjectNotFoundError(workflow_name_or_id)

        child_workflows.append(child_workflow)

    plan = CompositePlan(
        description=description, id=CompositePlan.generate_id(), keywords=keywords, name=name, plans=child_workflows
    )

    if mappings:
        plan.set_mappings_from_strings(mappings)

    if defaults:
        plan.set_mapping_defaults(defaults)

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
        apply_run_values(plan)

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
    """Command that creates a group of several workflows."""
    return (
        Command().command(_group_workflow).require_migration().require_clean().with_database(write=True).with_commit()
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
def _export_workflow(name_or_id, client: LocalClient, format: str, output: Optional[str], values: Optional[str]):
    """Export a workflow to a given format."""

    workflow = _find_workflow(name_or_id)
    if output:
        output = Path(output)

    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    supported_formats = pm.hook.workflow_format()
    export_plugins = list(map(lambda x: x[0], supported_formats))
    converter = list(map(lambda x: x[0], filter(lambda x: format in x[1], supported_formats)))
    if not any(converter):
        raise errors.ParameterError(f"The specified workflow exporter format '{format}' is not available.")
    elif len(converter) > 1:
        raise errors.ConfigurationError(
            f"The specified format '{format}' is supported by more than one export plugins!"
        )

    if values:
        from renku.core.models import jsonld as jsonld

        values = jsonld.read_yaml(values)
        workflow = apply_run_values(workflow, values)

    export_plugins.remove(converter[0])
    converter = pm.subset_hook_caller("workflow_convert", export_plugins)
    return converter(workflow=workflow, basedir=client.path, output=output, output_format=format)


def export_workflow_command():
    """Command that exports a workflow into a given format."""
    return Command().command(_export_workflow).require_clean().with_database(write=False)
