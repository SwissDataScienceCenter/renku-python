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
from typing import Dict, List, Optional

from renku.core import errors
from renku.core.commands.view_model.composite_plan import CompositePlanViewModel
from renku.core.commands.view_model.plan import PlanViewModel
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.value_resolution import apply_run_values
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.parameter import CommandParameter
from renku.core.utils import communication


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


@inject.autoparams()
def _list_workflows(plan_gateway: IPlanGateway):
    """List or manage workflows with subcommands."""
    workflows = plan_gateway.get_newest_plans_by_names()
    for name in workflows.keys():
        communication.echo(f"{name}")


def list_workflows_command():
    """Command to list or manage workflows with subcommands."""
    return Command().command(_list_workflows).require_migration().with_database(write=False)


@inject.autoparams()
def _set_workflow_name(name, path, force, client: LocalClient):
    """Sets the <name> for remote <path>."""
    from renku.core.models.refs import LinkReference

    LinkReference.create(client=client, name=_ref(name), force=force).set_reference(path)


def set_workflow_name_command():
    """Command that sets the <name> for remote <path>."""
    return Command().command(_set_workflow_name).require_clean().with_commit()


@inject.autoparams()
def _rename_workflow(old, new, force, client: LocalClient):
    """Rename the workflow named <old> to <new>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(old)).rename(_ref(new), force=force)


def rename_workflow_command():
    """Command that renames the workflow named <old> to <new>."""
    return Command().command(_rename_workflow).require_clean().with_commit()


@inject.autoparams()
def _remove_workflow(name, force, plan_gateway: IPlanGateway):
    """Remove the remote named <name>."""
    workflows = plan_gateway.get_newest_plans_by_names()
    not_found_text = f'The specified workflow is "{name}" is not an active workflow.'
    plan = None
    if name.startswith("/plans/"):
        plan = next(filter(lambda x: x.id == name, workflows.values()), None)
    if not plan and name not in workflows:
        raise errors.ParameterError(not_found_text)

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


@inject.autoparams()
def _create_workflow(output_file, revision, paths, client: LocalClient):
    """Create a workflow description for a file."""
    pass
    # TODO: implement with new database
    # graph = Graph()
    # outputs = graph.build(paths=paths, revision=revision)

    # workflow = graph.as_workflow(outputs=outputs)

    # if output_file:
    #     output_file = Path(output_file)

    # wf, path = CWLConverter.convert(workflow, client.path, path=output_file)

    # return wf.export_string()


def create_workflow_command():
    """Command that create a workflow description for a file."""
    return Command().command(_create_workflow)


@inject.autoparams()
def _show_workflow(name_or_id: str, plan_gateway: IPlanGateway):
    """Show the details of a workflow."""
    workflow = plan_gateway.get_by_id(name_or_id)

    if not workflow:
        workflow = plan_gateway.get_by_name(name_or_id)

    if isinstance(workflow, CompositePlan):
        return CompositePlanViewModel.from_composite_plan(workflow)

    return PlanViewModel.from_plan(workflow)


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
    set_params: Dict[str, str],
    map_params: List[str],
    rename_params: Dict[str, str],
    describe_params: Dict[str, str],
    plan_gateway: IPlanGateway,
):
    """Edits a workflow details."""
    workflow = plan_gateway.get_by_name(name)
    if not workflow:
        errors.ParameterError(f'The specified workflow is "{name}" is not an active workflow.')

    workflow._v_immutable = False
    if new_name:
        workflow.name = new_name

    if description:
        workflow.description = description

    for name, value in set_params.items():
        for param in workflow.parameters:
            if param.name == name:
                param.default_value = value
                break
        else:
            workflow.parameters.append(
                CommandParameter(default_value=value, id=CommandParameter.generate_id(plan_id=workflow.id), name=name)
            )

    if len(map_params) and isinstance(workflow, CompositePlan):
        workflow.set_mappings_from_strings(map_params)

    for name, new_name in rename_params.items():
        for param in workflow.parameters:
            if param.name == name:
                param.name = new_name
                break

    for name, descirption in describe_params.items():
        for param in workflow.parameters:
            if param.name == name:
                param.description = description
                break

    workflow.freeze()


def edit_workflow_command():
    """Command that edits the properties of a given workflow."""
    return Command().command(_edit_workflow).require_clean().with_database(write=True).with_commit()
