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


from collections import defaultdict
from pathlib import Path
from typing import List

from renku.core import errors
from renku.core.commands.graph import Graph
from renku.core.commands.view_model.composite_plan import CompositePlanViewModel
from renku.core.commands.view_model.plan import PlanViewModel
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.management.workflow.value_resolution import apply_run_values
from renku.core.metadata.database import Database
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.converters.cwl import CWLConverter
from renku.core.utils import communication


def _ref(name):
    """Return workflow reference name."""
    return "workflows/{0}".format(name)


def _deref(ref):
    """Remove workflows prefix."""
    assert ref.startswith("workflows/")
    return ref[len("workflows/") :]


@inject.autoparams()
def _list_workflows(client: LocalClient):
    """List or manage workflows with subcommands."""
    from renku.core.models.refs import LinkReference

    names = defaultdict(list)
    for ref in LinkReference.iter_items(common_path="workflows"):
        names[ref.reference.name].append(ref.name)

    for path in client.workflow_path.glob("*.yaml"):
        communication.echo(
            "{path}: {names}".format(path=path.name, names=", ".join(_deref(name) for name in names[path.name]))
        )


def list_workflows_command():
    """Command to list or manage workflows with subcommands."""
    return Command().command(_list_workflows).require_migration()


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
def _remove_workflow(name, client: LocalClient):
    """Remove the remote named <name>."""
    from renku.core.models.refs import LinkReference

    LinkReference(client=client, name=_ref(name)).delete()


def remove_workflow_command():
    """Command that removes the remote named <name>."""
    return Command().command(_remove_workflow).require_clean().with_commit()


@inject.autoparams()
def _create_workflow(output_file, revision, paths, client: LocalClient):
    """Create a workflow description for a file."""
    graph = Graph()
    outputs = graph.build(paths=paths, revision=revision)

    workflow = graph.as_workflow(outputs=outputs)

    if output_file:
        output_file = Path(output_file)

    wf, path = CWLConverter.convert(workflow, client.path, path=output_file)

    return wf.export_string()


def create_workflow_command():
    """Command that create a workflow description for a file."""
    return Command().command(_create_workflow)


@inject.autoparams()
def _show_workflow(name_or_id: str, database: Database):
    """Show the details of a workflow."""
    workflow = database["plans"].get(name_or_id)

    if not workflow:
        workflow = database["plans-by-name"].get(name_or_id)

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
    database: Database,
) -> CompositePlan:
    """Group workflows into a CompositePlan."""

    if database["plans-by-name"].get(name):
        raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    child_workflows = []

    for workflow_name_or_id in steps:
        child_workflow = database["plans"].get(workflow_name_or_id)

        if not child_workflow:
            child_workflow = database["plans-by-name"].get(workflow_name_or_id)

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

    database["plans"].add(plan)
    database["plans-by-name"].add(plan)

    return CompositePlanViewModel.from_composite_plan(plan)


def compose_workflow_command():
    """Command that creates a group of several workflows."""
    return (
        Command().command(_group_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )
