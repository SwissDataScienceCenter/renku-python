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
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.metadata.database import Database
from renku.core.models.workflow.converters.cwl import CWLConverter
from renku.core.models.workflow.grouped_run import GroupedRun
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
    keywords: List[str],
    workflows: List[str],
    database: Database,
):
    """Group workflows into a GroupedRun."""

    if database.get("plans-by-name").get(name):
        raise errors.ParameterError(f"Duplicate workflow name: workflow '{name}' already exists.")

    child_workflows = []

    for workflow_name_or_id in workflows:
        child_workflow = database.get("plans").get(workflow_name_or_id)

        if not child_workflow:
            child_workflow = database.get("plans-by-name").get(workflow_name_or_id)

        if not child_workflow:
            raise errors.ObjectNotFoundError(workflow_name_or_id)

        child_workflows.append(child_workflow)

    run = GroupedRun(
        description=description, id=GroupedRun.generate_id(), keywords=keywords, name=name, plans=child_workflows
    )

    if mappings:
        run.set_mappings_from_strings(mappings)

    if defaults:
        run.set_mapping_defaults(defaults)

    if links:
        run.set_links_from_strings(links)
        graph = ExecutionGraph(run)
        cycles = graph.cycles
        if cycles:
            raise errors.GraphCycleError(cycles)

    if param_descriptions:
        run.set_mapping_descriptions(param_descriptions)

    if map_inputs:
        run.map_all_inputs()

    if map_outputs:
        run.map_all_outputs()

    if map_params:
        run.map_all_parameters()

    database.get("plans")[run.id] = run
    database.get("plans-by-name")[run.name] = run


def group_workflow_command():
    """Command that creates a group of several workflows."""
    return (
        Command().command(_group_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )
