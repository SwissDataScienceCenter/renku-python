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
"""Renku workflow commands."""


from renku.command.command_builder.command import Command
from renku.core.workflow.activity import revert_activity
from renku.core.workflow.execute import execute_workflow, iterate_workflow
from renku.core.workflow.plan import (
    compose_workflow,
    edit_workflow,
    export_workflow,
    list_workflows,
    remove_plan,
    search_workflows,
    show_workflow,
    visualize_graph,
    workflow_inputs,
    workflow_outputs,
)


def search_workflows_command():
    """Command to get all the workflows whose Plan.name are greater than or equal to the given name."""
    return Command().command(search_workflows).require_migration().with_database(write=False)


def list_workflows_command():
    """Command to list or manage workflows with subcommands."""
    return Command().command(list_workflows).require_migration().with_database(write=False)


def remove_plan_command():
    """Command that removes the workflow named <name>."""
    return Command().command(remove_plan).require_clean().with_database(write=True).with_commit()


def show_workflow_command():
    """Command that the details of a workflow."""
    return Command().command(show_workflow).require_migration().with_database(write=False)


def compose_workflow_command():
    """Command that creates a composite of several workflows."""
    return (
        Command().command(compose_workflow).require_migration().require_clean().with_database(write=True).with_commit()
    )


def edit_workflow_command():
    """Command that edits the properties of a given workflow."""
    return Command().command(edit_workflow).require_clean().with_database(write=True).with_commit()


def export_workflow_command():
    """Command that exports a workflow into a given format."""
    return Command().command(export_workflow).require_migration().with_database(write=False)


def workflow_inputs_command():
    """Command that shows inputs used by workflows."""
    return Command().command(workflow_inputs).require_migration().with_database(write=False)


def workflow_outputs_command():
    """Command that shows inputs used by workflows."""
    return Command().command(workflow_outputs).require_migration().with_database(write=False)


def execute_workflow_command(skip_metadata_update: bool):
    """Command that executes a workflow."""
    command = Command().command(execute_workflow).require_migration()
    if skip_metadata_update:
        command = command.with_database(write=False)
    else:
        command = command.with_database(write=True).with_commit()
    return command


def visualize_graph_command():
    """Execute the graph visualization command."""
    return Command().command(visualize_graph).require_migration().with_database(write=False)


def iterate_workflow_command(skip_metadata_update: bool):
    """Command that executes several workflows given a set of variables."""
    command = Command().command(iterate_workflow).require_migration().require_clean()
    if skip_metadata_update:
        command = command.with_database(write=False)
    else:
        command = command.with_database(write=True).with_commit()
    return command


def revert_activity_command():
    """Command that reverts an activity."""
    return (
        Command().command(revert_activity).require_migration().require_clean().with_database(write=True).with_commit()
    )
