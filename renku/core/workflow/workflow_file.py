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
"""Workflow file core logic."""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import List, Optional, Union

import networkx as nx
from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.plugin.workflow_file_parser import read_workflow_file
from renku.core.util import communication
from renku.core.util.os import is_subpath
from renku.core.workflow.execute import check_for_cycles, execute_workflow_graph
from renku.core.workflow.model.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.model.workflow_file import WorkflowFile
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan, WorkflowFilePlan


@inject.params(plan_gateway=IPlanGateway)
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def run_workflow_file(
    path: Union[Path, str],
    steps: List[str],
    dry_run: bool,
    workflow_file: Optional[WorkflowFile],
    provider: str,
    plan_gateway: IPlanGateway,
):
    """Run a workflow file."""
    from renku.command.view_model.workflow_file import WorkflowFileViewModel

    try:
        if not dry_run and not is_subpath(path=path, base=project_context.path):
            raise errors.ParameterError("Workflow file must be inside the project for execution")
    except errors.ProjectContextError:
        pass

    workflow_file = workflow_file or read_workflow_file(path=path, parser="renku")
    workflow = workflow_file.to_plan()

    # NOTE: Check workflow file name is unique
    existing_plan = plan_gateway.get_by_name(workflow.name)
    if existing_plan:
        if not isinstance(existing_plan, WorkflowFileCompositePlan) or existing_plan.path != workflow.path:
            raise errors.DuplicateWorkflowNameError(
                f"Duplicate workflow file name: Workflow '{workflow.name}' already exists."
            )

    # NOTE: Filter steps after calculating derivatives so that the root plan (that has a subset of steps) isn't
    # considered as a new version of the plan.
    selected_steps = filter_steps(workflow=workflow, steps=steps) if steps else [workflow]

    graph = ExecutionGraph(workflows=selected_steps, virtual_links=True)

    check_for_cycles(graph=graph)

    if not dry_run:
        # NOTE: We pass the non-filtered plan to be tracked; we can detect that a subset of steps were executed by
        # looking at the number of generated activities.
        execute_workflow_graph(dag=graph.workflow_graph, provider=provider, workflow_file_plan=workflow)
        dry_run_executions = []
    else:
        plans = list(nx.topological_sort(graph.workflow_graph))
        plan_step_mapping = {p: s for p in plans for s in workflow_file.steps if p.name == s.qualified_name}
        executed_steps = [plan_step_mapping[plan] for plan in plans]
        dry_run_executions = [f"Will execute step '{s.name}': {s.original_command}" for s in executed_steps]

    selected_steps_names = [s.name for s in selected_steps]
    workflow_file.steps = [s for s in workflow_file.steps if s.name in selected_steps_names]

    return WorkflowFileViewModel.from_workflow_file(workflow_file), dry_run_executions


def filter_steps(workflow: WorkflowFileCompositePlan, steps: List[str]) -> List[WorkflowFilePlan]:
    """Return a subset of workflow file steps."""
    selected_steps = set(steps)
    if len(steps) != len(selected_steps):
        communication.warn("Duplicated steps will be included only once.")

    not_found = selected_steps - {s.unqualified_name for s in workflow.plans}
    if not_found:
        not_found_str = ", ".join(not_found)
        raise errors.ParameterError(f"Cannot find steps: {not_found_str}")

    return [s for s in workflow.plans if s.unqualified_name in selected_steps]


def get_all_workflow_file_inputs_and_outputs(workflow_file: WorkflowFile) -> List[str]:
    """Return a list of all inputs and outputs that must be committed."""
    return [io.path for step in workflow_file.steps for io in itertools.chain(step.inputs, step.outputs) if io.persist]
