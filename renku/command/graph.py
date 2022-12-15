# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Knowledge graph building."""

import json
from typing import Dict, List, Optional, Set, Union

from pydantic import validate_arguments

from renku.command.command_builder.command import Command, inject
from renku.command.schema.activity import ActivitySchema, WorkflowFileActivityCollectionSchema
from renku.command.schema.composite_plan import CompositePlanSchema
from renku.command.schema.dataset import DatasetSchema, DatasetTagSchema
from renku.command.schema.plan import PlanSchema
from renku.command.schema.project import ProjectSchema
from renku.command.schema.workflow_file import WorkflowFileCompositePlanSchema, WorkflowFilePlanSchema
from renku.command.view_model.graph import GraphViewModel
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util.shacl import validate_graph
from renku.core.util.urls import get_host
from renku.domain_model.dataset import Dataset, DatasetTag
from renku.domain_model.project import Project
from renku.domain_model.provenance.activity import Activity, WorkflowFileActivityCollection
from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.plan import AbstractPlan, Plan
from renku.domain_model.workflow.workflow_file import WorkflowFileCompositePlan, WorkflowFilePlan

try:
    import importlib_resources  # type: ignore[import]
except ImportError:
    import importlib.resources as importlib_resources  # type: ignore


def export_graph_command():
    """Return a command for exporting graph data."""
    return Command().command(export_graph).with_database(write=False).require_migration()


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def export_graph(
    format: str = "json-ld", revision_or_range: Optional[str] = None, strict: bool = False
) -> GraphViewModel:
    """Output graph in specific format.

    Args:
        format(str, optional): Output format (Default value = "json-ld").
        revision_or_range(str, optional): Revision or range of revisions to export for (Default value = None).
        strict(bool, optional): Whether to check generated JSON-LD against the SHACL schema (Default value = False).

    Returns:
        Renku metadata as string.
    """

    format = format.lower()

    if revision_or_range:
        graph = get_graph_for_revision(revision_or_range=revision_or_range)
    else:
        graph = get_graph_for_all_objects()

    # NOTE: rewrite ids for current environment
    host = get_host()

    for node in graph:
        update_nested_node_host(node, host)

    if strict:
        _validate_graph(json.dumps(graph, indent=2), "json-ld")

    return GraphViewModel(graph)


def update_nested_node_host(node: Dict, host: str) -> None:
    """Update all @id in a node to include host if necessary.

    Args:
        node(Dict): Node to update.
        host(str): Host to set.
    """
    for k, v in node.items():
        if k == "@id":
            if not v.startswith("http") and not v.startswith("mailto"):
                node[k] = f"https://{host}{v}"
        elif isinstance(v, dict):
            update_nested_node_host(v, host)
        elif isinstance(v, list):
            for entry in v:
                if isinstance(entry, dict):
                    update_nested_node_host(entry, host)


@inject.autoparams()
def get_graph_for_revision(
    revision_or_range: str,
    database_gateway: IDatabaseGateway,
    project_gateway: IProjectGateway,
) -> List[Dict]:
    """Get the graph for changes made in a specific revision.

    Args:
        revision_or_range(str): Revision or range of revisions to export for.
        database_gateway(IDatabaseGateway): Injected database gateway.
        project_gateway(IProjectGateway): Injected project gateway.

    Returns:
        List of JSON-LD metadata.
    """
    all_objects = database_gateway.get_modified_objects_from_revision(revision_or_range=revision_or_range)

    change_types = (Project, Dataset, DatasetTag, Activity, Plan, CompositePlan)

    changed_objects = []

    for obj in all_objects:
        if isinstance(obj, change_types):
            changed_objects.append(obj)

    project = project_gateway.get_project()

    return _convert_entities_to_graph(changed_objects, project)


@inject.autoparams()
def get_graph_for_all_objects(
    project_gateway: IProjectGateway,
    dataset_gateway: IDatasetGateway,
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
) -> List[Dict]:
    """Get JSON-LD graph for all entities.

    Args:
        project_gateway(IProjectGateway): Injected project gateway.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
        activity_gateway(IActivityGateway): Injected activity gateway.
        plan_gateway(IPlanGateway): Injected plan gateway.

    Returns:
        List of JSON-LD metadata.
    """
    project = project_gateway.get_project()
    # NOTE: Include deleted activities when exporting graph
    objects: List[Union[Project, Dataset, DatasetTag, Activity, AbstractPlan, WorkflowFileActivityCollection]]

    objects = activity_gateway.get_all_activities(include_deleted=True)

    workflow_file_executions = [
        a for a in activity_gateway.get_all_activity_collections() if isinstance(a, WorkflowFileActivityCollection)
    ]
    objects.extend(workflow_file_executions)

    processed_plans = set()

    for activity in objects:
        processed_plans |= get_activity_plan_ids(activity)

    plans = [p for p in plan_gateway.get_all_plans() if p.id not in processed_plans]

    objects.extend(plans)

    objects.append(project)

    datasets = dataset_gateway.get_all_active_datasets()
    objects.extend(datasets)

    for dataset in datasets:
        objects.extend(dataset_gateway.get_all_tags(dataset))

        current_dataset = dataset
        while current_dataset.is_derivation():
            assert current_dataset.derived_from is not None
            current_dataset = dataset_gateway.get_by_id(current_dataset.derived_from.value)
            objects.append(current_dataset)

    return _convert_entities_to_graph(objects, project)


def _convert_entities_to_graph(
    entities: List[Union[Project, Dataset, DatasetTag, Activity, AbstractPlan, WorkflowFileActivityCollection]],
    project: Project,
) -> List[Dict]:
    """Convert entities to JSON-LD graph.

    Args:
        entities(List[Union[Project, Dataset, DatasetTag, Activity, Plan, CompositePlan]]): Entities to convert.
        project(Project): Current project.

    Returns:
        List of JSON-LD metadata.
    """
    graph = []
    schemas = {
        Project: ProjectSchema,
        Dataset: DatasetSchema,
        DatasetTag: DatasetTagSchema,
        Activity: ActivitySchema,
        WorkflowFilePlan: WorkflowFilePlanSchema,
        Plan: PlanSchema,
        WorkflowFileCompositePlan: WorkflowFileCompositePlanSchema,
        CompositePlan: CompositePlanSchema,
        WorkflowFileActivityCollection: WorkflowFileActivityCollectionSchema,
    }

    processed_plans = set()
    project_id = project.id

    for entity in entities:
        if entity.id in processed_plans:
            continue
        if isinstance(entity, (Dataset, Activity, AbstractPlan, WorkflowFileActivityCollection)):
            # NOTE: Since the database is read-only, it's OK to modify objects; they won't be written back
            entity.unfreeze()
            entity.project_id = project_id

            if isinstance(entity, (Activity, WorkflowFileActivityCollection)):
                entity.association.plan.unfreeze()
                entity.association.plan.project_id = project_id
        schema = next(s for t, s in schemas.items() if isinstance(entity, t))
        graph.extend(schema(flattened=True).dump(entity))

        if not isinstance(entity, (Activity, WorkflowFileActivityCollection)):
            continue

        # NOTE: mark activity plans as processed
        processed_plans |= get_activity_plan_ids(entity)

    return graph


def get_activity_plan_ids(activity: Activity) -> Set[str]:
    """Get the ids of all plans associated with an activity.

    Args:
        activity(Activity): Activity tog et Plans for.

    Returns:
        Set of ids of Plans contained in Activity.
    """
    plan_stack = [activity.association.plan]
    plan_ids = set()

    while plan_stack:
        plan = plan_stack.pop()

        plan_ids.add(plan.id)

        if isinstance(plan, CompositePlan):
            plan_stack.extend(plan.plans)

    return plan_ids


def _validate_graph(rdf_graph, format):
    ref = importlib_resources.files("renku.data") / "shacl_shape.json"
    with importlib_resources.as_file(ref) as shacl_path:
        r, _, t = validate_graph(rdf_graph, shacl_path=shacl_path, format=format)

    if not r:
        raise errors.SHACLValidationError(f"{t}\nCouldn't export: Invalid Knowledge Graph data")
