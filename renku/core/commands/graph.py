# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
from typing import Dict, List, Set, Union

from renku.core import errors
from renku.core.commands.format.graph import GRAPH_FORMATS
from renku.core.commands.schema.activity import ActivitySchema
from renku.core.commands.schema.composite_plan import CompositePlanSchema
from renku.core.commands.schema.dataset import DatasetSchema, DatasetTagSchema
from renku.core.commands.schema.plan import PlanSchema
from renku.core.commands.schema.project import ProjectSchema
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.models.dataset import Dataset, DatasetTag
from renku.core.models.project import Project
from renku.core.models.provenance.activity import Activity
from renku.core.models.workflow.composite_plan import CompositePlan
from renku.core.models.workflow.plan import AbstractPlan, Plan
from renku.core.utils.shacl import validate_graph
from renku.core.utils.urls import get_host

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources


def export_graph_command():
    """Return a command for exporting graph data."""
    return Command().command(_export_graph).with_database(write=False).require_migration()


@inject.autoparams("client_dispatcher")
def _export_graph(
    client_dispatcher: IClientDispatcher,
    format: str = "json-ld",
    revision_or_range: str = None,
    strict: bool = False,
):
    """Output graph in specific format."""

    format = format.lower()

    if revision_or_range:
        graph = _get_graph_for_revision(revision_or_range=revision_or_range)
    else:
        graph = _get_graph_for_all_objects()

    # NOTE: rewrite ids for current environment
    host = get_host(client_dispatcher.current_client)

    for node in graph:
        update_nested_node_host(node, host)

    if strict:
        _validate_graph(json.dumps(graph, indent=2), "json-ld")

    return GRAPH_FORMATS[format](graph)


def update_nested_node_host(node: Dict, host: str) -> None:
    """Update all @id in a node to include host if necessary."""
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
def _get_graph_for_revision(
    revision_or_range: str,
    database_gateway: IDatabaseGateway,
    project_gateway: IProjectGateway,
) -> List[Dict]:
    """Get the graph for changes made in a specific revision."""
    all_objects = database_gateway.get_modified_objects_from_revision(revision_or_range=revision_or_range)

    change_types = (Project, Dataset, DatasetTag, Activity, Plan, CompositePlan)

    changed_objects = []

    for obj in all_objects:
        if isinstance(obj, change_types):
            changed_objects.append(obj)

    project = project_gateway.get_project()

    return _convert_entities_to_graph(changed_objects, project)


@inject.autoparams()
def _get_graph_for_all_objects(
    project_gateway: IProjectGateway,
    dataset_gateway: IDatasetGateway,
    activity_gateway: IActivityGateway,
    plan_gateway: IPlanGateway,
) -> List[Dict]:
    """Get JSON-LD graph for all entities."""
    project = project_gateway.get_project()
    objects: List[Union[Project, Dataset, DatasetTag, Activity, AbstractPlan]] = activity_gateway.get_all_activities()

    processed_plans = set()

    for activity in objects:
        processed_plans |= get_activity_plan_ids(activity)

    plans = [p for p in plan_gateway.get_all_plans() if p.id not in processed_plans]

    objects.extend(plans)

    objects.append(project)

    datasets = dataset_gateway.get_all_datasets()
    objects.extend(datasets)

    for dataset in datasets:
        objects.extend(dataset_gateway.get_all_tags(dataset))

        current_dataset = dataset
        while current_dataset.derived_from:
            current_dataset = dataset_gateway.get_by_id(current_dataset.derived_from.url_id)
            objects.append(current_dataset)

    return _convert_entities_to_graph(objects, project)


def _convert_entities_to_graph(
    entities: List[Union[Project, Dataset, DatasetTag, Activity, Plan, CompositePlan]], project: Project
) -> List[Dict]:
    """Convert entities to JSON-LD graph."""
    graph = []
    schemas = {
        Project: ProjectSchema,
        Dataset: DatasetSchema,
        DatasetTag: DatasetTagSchema,
        Activity: ActivitySchema,
        Plan: PlanSchema,
        CompositePlan: CompositePlanSchema,
    }

    processed_plans = set()
    project_id = project.id

    for entity in entities:
        if entity.id in processed_plans:
            continue
        if isinstance(entity, (Dataset, Activity, AbstractPlan)):
            # NOTE: Since the database is read-only, it's OK to modify objects; they won't be written back
            entity.unfreeze()
            entity.project_id = project_id
        schema = next(s for t, s in schemas.items() if isinstance(entity, t))
        graph.extend(schema(flattened=True).dump(entity))

        if not isinstance(entity, Activity):
            continue

        # NOTE: mark activity plans as processed
        processed_plans |= get_activity_plan_ids(entity)

    return graph


def get_activity_plan_ids(activity: Activity) -> Set[str]:
    """Get the ids of all plans associated with an activity."""
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
