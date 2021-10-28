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
from collections import defaultdict
from typing import List, Optional, Set

import networkx

from renku.core import errors
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.models.provenance.activity import Activity
from renku.core.utils import communication
from renku.core.utils.metadata import add_activity_if_recent


def get_activities_until_paths(
    paths: List[str],
    sources: List[str],
    activity_gateway: IActivityGateway,
    client_dispatcher: IClientDispatcher,
    revision: Optional[str] = None,
) -> Set[Activity]:
    """Get all current activities leading to `paths`, from `sources`."""
    all_activities = defaultdict(set)

    def include_newest_activity(activity):
        existing_activities = all_activities[activity.association.plan.id]
        add_activity_if_recent(activity=activity, activities=existing_activities)

    commit = None

    if revision:
        client = client_dispatcher.current_client
        commit = client.repo.commit(revision)

    for path in paths:
        checksum = None
        if commit:
            try:
                blob = commit.tree / path
            except KeyError:
                raise errors.GitError(f"Couldn't find file {path} at revision {revision}")
            checksum = blob.hexsha

        activities = activity_gateway.get_activities_by_generation(path, checksum=checksum)

        if len(activities) == 0:
            communication.warn(f"Path '{path}' is not generated by any workflows.")
            continue

        latest_activity = max(activities, key=lambda a: a.ended_at_time)

        upstream_chains = activity_gateway.get_upstream_activity_chains(latest_activity)

        if sources:
            # NOTE: Add the activity to check if it also matches the condition
            upstream_chains.append((latest_activity,))
            # NOTE: Only include paths that is using at least one of the sources
            upstream_chains = [c for c in upstream_chains if any(u.entity.path in sources for u in c[-1].usages)]

            # NOTE: Include activity only if any of its upstream match the condition
            if upstream_chains:
                include_newest_activity(latest_activity)
        else:
            include_newest_activity(latest_activity)

        for chain in upstream_chains:
            for activity in chain:
                include_newest_activity(activity)

    return {a for activities in all_activities.values() for a in activities}


def create_activity_graph(
    activities: List[Activity], remove_overridden_parents=True, with_inputs_outputs=False
) -> networkx.Graph:
    """Create a dependency DAG from activities."""
    by_usage = defaultdict(set)
    by_generation = defaultdict(set)

    overridden_activities = defaultdict(set)

    graph = networkx.DiGraph()

    def connect_nodes_based_on_dependencies():
        for activity in activities:
            # NOTE: Make sure that activity is in the graph in case it has no connection to others
            graph.add_node(activity)
            if with_inputs_outputs:
                create_input_output_edges(activity)
            for usage in activity.usages:
                path = usage.entity.path
                by_usage[path].add(activity)
                parent_activities = by_generation[path]
                for parent in parent_activities:
                    create_edge(parent, activity, path)

            for generation in activity.generations:
                path = generation.entity.path
                by_generation[path].add(activity)
                child_activities = by_usage[path]
                for child in child_activities:
                    create_edge(activity, child, path)

    def create_input_output_edges(activity):
        for generation in activity.generations:
            path = generation.entity.path
            if not graph.has_node(path):
                graph.add_node(path)
            if not graph.has_edge(activity, path):
                graph.add_edge(activity, path)
        for usage in activity.usages:
            path = usage.entity.path
            if not graph.has_node(path):
                graph.add_node(path)
            if not graph.has_edge(path, activity):
                graph.add_edge(path, activity)

    def create_edge(parent, child, path: str):
        if with_inputs_outputs:
            if not graph.has_edge(parent, path):
                graph.add_edge(parent, path)
            if not graph.has_edge(path, child):
                graph.add_edge(path, child)
        else:
            if graph.has_edge(parent, child):
                return
            graph.add_edge(parent, child, path=path)

    def connect_nodes_by_execution_order():
        for path, values in by_generation.items():
            if len(values) <= 1:
                continue

            # NOTE: Order multiple activities that generate a common path
            create_order_among_activities(values, path)

    def create_order_among_activities(activities: Set[Activity], path):
        for a, b in itertools.combinations(activities, 2):
            if (networkx.has_path(graph, a, b) and path in overridden_activities[a]) or (
                networkx.has_path(graph, b, a) and path in overridden_activities[b]
            ):
                continue

            # NOTE: More recent activity should be executed after the other one
            # NOTE: This won't introduce a cycle in the graph because there is no other path between the two nodes
            comparison = a.compare_to(b)
            if comparison < 0:
                if not networkx.has_path(graph, a, b):
                    graph.add_edge(a, b)
                overridden_activities[a].add(path)
            elif comparison > 0:
                if not networkx.has_path(graph, b, a):
                    graph.add_edge(b, a)
                overridden_activities[b].add(path)
            else:
                raise ValueError(f"Cannot create an order between activities {a.id} and {b.id}")

    def remove_overridden_activities():
        to_be_removed = set()
        to_be_processed = set(overridden_activities.keys())

        while len(to_be_processed) > 0:
            activity = to_be_processed.pop()
            overridden_paths = overridden_activities[activity]
            generated_path = {g.entity.path for g in activity.generations}
            if generated_path != overridden_paths:
                continue

            # NOTE: All generated paths are overridden; there is no point in executing the activity
            to_be_removed.add(activity)

            if not remove_overridden_parents:
                continue

            # NOTE: Check if its parents can be removed as well
            for parent in graph.predecessors(activity):
                if parent in to_be_removed:
                    continue
                data = graph.get_edge_data(parent, activity)
                if data and "path" in data:
                    overridden_activities[parent].add(data["path"])
                    to_be_processed.add(parent)

        for activity in to_be_removed:
            graph.remove_node(activity)

    connect_nodes_based_on_dependencies()

    if not networkx.algorithms.dag.is_directed_acyclic_graph(graph):
        raise ValueError("Cannot find execution order: Project has cyclic dependencies.")

    connect_nodes_by_execution_order()
    remove_overridden_activities()

    return graph


def sort_activities(activities: List[Activity], remove_overridden_parents=True) -> List[Activity]:
    """Returns a sorted list of activities based on their dependencies and execution order."""
    graph = create_activity_graph(activities, remove_overridden_parents)

    return list(networkx.topological_sort(graph))
