# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Helpers functions for metadata management/parsing."""

import itertools
from collections import defaultdict
from collections.abc import Iterable
from typing import List, Set, Tuple, Union

import networkx
from git import Repo

from renku.core import errors
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity
from renku.core.models.provenance.agent import Person
from renku.core.utils.git import get_object_hash


def construct_creators(creators: List[Union[dict, str]], ignore_email=False):
    """Parse input and return a list of Person."""
    creators = creators or ()

    if not isinstance(creators, Iterable) or isinstance(creators, str):
        raise errors.ParameterError("Invalid creators type")

    people = []
    no_email_warnings = []
    for creator in creators:
        person, no_email_warning = construct_creator(creator, ignore_email=ignore_email)

        people.append(person)

        if no_email_warning:
            no_email_warnings.append(no_email_warning)

    return people, no_email_warnings


def construct_creator(creator: Union[dict, str], ignore_email):
    """Parse input and return an instance of Person."""
    if not creator:
        return None, None

    if isinstance(creator, str):
        person = Person.from_string(creator)
    elif isinstance(creator, dict):
        person = Person.from_dict(creator)
    else:
        raise errors.ParameterError("Invalid creator type")

    message = 'A valid format is "Name <email> [affiliation]"'

    if not person.name:  # pragma: no cover
        raise errors.ParameterError(f'Name is invalid: "{creator}".\n{message}')

    if not person.email:
        if not ignore_email:  # pragma: no cover
            raise errors.ParameterError(f'Email is invalid: "{creator}".\n{message}')
        else:
            no_email_warning = creator
    else:
        no_email_warning = None

    return person, no_email_warning


def get_modified_activities(
    activities: List[Activity], repo: Repo
) -> Tuple[Set[Tuple[Activity, Entity]], Set[Tuple[Activity, Entity]]]:
    """Get lists of activities that have modified/deleted usage entities."""
    modified = set()
    deleted = set()

    for activity in activities:
        for usage in activity.usages:
            entity = usage.entity
            current_checksum = get_object_hash(repo=repo, path=entity.path)
            if current_checksum is None:
                deleted.add((activity, entity))
            elif current_checksum != entity.checksum:
                modified.add((activity, entity))

    return modified, deleted


def add_activity_if_recent(activity: Activity, activities: Set[Activity]):
    """Add ``activity`` to ``activities`` if it's not in the set or is the latest executed instance."""
    if activity in activities:
        return

    for existing_activity in activities:
        if activity.has_identical_inputs_and_outputs_as(existing_activity):
            if activity.ended_at_time > existing_activity.ended_at_time:  # activity is newer
                activities.remove(existing_activity)
                activities.add(activity)
            return

    # NOTE: No similar activity was found
    activities.add(activity)


def sort_activities(activities: List[Activity], remove_overridden_parents=True) -> List[Activity]:
    """Returns a sorted list of activities based on their dependencies and execution order."""
    by_usage = defaultdict(set)
    by_generation = defaultdict(set)

    overridden_activities = defaultdict(set)

    graph = networkx.DiGraph()

    def connect_nodes_based_on_dependencies():
        for activity in activities:
            # NOTE: Make sure that activity is in the graph in case it has no connection to others
            graph.add_node(activity)

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

    def create_edge(parent, child, path: str):
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
            if networkx.has_path(graph, a, b) or networkx.has_path(graph, b, a):
                continue

            # NOTE: More recent activity should be executed after the other one
            # NOTE: This won't introduce a cycle in the graph because there is no other path between the two nodes
            comparison = compare_activities(a, b)
            if comparison < 0:
                graph.add_edge(a, b)
                overridden_activities[a].add(path)
            elif comparison > 0:
                graph.add_edge(b, a)
                overridden_activities[b].add(path)
            else:
                raise ValueError(f"Cannot create an order between activities {a.id} and {b.id}")

    def compare_activities(a: Activity, b: Activity) -> int:
        if a.ended_at_time < b.ended_at_time:
            return -1
        elif a.ended_at_time > b.ended_at_time:
            return 1
        elif a.started_at_time < b.started_at_time:
            return -1
        elif a.started_at_time > b.started_at_time:
            return 1

        return 0

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

    return list(networkx.topological_sort(graph))
