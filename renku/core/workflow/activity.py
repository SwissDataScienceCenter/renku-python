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
"""Activity management."""

import itertools
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

import networkx

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.util import communication
from renku.core.util.datetime8601 import local_now
from renku.core.workflow.plan import get_activities, is_plan_removed, remove_plan
from renku.domain_model.entity import Entity
from renku.domain_model.provenance.activity import Activity


def get_activities_until_paths(
    paths: List[str],
    sources: List[str],
    activity_gateway: IActivityGateway,
    client_dispatcher: IClientDispatcher,
    revision: Optional[str] = None,
) -> Set[Activity]:
    """Get all current activities leading to `paths`, from `sources`."""
    all_activities: Dict[str, Set[Activity]] = defaultdict(set)

    def include_newest_activity(activity):
        existing_activities = all_activities[activity.association.plan.id]
        add_activity_if_recent(activity=activity, activities=existing_activities)

    commit = None

    if revision:
        client = client_dispatcher.current_client
        commit = client.repository.get_commit(revision)

    for path in paths:
        checksum = None
        if commit:
            try:
                blob = commit.tree[path]
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
    by_usage: Dict[str, Set[Activity]] = defaultdict(set)
    by_generation: Dict[str, Set[Activity]] = defaultdict(set)

    overridden_activities: Dict[Activity, Set[str]] = defaultdict(set)

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

    cycles = list(networkx.algorithms.cycles.simple_cycles(graph))

    if cycles:
        cycles = [map(lambda x: getattr(x, "id", x), cycle) for cycle in cycles]
        raise errors.GraphCycleError(cycles)

    connect_nodes_by_execution_order()
    remove_overridden_activities()

    return graph


def sort_activities(activities: List[Activity], remove_overridden_parents=True) -> List[Activity]:
    """Return a sorted list of activities based on their dependencies and execution order."""
    graph = create_activity_graph(activities, remove_overridden_parents)

    return list(networkx.topological_sort(graph))


@inject.autoparams()
def get_all_modified_and_deleted_activities_and_entities(
    repository, activity_gateway: IActivityGateway
) -> Tuple[Set[Tuple[Activity, Entity]], Set[Tuple[Activity, Entity]]]:
    """
    Return latest activities with at least one modified or deleted input along with the modified/deleted input entity.

    An activity can be repeated if more than one of its inputs are modified.

    Args:
        repository: The current ``Repository``.
        activity_gateway(IActivityGateway): The injected Activity gateway.

    Returns:
        Tuple[Set[Tuple[Activity, Entity]], Set[Tuple[Activity, Entity]]]: Tuple of modified and deleted
            activities and entities.

    """
    all_activities = activity_gateway.get_all_activities()
    relevant_activities = filter_overridden_activities(all_activities)
    return get_modified_activities(activities=relevant_activities, repository=repository)


@inject.autoparams()
def get_downstream_generating_activities(
    starting_activities: Set[Activity],
    paths: List[str],
    ignore_deleted: bool,
    client_path: Path,
    activity_gateway: IActivityGateway,
) -> List[Activity]:
    """Return activities downstream of passed activities that generate at least a path in ``paths``.

    Args:
        starting_activities(Set[Activity]): Activities to use as starting/upstream nodes.
        paths(List[str]): Optional generated paths to end downstream chains at.
        ignore_deleted(bool): Whether to ignore deleted generations.
        client_path(Path): Path to project's root directory.
        activity_gateway(IActivityGateway): The injected Activity gateway.

    Returns:
        Set[Activity]: All activities and their downstream activities.

    """
    all_activities: Dict[str, Set[Activity]] = defaultdict(set)

    def include_newest_activity(activity):
        existing_activities = all_activities[activity.association.plan.id]
        add_activity_if_recent(activity=activity, activities=existing_activities)

    def does_activity_generate_any_paths(activity) -> bool:
        is_same = any(g.entity.path in paths for g in activity.generations)
        is_parent = any(Path(p) in Path(g.entity.path).parents for p in paths for g in activity.generations)

        return is_same or is_parent

    def has_an_existing_generation(activity) -> bool:
        for generation in activity.generations:
            if (client_path / generation.entity.path).exists():
                return True

        return False

    for starting_activity in starting_activities:
        downstream_chains = activity_gateway.get_downstream_activity_chains(starting_activity)

        if paths:
            # NOTE: Add the activity to check if it also matches the condition
            downstream_chains.append((starting_activity,))
            downstream_chains = [c for c in downstream_chains if does_activity_generate_any_paths(c[-1])]
            # NOTE: Include activity only if any of its downstream matched the condition
            include_starting_activity = len(downstream_chains) > 0
        elif ignore_deleted:  # NOTE: Excluded deleted generations only if they are not passed in ``paths``
            # NOTE: Add the activity to check if it also matches the condition
            downstream_chains.append((starting_activity,))
            downstream_chains = [c for c in downstream_chains if has_an_existing_generation(c[-1])]
            # NOTE: Include activity only if any of its downstream matched the condition
            include_starting_activity = len(downstream_chains) > 0
        else:
            include_starting_activity = True

        if include_starting_activity:
            include_newest_activity(starting_activity)

        for chain in downstream_chains:
            for activity in chain:
                if not is_activity_valid(activity):
                    # don't process further downstream activities as the plan in question was deleted
                    break
                include_newest_activity(activity)

    return list({a for activities in all_activities.values() for a in activities})


def get_modified_activities(
    activities: FrozenSet[Activity], repository
) -> Tuple[Set[Tuple[Activity, Entity]], Set[Tuple[Activity, Entity]]]:
    """Get lists of activities that have modified/deleted usage entities."""
    modified = set()
    deleted = set()

    paths = []

    for activity in activities:
        for usage in activity.usages:
            paths.append(usage.entity.path)

    hashes = repository.get_object_hashes(paths=paths)

    for activity in activities:
        for usage in activity.usages:
            entity = usage.entity
            current_checksum = hashes.get(entity.path, None)
            usage_path = repository.path / usage.entity.path
            if current_checksum is None or not usage_path.exists():
                deleted.add((activity, entity))
            elif current_checksum != entity.checksum:
                modified.add((activity, entity))

    return modified, deleted


def filter_overridden_activities(activities: List[Activity]) -> FrozenSet[Activity]:
    """Filter out overridden activities from a list of activities."""
    relevant_activities: Dict[FrozenSet[str], Activity] = {}

    for activity in activities[::-1]:
        outputs = frozenset(g.entity.path for g in activity.generations)

        subset_of = set()
        superset_of = set()

        for o, a in relevant_activities.items():
            if outputs.issubset(o):
                subset_of.add((o, a))
            elif outputs.issuperset(o):
                superset_of.add((o, a))

        if not subset_of and not superset_of:
            relevant_activities[outputs] = activity
            continue

        if subset_of and any(activity.ended_at_time < a.ended_at_time for _, a in subset_of):
            # activity is a subset of another, newer activity, ignore it
            continue

        older_subsets = [o for o, a in superset_of if activity.ended_at_time > a.ended_at_time]

        for older_subset in older_subsets:
            # remove other activities that this activity is a superset of
            del relevant_activities[older_subset]

        relevant_activities[outputs] = activity

    return frozenset(relevant_activities.values())


def add_activity_if_recent(activity: Activity, activities: Set[Activity]):
    """Add ``activity`` to ``activities`` if it's not in the set or is the latest executed instance.

    Remove existing activities that were executed earlier.
    """
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


def get_latest_activity(activities: Iterable[Activity]) -> Optional[Activity]:
    """Return the activity that was executed after all other activities."""
    return max(activities, key=lambda a: a.ended_at_time) if activities else None


def get_latest_activity_before(activities: Iterable[Activity], activity: Activity) -> Optional[Activity]:
    """Return the latest activity that was executed before the passed activity."""
    activities_before = [a for a in activities if a.ended_at_time <= activity.ended_at_time and a.id != activity.id]
    return get_latest_activity(activities_before)


@inject.autoparams("activity_gateway", "client_dispatcher")
def revert_activity(
    *,
    activity_gateway: IActivityGateway,
    activity_id: str,
    client_dispatcher: IClientDispatcher,
    delete_plan: bool,
    force: bool,
    metadata_only: bool,
) -> Activity:
    """Revert an activity.

    Args:
        activity_gateway(IActivityGateway): The injected activity gateway.
        activity_id(str): ID of the activity to be reverted.
        client_dispatcher(IClientDispatcher): The injected client dispatcher.
        delete_plan(bool): Delete the plan if it's not used by any other activity.
        force(bool): Revert the activity even if it has some downstream activities.
        metadata_only(bool): Only revert the metadata and don't touch generated files.

    Returns:
        The deleted activity.
    """
    client = client_dispatcher.current_client

    delete_time = local_now()

    def delete_associated_plan(activity):
        if not delete_plan:
            return

        plan = activity.association.plan

        used_by_other_activities = any(a for a in get_activities(plan) if a.id != activity.id)
        if used_by_other_activities:
            return

        remove_plan(name_or_id=plan.id, force=True, when=delete_time)

    def revert_generations(activity) -> Tuple[Set[str], Set[str]]:
        """Either revert each generation to an older version (created by an earlier activity) or delete it."""
        deleted_paths = set()
        updated_paths: Dict[str, str] = {}

        if metadata_only:
            return set(), set()

        for generation in activity.generations:
            path = generation.entity.path

            generator_activities = activity_gateway.get_activities_by_generation(path=path)
            generator_activities = [a for a in generator_activities if is_activity_valid(a) and not a.deleted]
            latest_generator = get_latest_activity(generator_activities)
            if latest_generator != activity:  # NOTE: A newer activity already generated the same path
                continue

            previous_generator = get_latest_activity_before(generator_activities, activity)

            if previous_generator is None:  # NOTE: The activity is the only generator
                # NOTE: Delete the path if there are no downstreams otherwise keep it
                downstream_activities = activity_gateway.get_activities_by_usage(path)
                if not downstream_activities:
                    deleted_paths.add(path)
                elif not force:
                    raise errors.ActivityDownstreamNotEmptyError(activity)
            else:  # NOTE: There is a previous generation of that path, so, revert to it
                previous_generation = next(g for g in previous_generator.generations if g.entity.path == path)
                updated_paths[path] = previous_generation.entity.checksum

        for path, previous_checksum in updated_paths.items():
            try:
                client.repository.copy_content_to_file(path, checksum=previous_checksum, output_path=path)
            except errors.FileNotFound:
                communication.warn(f"Cannot revert '{path}' to a previous version, will keep the current version")

        for path in deleted_paths:
            try:
                os.unlink(client.path / path)
            except OSError:
                communication.warn(f"Cannot delete '{path}'")

        return deleted_paths, set(updated_paths.keys())

    activity = activity_gateway.get_by_id(activity_id)

    if activity is None:
        raise errors.ParameterError(f"Cannot find activity with ID '{activity}'")
    if activity.deleted:
        raise errors.ParameterError(f"Activity with ID '{activity}' is already deleted")

    # NOTE: The order of removal is important here so don't change it
    delete_associated_plan(activity)
    revert_generations(activity)
    activity_gateway.remove(activity, force=force)
    # NOTE: Delete the activity after processing metadata or otherwise we won't see the activity as the latest generator
    activity.delete(when=delete_time)

    return activity


@inject.autoparams()
def is_activity_valid(activity: Activity) -> bool:
    """Return whether this plan has not been deleted.

    Args:
        activity(Activity): The Activity whose Plan should be checked.

    Returns:
        bool: True if the activities' Plan is still valid, False otherwise.

    """
    return not is_plan_removed(plan=activity.association.plan)
