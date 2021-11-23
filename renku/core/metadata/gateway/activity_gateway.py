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
"""Renku activity database gateway implementation."""

from itertools import chain
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

from persistent.list import PersistentList

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.metadata.gateway.database_gateway import ActivityDownstreamRelation
from renku.core.models.provenance.activity import Activity, ActivityCollection, Usage
from renku.core.models.workflow.plan import AbstractPlan, Plan
from renku.core.utils.os import are_paths_related


class ActivityGateway(IActivityGateway):
    """Gateway for activity database operations."""

    database_dispatcher = inject.attr(IDatabaseDispatcher)

    def get_latest_activities(self) -> List[Activity]:
        """Get the latest activites by their usages and generations."""
        database = self.database_dispatcher.current_database
        activities_with_generations = database["latest-activity-by-generations"].values()
        activities_without_generations = database["activities-without-generation"].values()
        return chain(activities_with_generations, activities_without_generations)

    def get_latest_activity_per_plan(self) -> Dict[AbstractPlan, Activity]:
        """Get latest activity for each plan."""
        plan_activities = self.database_dispatcher.current_database["latest-activity-by-plan"].values()

        return {a.association.plan: a for a in plan_activities}

    def get_plans_and_usages_for_latest_activities(self) -> Dict[AbstractPlan, List[Usage]]:
        """Get all usages associated with a plan by its latest activity."""
        plan_activities = self.database_dispatcher.current_database["latest-activity-by-plan"].values()

        return {a.association.plan: a.usages for a in plan_activities}

    def get_all_usage_paths(self) -> List[str]:
        """Return all usage paths."""
        database = self.database_dispatcher.current_database

        return list(database["activities-by-usage"].keys())

    def get_all_generation_paths(self) -> List[str]:
        """Return all generation paths."""
        database = self.database_dispatcher.current_database

        return list(database["activities-by-generation"].keys())

    def get_activities_by_generation(self, path: Union[Path, str], checksum: Optional[str] = None) -> List[Activity]:
        """Return the list of all activities that generate a path."""
        by_generation = self.database_dispatcher.current_database["activities-by-generation"]
        activities = by_generation.get(str(path), [])

        if not checksum:
            return activities

        result = []

        for activity in activities:
            generation = next((g for g in activity.generations if g.entity.checksum == checksum), None)

            if generation:
                result.append(activity)

        return result

    def get_downstream_activities(self, activity: Activity, max_depth=None) -> Set[Activity]:
        """Get downstream activities that depend on this activity."""
        # NOTE: since indices are populated one way when adding an activity, we need to query two indices
        database = self.database_dispatcher.current_database

        activity_catalog = database["activity-catalog"]
        tok = activity_catalog.tokenizeQuery
        downstream = set(activity_catalog.findValues("downstream", tok(upstream=activity), maxDepth=max_depth))

        return downstream

    def get_downstream_activity_chains(self, activity: Activity) -> List[Tuple[Activity, ...]]:
        """Get a list of tuples of all downstream paths of this activity."""
        database = self.database_dispatcher.current_database

        activity_catalog = database["activity-catalog"]
        tok = activity_catalog.tokenizeQuery
        downstream_chains = activity_catalog.findRelationChains(tok(upstream=activity))
        downstream_chains = [tuple(r.downstream for r in c) for c in downstream_chains]

        return downstream_chains

    def get_upstream_activity_chains(self, activity: Activity) -> List[Tuple[Activity, ...]]:
        """Get a list of tuples of all upstream paths of this activity."""
        database = self.database_dispatcher.current_database

        activity_catalog = database["activity-catalog"]
        tok = activity_catalog.tokenizeQuery
        upstream_chains = activity_catalog.findRelationChains(tok(downstream=activity))
        upstream_chains = [tuple(r.upstream for r in c) for c in upstream_chains]

        return upstream_chains

    def get_all_activities(self) -> List[Activity]:
        """Get all activities in the project."""
        return list(self.database_dispatcher.current_database["activities"].values())

    def add(self, activity: Activity):
        """Add an ``Activity`` to storage."""

        def update_latest_activity_by_plan(plan):
            existing_activity = database["latest-activity-by-plan"].get(plan.id)

            if not existing_activity or existing_activity.ended_at_time < activity.ended_at_time:
                database["latest-activity-by-plan"].add(activity, key=plan.id, verify=False)

        database = self.database_dispatcher.current_database

        database["activities"].add(activity)

        upstreams = set()
        downstreams = set()

        by_usage = database["activities-by-usage"]
        latest_by_generations = database["latest-activity-by-generations"]
        activities_without_generations = database["activities-without-generation"]
        by_generation = database["activities-by-generation"]

        generations = []

        for usage in activity.usages:
            if usage.entity.path not in by_usage:
                by_usage[usage.entity.path] = PersistentList()
            by_usage[usage.entity.path].append(activity)

            for path, activities in by_generation.items():
                if are_paths_related(path, usage.entity.path):
                    upstreams.update(activities)

        for generation in activity.generations:
            generations.append(g.entity.path)

            if generation.entity.path not in by_generation:
                by_generation[generation.entity.path] = PersistentList()
            by_generation[generation.entity.path].append(activity)

            for path, activities in by_usage.items():
                if are_paths_related(path, generation.entity.path):
                    downstreams.update(activities)

        if generations:
            latest_by_generations[frozenset(generations)] = activity
        else:
            # activity has no outputs
            usages = set(u.entity.path for u in activity.usages)
            existing_activities = [
                a for a in activities_without_generations.values() if a.association.plan == activity.association.plan
            ]
            existing_activities = [a for a in existing_activities if set(u.entity.path for u in a.usages) == usages]

            if existing_activities:
                for a in existing_activities:
                    activities_without_generations.pop(a.id)

            activities_without_generations.add(activity)

        if upstreams:
            for s in upstreams:
                database["activity-catalog"].index(ActivityDownstreamRelation(downstream=activity, upstream=s))

        if downstreams:
            for s in downstreams:
                database["activity-catalog"].index(ActivityDownstreamRelation(downstream=s, upstream=activity))

        assert isinstance(activity.association.plan, Plan)

        plan_gateway = inject.instance(IPlanGateway)
        plan_gateway.add(activity.association.plan)

        update_latest_activity_by_plan(activity.association.plan)

    def add_activity_collection(self, activity_collection: ActivityCollection):
        """Add an ``ActivityCollection`` to storage."""
        database = self.database_dispatcher.current_database

        database["activity-collections"].add(activity_collection)

    def get_all_activity_collections(self) -> List[ActivityCollection]:
        """Get all activity collections in the project."""
        return list(self.database_dispatcher.current_database["activity-collections"].values())
