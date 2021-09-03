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

import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

from persistent.list import PersistentList

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.metadata.gateway.database_gateway import ActivityDownstreamRelation
from renku.core.models.entity import Collection
from renku.core.models.provenance.activity import Activity, Usage
from renku.core.models.workflow.plan import AbstractPlan, Plan


class ActivityGateway(IActivityGateway):
    """Gateway for activity database operations."""

    database_dispatcher = inject.attr(IDatabaseDispatcher)

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
        downstream_chains = [tuple(adr.downstream[0] for adr in d) for d in downstream_chains]

        return downstream_chains

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

        upstreams = []
        downstreams = []

        by_usage = database["activities-by-usage"]
        by_generation = database["activities-by-generation"]

        for usage in activity.usages:
            if usage.entity.path not in by_usage:
                by_usage[usage.entity.path] = PersistentList()
            by_usage[usage.entity.path].append(activity)

            if isinstance(usage.entity, Collection):
                # NOTE: Get dependants that are in a generated directory
                for path, activities in by_generation.items():
                    parent = Path(usage.entity.path).resolve()
                    child = Path(os.path.abspath(path))
                    if parent == child or parent in child.parents:
                        upstreams.extend(activities)
            elif usage.entity.path in by_generation:
                upstreams.extend(by_generation[usage.entity.path])

        for generation in activity.generations:
            if generation.entity.path not in by_generation:
                by_generation[generation.entity.path] = PersistentList()
            by_generation[generation.entity.path].append(activity)

            if isinstance(generation.entity, Collection):
                # NOTE: Get dependants that are in a generated directory
                for path, activities in by_usage.items():
                    parent = Path(generation.entity.path).resolve()
                    child = Path(os.path.abspath(path))
                    if parent == child or parent in child.parents:
                        downstreams.extend(activities)
            elif generation.entity.path in by_usage:
                downstreams.extend(by_usage[generation.entity.path])

        if upstreams:
            database["activity-catalog"].index(ActivityDownstreamRelation(downstream=[activity], upstream=upstreams))

        if downstreams:
            database["activity-catalog"].index(ActivityDownstreamRelation(downstream=downstreams, upstream=[activity]))

        assert isinstance(activity.association.plan, Plan)

        plan_gateway = inject.instance(IPlanGateway)
        plan_gateway.add(activity.association.plan)

        update_latest_activity_by_plan(activity.association.plan)
