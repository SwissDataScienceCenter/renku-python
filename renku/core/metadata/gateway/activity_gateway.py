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

from operator import attrgetter
from typing import Dict, List, Set

from persistent.list import PersistentList
from zc.relation import RELATION

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.plan_gateway import IPlanGateway
from renku.core.metadata.database import Database
from renku.core.metadata.gateway.database_gateway import downstream_transitive_factory, upstream_transitive_factory
from renku.core.models.provenance.activity import Activity, Usage
from renku.core.models.workflow.plan import AbstractPlan


class ActivityGateway(IActivityGateway):
    """Gateway for activity database operations."""

    database = inject.attr(Database)

    def get_latest_activity_per_plan(self) -> Dict[AbstractPlan, Activity]:
        """Get latest activity for each plan."""
        activities = self.database["activities"].values()
        activities = sorted(activities, key=attrgetter("ended_at_time"))

        return {a.association.plan: a for a in activities}

    def get_plans_and_usages_for_latest_activities(self) -> Dict[AbstractPlan, List[Usage]]:
        """Get all usages associated with a plan by its latest activity."""
        activities = self.database["activities"].values()
        activities = sorted(activities, key=attrgetter("ended_at_time"))

        return {a.association.plan: a.usages for a in activities}

    def get_downstream_activities(self, activity: Activity) -> Set[Activity]:
        """Get downstream activities that depend on this activity."""
        # NOTE: since indices are populated one way when adding an activity, we need to query two indices
        tok = self.database["activity-catalog"].tokenizeQuery
        downstream = set(
            self.database["activity-catalog"].findValues(
                "downstream_activity", tok({RELATION: activity}), queryFactory=downstream_transitive_factory
            )
        )

        downstream |= set(
            self.database["activity-catalog"].findRelations(
                tok({"upstream_activity": activity}), queryFactory=upstream_transitive_factory
            )
        )

        return downstream

    def add(self, activity: Activity):
        """Add an ``Activity`` to storage."""
        self.database["activities"].add(activity)

        by_usage = self.database["activities-by-usage"]
        for usage in activity.usages:
            if usage.entity.path not in by_usage:
                by_usage[usage.entity.path] = PersistentList()
            by_usage[usage.entity.path].append(activity)

        by_generation = self.database["activities-by-generation"]
        for generation in activity.generations:
            if generation.entity.path not in by_generation:
                by_generation[generation.entity.path] = PersistentList()
            by_generation[generation.entity.path].append(activity)

        self.database["activity-catalog"].index(activity)

        plan_gateway = inject.instance(IPlanGateway)

        plan_gateway.add(activity.association.plan)
