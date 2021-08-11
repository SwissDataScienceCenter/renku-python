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
"""Renku generic database gateway implementation."""

from pathlib import Path
from typing import Iterator

import BTrees
from persistent.list import PersistentList
from zc.relation import RELATION
from zc.relation.catalog import Catalog
from zc.relation.queryfactory import TransposingTransitive

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.metadata.database import Database
from renku.core.models.dataset import Dataset
from renku.core.models.entity import Collection
from renku.core.models.provenance.activity import Activity
from renku.core.models.workflow.plan import AbstractPlan


def dump_activity(activity: Activity, catalog, cache) -> str:
    """Get storage token for an activity."""
    return activity.id


@inject.autoparams()
def load_activity(token: str, catalog, cache, database: Database) -> Activity:
    """Load activity from storage token."""
    return database["activities"].get(token)


@inject.autoparams()
def downstream_activity(activity: Activity, catalog, database: Database) -> Iterator[Activity]:
    """Map an activity to its downstream dependants."""
    result = []
    for generation in activity.generations:
        if not isinstance(generation.entity, Collection):
            # NOTE: Get direct dependants
            result.extend(database["activities-by-usage"].get(generation.entity.path, []))
        else:
            # NOTE: Get dependants that are in a generated directory
            for path, activities in database["activities-by-usage"].items():
                parent = Path(generation.entity.path).resolve()
                child = Path(path).resolve()
                if parent == child or parent in child.parents:
                    result.extend(activities)

    return result


@inject.autoparams()
def upstream_activity(activity: Activity, catalog, database: Database) -> Iterator[Activity]:
    """Map an activity to its upstream predecessors."""
    result = []
    for usage in activity.usages:
        if not isinstance(usage.entity, Collection):
            # NOTE: Get direct dependants
            result.extend(database["activities-by-generation"].get(usage.entity.path, []))
        else:
            # NOTE: Get dependants that are in a generated directory
            for path, activities in database["activities-by-generation"].items():
                parent = Path(usage.entity.path).resolve()
                child = Path(path).resolve()
                if parent == child or parent in child.parents:
                    result.extend(activities)

    return result


# NOTE: Transitive query factory is needed for transitive (follow more than 1 edge) queries
downstream_transitive_factory = TransposingTransitive(RELATION, "downstream_activity")
upstream_transitive_factory = TransposingTransitive(RELATION, "upstream_activity")


class DatabaseGateway(IDatabaseGateway):
    """Gateway for base database operations."""

    database = inject.attr(Database)

    def initialize(self) -> None:
        """Initialize the database."""
        self.database.clear()

        self.database.add_index(name="activities", object_type=Activity, attribute="id")
        self.database.add_index(name="latest-activity-by-plan", object_type=Activity, attribute="association.plan.id")
        self.database.add_root_object(name="activities-by-usage", obj=BTrees.OOBTree.OOBTree())
        self.database.add_root_object(name="activities-by-generation", obj=BTrees.OOBTree.OOBTree())

        activity_catalog = Catalog(dump_activity, load_activity, btree=BTrees.family32.OO)
        activity_catalog.addValueIndex(
            downstream_activity, dump_activity, load_activity, btree=BTrees.family32.OO, multiple=True
        )
        activity_catalog.addValueIndex(
            upstream_activity, dump_activity, load_activity, btree=BTrees.family32.OO, multiple=True
        )
        self.database.add_root_object(name="activity-catalog", obj=activity_catalog)

        self.database.add_index(name="plans", object_type=AbstractPlan, attribute="id")
        self.database.add_index(name="plans-by-name", object_type=AbstractPlan, attribute="name")

        self.database.add_index(name="datasets", object_type=Dataset, attribute="name")
        self.database.add_index(name="datasets-provenance-tails", object_type=Dataset, attribute="id")
        self.database.add_index(name="datasets-tags", object_type=PersistentList)

        self.database.commit()

    def commit(self) -> None:
        """Commit changes to database."""
        self.database.commit()
