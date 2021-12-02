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
from typing import Generator

import BTrees
from persistent import Persistent
from persistent.list import PersistentList
from zc.relation.catalog import Catalog
from zc.relation.queryfactory import TransposingTransitive
from zope.interface import Attribute, Interface, implementer

from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.metadata.database import RenkuOOBTree
from renku.core.models.dataset import Dataset
from renku.core.models.provenance.activity import Activity, ActivityCollection
from renku.core.models.workflow.plan import AbstractPlan


class IActivityDownstreamRelation(Interface):
    """Interface for activity downstream relation."""

    downstream = Attribute("the downstream activities")
    upstream = Attribute("the upstream activities")


@implementer(IActivityDownstreamRelation)
class ActivityDownstreamRelation:
    """Implementation of Downstream interface."""

    def __init__(self, downstream, upstream):
        self.downstream = downstream
        self.upstream = upstream

        self.id = f"{upstream.id}:{downstream.id}"


def dump_activity(activity: Activity, catalog, cache) -> str:
    """Get storage token for an activity."""
    return activity.id


@inject.autoparams()
def load_activity(token: str, catalog, cache, database_dispatcher: IDatabaseDispatcher) -> Activity:
    """Load activity from storage token."""
    database = database_dispatcher.current_database
    return database["activities"].get(token)


@inject.autoparams()
def dump_downstream_relations(
    relation: ActivityDownstreamRelation, catalog, cache, database_dispatcher: IDatabaseDispatcher
):
    """Dump relation entry to database."""
    btree = database_dispatcher.current_database["_downstream_relations"]

    btree[relation.id] = relation

    return relation.id


@inject.autoparams()
def load_downstream_relations(token, catalog, cache, database_dispatcher: IDatabaseDispatcher):
    """Load relation entry from database."""
    btree = database_dispatcher.current_database["_downstream_relations"]

    return btree[token]


def initialize_database(database):
    """Initialize an empty database with all required metadata."""
    database.add_index(name="activities", object_type=Activity, attribute="id")
    database.add_root_object(name="activities-by-usage", obj=RenkuOOBTree())
    database.add_root_object(name="activities-by-generation", obj=RenkuOOBTree())

    database.add_index(name="activity-collections", object_type=ActivityCollection, attribute="id")

    database.add_root_object(name="_downstream_relations", obj=RenkuOOBTree())

    activity_catalog = Catalog(dump_downstream_relations, load_downstream_relations, btree=BTrees.family32.OO)
    activity_catalog.addValueIndex(
        IActivityDownstreamRelation["downstream"], dump_activity, load_activity, btree=BTrees.family32.OO
    )
    activity_catalog.addValueIndex(
        IActivityDownstreamRelation["upstream"], dump_activity, load_activity, btree=BTrees.family32.OO
    )
    # NOTE: Transitive query factory is needed for transitive (follow more than 1 edge) queries
    downstream_transitive_factory = TransposingTransitive("downstream", "upstream")
    activity_catalog.addDefaultQueryFactory(downstream_transitive_factory)

    database.add_root_object(name="activity-catalog", obj=activity_catalog)

    database.add_index(name="plans", object_type=AbstractPlan, attribute="id")
    database.add_index(name="plans-by-name", object_type=AbstractPlan, attribute="name")

    database.add_index(name="datasets", object_type=Dataset, attribute="name")
    database.add_index(name="datasets-provenance-tails", object_type=Dataset, attribute="id")
    database.add_index(name="datasets-tags", object_type=PersistentList)


class DatabaseGateway(IDatabaseGateway):
    """Gateway for base database operations."""

    database_dispatcher = inject.attr(IDatabaseDispatcher)

    def initialize(self) -> None:
        """Initialize the database."""
        database = self.database_dispatcher.current_database

        database.clear()
        initialize_database(database)
        database.commit()

    def commit(self) -> None:
        """Commit changes to database."""
        database = self.database_dispatcher.current_database

        database.commit()

    def get_modified_objects_from_revision(self, revision_or_range: str) -> Generator[Persistent, None, None]:
        """Get all database objects modified in a revision."""
        # TODO: use gateway once #renku-python/issues/2253 is done

        client_dispatcher = inject.instance(IClientDispatcher)
        client = client_dispatcher.current_client

        if ".." in revision_or_range:
            commits = client.repository.iterate_commits(revision=revision_or_range)
        else:
            commits = [client.repository.get_commit(revision_or_range)]

        for commit in commits:
            for file in commit.get_changes(paths=f"{client.database_path}/**"):
                if file.deleted:
                    continue

                oid = Path(file.a_path).name

                yield self.database_dispatcher.current_database.get(oid)
