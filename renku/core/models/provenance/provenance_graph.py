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
"""Represent dependency graph."""

import json
import os
from collections import deque
from pathlib import Path
from typing import Union

from marshmallow import EXCLUDE
from rdflib import ConjunctiveGraph

from renku.core.models.calamus import JsonLDSchema, Nested, schema
from renku.core.models.provenance.activity import Activity, ActivityCollection, ActivitySchema


class ProvenanceGraph:
    """A graph of all executions (Activities)."""

    def __init__(self, activities=None):
        """Set uninitialized properties."""
        self._activities = activities or []
        self._path = None
        self._order = 0 if len(self._activities) == 0 else max([a.order for a in self._activities])
        self._graph = None
        self._loaded = False
        self._custom_bindings = {}
        self._provenance_paths = None
        self._split_load = False

    @property
    def activities(self):
        """Return list of activities."""
        assert self._loaded

        return self._activities

    @property
    def order(self):
        """Return current order value."""
        return self._order

    @property
    def read_only(self):
        """Return true if graph is writable."""
        return self._split_load

    @property
    def custom_bindings(self):
        """Return custom bindings."""
        return self._custom_bindings

    @custom_bindings.setter
    def custom_bindings(self, custom_bindings):
        """Set custom prefix to namespace bindings."""
        self._custom_bindings = custom_bindings

    def add(self, node: Union[Activity, ActivityCollection]):
        """Add an Activity/ActivityCollection to the graph."""
        assert self._loaded

        activity_collection = node if isinstance(node, ActivityCollection) else ActivityCollection(activities=[node])

        for activity in activity_collection.activities:
            assert not any([a for a in self._activities if a.id_ == activity.id_]), f"Identifier exists {activity.id_}"
            self._order += 1
            activity.order = self._order
            self._activities.append(activity)

    @classmethod
    def from_json(cls, path, lazy=False):
        """Return an instance from a JSON file."""
        if Path(path).exists():
            if not lazy:
                with open(path) as file_:
                    data = json.load(file_)
                    self = cls.from_jsonld(data=data) if data else ProvenanceGraph(activities=[])
                    self._activities.sort(key=lambda e: e.order)
                    self._loaded = True
            else:
                self = ProvenanceGraph(activities=[])
                self._loaded = False
        else:
            self = ProvenanceGraph(activities=[])
            self._loaded = True

        self._path = path

        return self

    @classmethod
    def from_provenance_paths(cls, paths, lazy=False):
        """Return an instance from a set of ActivityCollection JSON file."""
        if paths:
            if not lazy:
                activities = []
                for path in paths:
                    activity_collection = ActivityCollection.from_json(path)
                    activities.extend(activity_collection.activities)

                self = ProvenanceGraph(activities=activities)
                self._activities.sort(key=lambda e: e.order)
                self._loaded = True
            else:
                self = ProvenanceGraph(activities=[])
                self._loaded = False
        else:
            self = ProvenanceGraph(activities=[])
            self._loaded = True

        self._provenance_paths = paths
        self._split_load = True

        return self

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        elif not isinstance(data, list):
            raise ValueError(data)

        self = ProvenanceGraphSchema(flattened=True).load(data)
        self._loaded = True

        return self

    def to_jsonld(self):
        """Create JSON-LD."""
        return ProvenanceGraphSchema(flattened=True).dump(self)

    def to_json(self, path=None):
        """Write an instance to file."""
        if self._split_load and not path:
            raise RuntimeError("No path provided to write a split provenance graph.")

        path = path or self._path
        data = self.to_jsonld()
        with open(path, "w", encoding="utf-8") as file_:
            json.dump(data, file_, ensure_ascii=False, sort_keys=True, indent=2)

    @property
    def rdf_graph(self):
        """Create an RDFLib ConjunctiveGraph."""
        self._create_rdf_graph()
        return self._graph

    def _create_rdf_graph(self):
        if self._graph:
            return

        paths = self._provenance_paths if self._split_load else [self._path]
        self._graph = self._create_graph(paths=paths)

    def _create_graph(self, paths=None, activities=None):
        graph = ConjunctiveGraph()

        if paths:
            if isinstance(paths, str):
                paths = [paths]
            for path in paths:
                graph.parse(location=str(path), format="json-ld")

        activities = activities or []
        for activity in activities:
            data = activity.to_jsonld_str()
            graph.parse(data=data, format="json-ld")

        bindings = {
            "foaf": "http://xmlns.com/foaf/0.1/",
            "oa": "http://www.w3.org/ns/oa#",
            "prov": "http://www.w3.org/ns/prov#",
            "renku": "https://swissdatasciencecenter.github.io/renku-ontology#",
            "schema": "http://schema.org/",
            "wf": "http://www.w3.org/2005/01/wf/flow#",
            "wfprov": "http://purl.org/wf4ever/wfprov#",
        }

        bindings.update(self._custom_bindings)

        for prefix, namespace in bindings.items():
            graph.bind(prefix, namespace)

        return graph

    def get_latest_plans_usages(self):
        """Return a list of tuples with path and check of all Usage paths."""
        plan_orders = self.query(LATEST_PLAN_EXECUTION_ORDER)
        usages = self.query(ALL_USAGES)

        latest_usages = (u for u in usages for o in plan_orders if u[1] == o[1])

        return [(str(u[0]), str(u[-2]), str(u[-1])) for u in latest_usages]

    def query(self, query):
        """Run a SPARQL query and return the result."""
        self._create_rdf_graph()
        return self._graph.query(query)

    def get_subgraph(self, paths):
        """Return a subgraph that generates specific paths."""
        starting_activities = []
        for path in paths:
            activity = self._get_latest_activity(path)
            if activity:
                starting_activities.append(activity)

        if not starting_activities:
            return self._create_graph()

        todo = deque(starting_activities)
        activities = []

        while todo:
            activity = todo.popleft()
            activities.append(activity)
            for usage in activity.qualified_usage:
                parent_activity = self._get_latest_activity(entity_id=usage.entity._id)
                if not parent_activity:
                    continue
                activities.append(parent_activity)
                todo.append(parent_activity)

        return self._create_graph(activities=activities)

    def _get_latest_activity(self, path=None, entity_id=None):
        """Return the latest activity that generated a path or entity."""

        def is_parent_path(parent, child):
            if not parent or not child:
                return False
            parent = Path(os.path.realpath(parent))
            child = Path(os.path.realpath(child))
            return parent == child or parent in child.parents

        for activity in reversed(self.activities):
            for generation in activity.generated:
                if (
                    is_parent_path(generation.entity.path, path)
                    or is_parent_path(path, generation.entity.path)
                    or generation.entity._id == entity_id
                ):
                    return activity


class ProvenanceGraphSchema(JsonLDSchema):
    """ProvenanceGraph schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Collection]
        model = ProvenanceGraph
        unknown = EXCLUDE

    _activities = Nested(schema.hasPart, ActivitySchema, init_name="activities", many=True, missing=None)


LATEST_PLAN_EXECUTION_ORDER = """
    SELECT ?plan (MAX(?order) AS ?maxOrder)
    WHERE
    {
        ?activity a prov:Activity .
        ?activity prov:qualifiedAssociation/prov:hadPlan ?plan .
        ?activity renku:order ?order
    }
    GROUP BY ?plan
    """


ALL_USAGES = """
    SELECT ?plan ?order ?usage ?path ?checksum
    WHERE
    {
        ?activity a prov:Activity .
        ?activity prov:qualifiedAssociation/prov:hadPlan ?plan .
        ?activity renku:order ?order .
        ?activity prov:qualifiedUsage ?usage .
        ?usage prov:entity ?entity .
        ?entity prov:atLocation ?path .
        ?entity renku:checksum ?checksum .
    }
    """
