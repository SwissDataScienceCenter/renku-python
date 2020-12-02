# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
from collections import deque
from pathlib import Path
from typing import List, Tuple, Union

import networkx
from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, schema
from renku.core.models.workflow.plan import Plan, PlanSchema


class DependencyGraph:
    """A graph of all execution templates (Plans)."""

    # TODO: dependency graph can have cycles in it because up until now there was no check to prevent this

    def __init__(self, plans=None):
        """Initialized."""
        self._plans = plans or []
        self._path = None

        self._graph = networkx.DiGraph()
        self._graph.add_nodes_from(self._plans)
        self._connect_all_nodes()

    def add(self, plan: Plan) -> Plan:
        """Add a plan to the graph if a similar plan does not exists."""
        existing_plan = self._find_similar_plan(plan)
        if existing_plan:
            return existing_plan

        assert not any([p for p in self._plans if p.name == plan.name]), f"Duplicate name {plan.id_}, {plan.name}"
        # FIXME it's possible to have the same identifier but different list of arguments (e.g.
        # test_rerun_with_edited_inputs)
        same_id_found = [p for p in self._plans if p.id_ == plan.id_]
        if same_id_found:
            plan.assign_new_id()
        assert not any([p for p in self._plans if p.id_ == plan.id_]), f"Identifier exists {plan.id_}"
        self._add_helper(plan)

        assert networkx.algorithms.dag.is_directed_acyclic_graph(self._graph)

        return plan

    def _find_similar_plan(self, plan: Plan) -> Union[Plan, None]:
        """Search for a similar plan and return it."""
        for p in self._plans:
            if p.is_similar_to(plan):
                return p

    def _add_helper(self, plan: Plan):
        self._plans.append(plan)

        self._graph.add_node(plan)
        self._connect_node_to_others(node=plan)

    def _connect_all_nodes(self):
        for node in self._graph:
            self._connect_node_to_others(node)

    def _connect_node_to_others(self, node: Plan):
        for other_node in self._graph:
            self._connect_two_nodes(from_=node, to_=other_node)
            self._connect_two_nodes(from_=other_node, to_=node)

    def _connect_two_nodes(self, from_: Plan, to_: Plan):
        for o in from_.outputs:
            for i in to_.inputs:
                if DependencyGraph._is_super_path(o.produces, i.consumes):
                    self._graph.add_edge(from_, to_, name=o.produces)

    def visualize_graph(self):
        """Visualize graph using matplotlib."""
        networkx.draw(self._graph, with_labels=True, labels={n: n.name for n in self._graph.nodes})

        pos = networkx.spring_layout(self._graph)
        edge_labels = networkx.get_edge_attributes(self._graph, "name")
        networkx.draw_networkx_edge_labels(self._graph, pos=pos, edge_labels=edge_labels)

    def to_png(self, path):
        """Create a PNG image from graph."""
        networkx.drawing.nx_pydot.to_pydot(self._graph).write_png(path)

    @staticmethod
    def _is_super_path(parent, child):
        parent = Path(parent).resolve()
        child = Path(child).resolve()
        return parent == child or parent in child.parents

    def get_dependent_paths(self, plan_id, path):
        """Get a list of downstream paths."""
        nodes = deque()
        node: Plan
        for node in self._graph:
            if plan_id == node.id_ and any(self._is_super_path(path, p.consumes) for p in node.inputs):
                nodes.append(node)

        paths = set()

        # TODO: This loops infinitely if there is a cycle in the graph
        while nodes:
            node = nodes.popleft()
            outputs_paths = [o.produces for o in node.outputs]
            paths.update(outputs_paths)

            nodes.extend(self._graph.successors(node))

        return paths

    def get_downstream(self, modified_usages, deleted_usages) -> Tuple[List[Plan], List[Plan]]:
        """Return a list of Plans in topological order that should be updated."""

        def node_has_deleted_inputs(node_):
            for _, path_, _ in deleted_usages:
                if any(self._is_super_path(path_, p.consumes) for p in node_.inputs):
                    return True
            return False

        nodes = set()
        nodes_with_deleted_inputs = set()
        node: Plan
        for plan_id, path, _ in modified_usages:
            for node in self._graph:
                if plan_id == node.id_ and any(self._is_super_path(path, p.consumes) for p in node.inputs):
                    nodes.add(node)
                    nodes.update(networkx.algorithms.dag.descendants(self._graph, node))

        sorted_nodes = []
        for node in networkx.algorithms.dag.topological_sort(self._graph):
            if node in nodes:
                if node_has_deleted_inputs(node):
                    nodes_with_deleted_inputs.add(node)
                else:
                    sorted_nodes.append(node)

        return sorted_nodes, list(nodes_with_deleted_inputs)

    @classmethod
    def from_json(cls, path):
        """Create an instance from a file."""
        if Path(path).exists():
            with open(path) as file_:
                data = json.load(file_)
                self = cls.from_jsonld(data=data) if data else DependencyGraph(plans=[])
        else:
            self = DependencyGraph(plans=[])

        self._path = path

        return self

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        elif not isinstance(data, list):
            raise ValueError(data)

        return DependencyGraphSchema(flattened=True).load(data)

    def to_jsonld(self):
        """Create JSON-LD."""
        return DependencyGraphSchema(flattened=True).dump(self)

    def to_json(self, path=None):
        """Write to file."""
        path = path or self._path
        data = self.to_jsonld()
        with open(path, "w", encoding="utf-8") as file_:
            json.dump(data, file_, ensure_ascii=False, sort_keys=True, indent=2)


class DependencyGraphSchema(JsonLDSchema):
    """DependencyGraph schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Collection]
        model = DependencyGraph
        unknown = EXCLUDE

    _plans = Nested(schema.hasPart, PlanSchema, init_name="plans", many=True, missing=None)
