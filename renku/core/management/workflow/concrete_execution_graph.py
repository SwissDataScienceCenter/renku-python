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
"""Build an execution graph for a workflow."""

from collections import defaultdict
from itertools import product
from typing import Union

import networkx as nx
from networkx.algorithms.cycles import simple_cycles

from renku.core.models.workflow import grouped_run, parameter, plan


class ExecutionGraph:
    """Represents an execution graph for one or more workflow steps."""

    def __init__(self, workflow: Union["plan.Plan", "grouped_run.GroupedRun"], virtual_links: bool = False):
        self.workflow: Union["plan.Plan", "grouped_run.GroupedRun"] = workflow
        self.virtual_links = []

        self.calculate_concrete_execution_graph(virtual_links=virtual_links)

    def calculate_concrete_execution_graph(self, virtual_links: bool = False):
        """Create an execution DAG between Plans showing dependencies between them.

        Resolve ParameterLink's involving ParameterMapping's to the underlying actual
        parameters and potentially also virtual links determined by parameter values.
        """

        self.graph = nx.DiGraph()
        self.virtual_links = []

        workflow_stack = [self.workflow]

        inputs = defaultdict(list)
        outputs = defaultdict(list)

        while workflow_stack:
            workflow = workflow_stack.pop()

            if isinstance(workflow, grouped_run.GroupedRun):
                workflow_stack.extend(workflow.plans)

                self._add_grouped_run_links_to_graph(workflow)
            else:
                if not virtual_links:
                    continue

                for input_ in workflow.inputs:
                    inputs[input_.actual_value].append(input_)
                    if not self.graph.has_edge(input_, input_):
                        self.graph.add_edge(input_, workflow)

                for output in workflow.outputs:
                    outputs[output.actual_value].append(output)
                    if not self.graph.has_edge(workflow, output):
                        self.graph.add_edge(workflow, output)

        for av, nodes in outputs.items():
            if av not in inputs.keys():
                continue

            edges = product(nodes, inputs[av])

            for edge in edges:
                if not self.graph.has_edge(*edge):
                    self.graph.add_edge(*edge)
                    self.virtual_links.append(edge)

    def _add_grouped_run_links_to_graph(self, workflow: "grouped_run.GroupedRun") -> None:
        """Adds links for a grouped run to the graph."""
        if not workflow.links:
            return

        for link in workflow.links:
            for sink in link.sinks:
                self._add_leaf_parameter_link(link.source, sink)

    def _add_leaf_parameter_link(
        self, source: "parameter.CommandParameterBase", sink: "parameter.CommandParameterBase"
    ):
        """Add links between leaf parameters (resolving Mappings)."""
        if isinstance(source, parameter.ParameterMapping):
            sources = source.leaf_parameters
        else:
            sources = [source]

        if isinstance(sink, parameter.ParameterMapping):
            sinks = sink.leaf_parameters
        else:
            sinks = [sink]

        for param in sources + sinks:
            wf = self.workflow.find_parameter_workflow(param)

            if isinstance(param, parameter.CommandOutput):
                edge = (wf, param)
            else:
                edge = (param, wf)

            self.graph.add_edge(*edge)

        edge_list = product(sources, sinks)
        self.graph.add_edges_from(edge_list)

    @property
    def cycles(self):
        """Get potential cycles in execution graph."""
        return list(simple_cycles(self.graph))
