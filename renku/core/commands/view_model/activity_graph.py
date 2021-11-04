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
"""Activity graph view model."""

from collections import namedtuple
from textwrap import shorten

Point = namedtuple("Point", ["x", "y"])

MAX_NODE_LENGTH = 40


class NodeViewer:
    """Gets width/height for a display node."""

    def __init__(self, text):
        lines = text.splitlines()
        self.w = max([len(line) for line in lines]) + 2
        self.h = len(lines) + 2


class ActivityGraphViewModel:
    """A view model for outputting a directed activity graph."""

    def __init__(self, graph):
        self.graph = graph

    def _format_vertex(self, node, columns) -> str:
        """Return vertex text for a node."""
        from renku.core.models.provenance.activity import Activity

        if isinstance(node, Activity):
            return "\n".join(shorten(c(node), MAX_NODE_LENGTH, placeholder="...") for c in columns), True, node

        return node, False, None

    def _subgraph_order_key(self, subgraph):
        """Return a sorting key for ordering subgraphs.

        Subgraphs are ordered by the ended_at_time of the lastest activits in them.
        """
        from renku.core.models.provenance.activity import Activity

        activity_times = [n.ended_at_time for n in subgraph.nodes() if isinstance(n, Activity)]

        return max(activity_times)

    def layout_graph(self, columns):
        """Create a Sugiyama layout of the graph."""
        import networkx
        from grandalf.graphs import Edge, Graph, Vertex
        from grandalf.layouts import SugiyamaLayout
        from grandalf.routing import EdgeViewer, route_with_lines

        from renku.core.models.provenance.activity import Activity

        columns = [ACTIVITY_GRAPH_COLUMNS[c] for c in columns.split(",")]

        self.layouts = []

        components = networkx.weakly_connected_components(self.graph)
        subgraphs = [self.graph.subgraph(component).copy() for component in components]
        subgraphs = filter(lambda s: any(isinstance(n, Activity) for n in s), subgraphs)
        subgraphs = sorted(subgraphs, key=self._subgraph_order_key)

        for subgraph in subgraphs:
            nodes = {n: Vertex(self._format_vertex(n, columns)) for n in subgraph.nodes}
            edges = [Edge(nodes[s], nodes[t]) for s, t in subgraph.edges]
            roots = [nodes[n] for n, d in subgraph.in_degree() if d == 0]

            graph = Graph(nodes.values(), edges)

            shortest_node_width = 2 ** 64
            shortest_node_height = 2 ** 64

            for node in nodes.values():
                node.view = NodeViewer(node.data[0])
                shortest_node_width = min(shortest_node_width, node.view.w)
                shortest_node_height = min(shortest_node_height, node.view.h)

            for edge in edges:
                edge.view = EdgeViewer()

            layout = SugiyamaLayout(graph.C[0])
            layout.init_all(roots=roots, optimize=True)
            layout.xspace = shortest_node_width / 2
            layout.yspace = shortest_node_height

            # space between beginning of one node and beginning of next
            self.vertical_space = shortest_node_height * 2

            layout.route_edge = route_with_lines
            layout.draw(5)
            self.layouts.append(layout)

    def _add_edge_to_canvas(self, edge, canvas, edges, min_y):
        """Add an edge to a canvas object.

        Makes sure overlapping edges don't have the same color.
        """
        from renku.core.commands.view_model.text_canvas import EdgeShape

        points = edge.view._pts
        edge_color = EdgeShape.next_color()
        new_edges = []
        max_y = 0
        for index in range(len(points) - 1):
            start = points[index]
            end = points[index + 1]
            max_y = max(max_y, end[1])
            new_edges.append(
                EdgeShape(Point(start[0], start[1] + min_y), Point(end[0], end[1] + min_y), color=edge_color)
            )

        # figure out if this edge crosses any existing edge with the same color, if so, change the color
        same_color_edges = [e for e in edges if e.color == edge_color]

        if same_color_edges:
            # check for intersections
            intersecting_edges = [e for e in same_color_edges if any(e.intersects_with(o) for o in new_edges)]

            colors = {e.color for e in intersecting_edges}

            if len(colors) < len(EdgeShape.EDGE_COLORS):
                while edge_color in colors:
                    edge_color = EdgeShape.next_color()
                for e in new_edges:
                    e.color = edge_color

        [canvas.add_shape(e, layer=0) for e in new_edges]
        edges.extend(new_edges)
        return max_y

    def text_representation(self, columns: str, color: bool = True, ascii=False):
        """Return an ascii representation of the graph."""
        from grandalf.layouts import DummyVertex

        from renku.core.commands.view_model.text_canvas import NodeShape, TextCanvas

        self.layout_graph(columns=columns)

        canvas = TextCanvas()
        navigation_data = []

        # min_y sets the minimum y coordinate for each layout(distinct subgraph) in the graph
        min_y = 0

        if not self.layouts:
            # nothing to show
            return None, None

        for layout in self.layouts:
            max_y = 0
            existing_edges = []

            # sort edges to have consistent coloring
            edges = sorted(layout.g.sE, key=lambda e: e.view._pts[0])

            for edge in edges:
                max_y = max(max_y, self._add_edge_to_canvas(edge, canvas, existing_edges, min_y))

            for layer in layout.layers:
                layer_nodes = []
                for node in layer:
                    if isinstance(node, DummyVertex):
                        continue
                    xy = node.view.xy
                    node_shape = NodeShape(node.data[0], Point(xy[0], xy[1] + min_y), double_border=node.data[1])
                    canvas.add_shape(node_shape, layer=1)
                    max_y = max(max_y, node_shape.extent[0][1])

                    if node.data[2]:
                        layer_nodes.append((node_shape, node.data[2]))

                if layer_nodes:
                    # add layer for navigation
                    navigation_data.append(layer_nodes)

            # move subsequent layouts down below current layout
            min_y += max_y + 5

        canvas.render(color=color, ascii=ascii)

        # adjust coordinates to actual coordinates in layers
        transformed_navigation_data = []
        for layer in navigation_data:
            node_list = []
            for node in layer:
                top_left, bottom_right = node[0].actual_extent
                start = canvas.get_coordinates(top_left)
                end = canvas.get_coordinates(bottom_right)
                node_list.append((start, end, node[1]))
            transformed_navigation_data.append(node_list)

        return canvas.text, transformed_navigation_data


ACTIVITY_GRAPH_COLUMNS = {
    "command": lambda a: " ".join(a.plan_with_values.to_argv(with_streams=True)),
    "id": lambda a: a.id,
    "date": lambda a: a.ended_at_time.isoformat(),
}
