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
"""Activity graph view model."""

from datetime import datetime
from itertools import repeat
from textwrap import shorten
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple

from renku.core import errors

if TYPE_CHECKING:
    from grandalf.graphs import Edge

    from renku.command.view_model.text_canvas import Point, TextCanvas

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

    def _format_vertex(self, node, columns: List[Callable]) -> Tuple[str, bool, Any]:
        """Return vertex text for a node.

        Args:
            node: The node to format.
            columns (List[str]): The fields to include in the node text.

        Returns:
            Tuple[str, bool, Any]: Tuple of string representation of node, whether or not
                it is an Activity and the Activity if it is.
        """
        from renku.domain_model.provenance.activity import Activity

        if isinstance(node, Activity):
            return "\n".join(shorten(c(node), MAX_NODE_LENGTH, placeholder="...") for c in columns), True, node

        return node, False, None

    def _subgraph_order_key(self, subgraph) -> datetime:
        """Return a sorting key for ordering subgraphs.

        Subgraphs are ordered by the ended_at_time of the lastest activities in them.

        Args:
            subgraph: Subgraph to get an order key for.

        Returns:
            datetime: The latest activity time in the subgraph.
        """
        from renku.domain_model.provenance.activity import Activity

        activity_times = [n.ended_at_time for n in subgraph.nodes() if isinstance(n, Activity)]

        return max(activity_times)

    def _format_vertex_raw(self, node, columns: List[Callable]) -> str:
        """Return vertex text for a node.

        Args:
            node: The node to format.
            columns (List[Callable]): The fields to include in the node text.

        Returns:
            string representation of node
        """
        import json

        from renku.domain_model.provenance.activity import Activity

        if isinstance(node, Activity):
            text = "\n".join(c(node) for c in columns)
        else:
            text = node

        # NOTE: double quotes are common in console command, repr() wouldn't escape properly
        return json.dumps(text)

    def _get_lambda_columns(self, columns):
        """Return lambda columns.

        Args:
            columns (str): comma-separated column names.

        Returns:
            List[Callable] lambda columns
        """

        try:
            return [ACTIVITY_GRAPH_COLUMNS[c] for c in columns.split(",")]
        except KeyError as e:
            wrong_values = ", ".join(e.args)
            suggestion = ",".join(ACTIVITY_GRAPH_COLUMNS.keys())
            raise errors.ParameterError(f"you can use any of '{suggestion}'", f"columns '{wrong_values}'")

    def layout_graph(self, columns):
        """Create a Sugiyama layout of the graph.

        Args:
            columns: Columns to show as node text.
        """
        import networkx
        from grandalf.graphs import Edge, Graph, Vertex
        from grandalf.layouts import SugiyamaLayout
        from grandalf.routing import EdgeViewer, route_with_lines

        from renku.domain_model.provenance.activity import Activity

        columns = self._get_lambda_columns(columns)

        self.layouts: List[SugiyamaLayout] = []

        components = networkx.weakly_connected_components(self.graph)
        subgraphs = [self.graph.subgraph(component).copy() for component in components]
        subgraphs = list(filter(lambda s: any(isinstance(n, Activity) for n in s), subgraphs))
        subgraphs = sorted(subgraphs, key=self._subgraph_order_key)

        for subgraph in subgraphs:
            nodes = {n: Vertex(self._format_vertex(n, columns)) for n in subgraph.nodes}
            edges = [Edge(nodes[s], nodes[t]) for s, t in subgraph.edges]
            roots = [nodes[n] for n, d in subgraph.in_degree() if d == 0]

            graph = Graph(nodes.values(), edges)

            shortest_node_width = 2**64
            shortest_node_height = 2**64

            for node in nodes.values():
                node.view = NodeViewer(node.data[0])
                shortest_node_width = min(shortest_node_width, node.view.w)
                shortest_node_height = min(shortest_node_height, node.view.h)

            for edge in edges:
                edge.view = EdgeViewer()

            layout = SugiyamaLayout(graph.C[0])
            layout.init_all(roots=roots, optimize=True)
            layout.xspace = max(shortest_node_width / 2, 20)
            layout.yspace = shortest_node_height

            # space between beginning of one node and beginning of next
            self.vertical_space = shortest_node_height * 2

            layout.route_edge = route_with_lines
            layout.draw(5)
            self.layouts.append(layout)

    def _add_edges_to_canvas(
        self, edges: List["Edge"], canvas: "TextCanvas", existing_edges: List["Edge"], min_y
    ) -> Tuple[int, str]:
        """Add an edge to a canvas object.

        Makes sure overlapping edges don't have the same color.

        Args:
            edges(List["Edge"]): Sub-edges of the edge to add.
            canvas("TextCanvas"): Canvas to add edge to.
            existing_edges(List["Edge"]): Already existing edges on canvas.
            min_y: Starting y coordinate inside canvas.

        Returns:
            Tuple[int, str]: Ending y coordinate after edge is added and color used for edge.
        """
        from renku.command.view_model.text_canvas import EdgeShape, Point

        edge_color = EdgeShape.next_color()
        new_edges = []
        max_y = 0

        for edge in edges:
            points = edge.view._pts
            for index in range(len(points) - 1):
                start = points[index]
                end = points[index + 1]
                max_y = max(max_y, end[1])
                new_edges.append(
                    EdgeShape(Point(start[0], start[1] + min_y), Point(end[0], end[1] + min_y), color=edge_color)
                )

        # figure out if this edge crosses any existing edge with the same color, if so, change the color
        same_color_edges = [e for e in existing_edges if e.color == edge_color]

        if same_color_edges:
            # check for intersections
            intersecting_edges = [e for e in same_color_edges if any(e.intersects_with(o) for o in new_edges)]

            colors = {e.color for e in intersecting_edges}

            if len(colors) < len(EdgeShape.COLORS):
                while edge_color in colors:
                    edge_color = EdgeShape.next_color()
                for e in new_edges:
                    e.color = edge_color

        for e in new_edges:
            canvas.add_shape(e, layer=0)

        existing_edges.extend(new_edges)
        return max_y, edge_color

    def dot_representation(self, columns: str) -> str:
        """Return the graph as a Graphviz Dot string.

        Args:
            columns(str): Columns to include in node text.

        Returns:
            string representing the Graphviz Dot graph
        """
        import io

        from renku.domain_model.provenance.activity import Activity

        # compute node text
        columns_callable = self._get_lambda_columns(columns)
        activities_text = {}
        for node in self.graph.nodes:
            if isinstance(node, Activity):
                output_text = "\n".join(c(node) for c in columns_callable)
                activities_text[str(node)] = output_text

        output = io.StringIO()
        output.write("digraph {\n")

        # add edges and track visited nodes
        visited_nodes = []
        for edge in self.graph.edges:
            vertexes = tuple(map(self._format_vertex_raw, edge, repeat(columns_callable, 2)))
            output.write(f"{vertexes[0]} -> {vertexes[1]};")
            for vertex in vertexes:
                if vertex not in visited_nodes:
                    visited_nodes.append(vertex)

        # add missing nodes
        for node in self.graph.nodes:
            lonely_node = self._format_vertex_raw(node, columns_callable)
            if lonely_node not in visited_nodes:
                output.write(f'"{lonely_node}";')

        output.write("\n}")
        return output.getvalue()

    def text_representation(
        self, columns: str, color: bool = True, ascii=False
    ) -> Tuple[Optional[str], Optional[List[List[Tuple["Point", "Point", Any]]]]]:
        """Return an ascii representation of the graph.

        Args:
            columns(str): Columns to include in node text.
            color(bool, optional): Whether to render in color or not (Default value = True).
            ascii: Whether to use only ascii characters or also UTF8  (Default value = False).

        Returns:
            Tuple[str, List[List[Tuple[Point, Point, Any]]]]: Tuple of rendered canvas text and coordinates
                of nodes within canvas.
        """
        from grandalf.layouts import DummyVertex

        from renku.command.view_model.text_canvas import NodeShape, Point, TextCanvas
        from renku.domain_model.provenance.activity import Activity

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
            existing_edges: List[Edge] = []

            for layer in layout.layers:
                layer_nodes = []

                for node in layer:
                    if isinstance(node, DummyVertex):
                        continue

                    node_color = None

                    if node.data[2] and isinstance(node.data[2], Activity):
                        # NOTE: get edges for node
                        connected_edges = list(layout.g.E(cond=lambda e: e.v[0] == node or e.v[1] == node))

                        visited_edges = set()
                        visited_nodes = {node}

                        # NOTE: Follow all edges connected to the node (might have DummyNode's in between)
                        while connected_edges:
                            current_edge = connected_edges.pop()
                            visited_edges.add(current_edge)

                            dummy_node = None

                            if isinstance(current_edge.v[0], DummyVertex) and current_edge.v[0] not in visited_nodes:
                                dummy_node = current_edge.v[0]
                            elif isinstance(current_edge.v[1], DummyVertex) and current_edge.v[0] not in visited_nodes:
                                dummy_node = current_edge.v[1]

                            if dummy_node:
                                connected_edges.extend(e for e in dummy_node.e if e not in visited_edges)
                                visited_nodes.add(dummy_node)

                        local_max_y, node_color = self._add_edges_to_canvas(
                            list(visited_edges), canvas, existing_edges, min_y
                        )
                        max_y = max(max_y, local_max_y)

                    xy = node.view.xy
                    node_shape = NodeShape(
                        node.data[0], Point(xy[0], xy[1] + min_y), double_border=node.data[1], color=node_color
                    )
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
    "plan": lambda a: a.plan_with_values.name,
}
