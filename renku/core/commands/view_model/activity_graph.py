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

from collections import defaultdict, namedtuple
from io import StringIO
from textwrap import shorten
from typing import List, Tuple

import networkx
import numpy as np
from click import style
from grandalf.graphs import Edge, Graph, Vertex
from grandalf.layouts import DummyVertex, SugiyamaLayout
from grandalf.routing import EdgeViewer, route_with_lines

from renku.core.models.provenance.activity import Activity

Point = namedtuple("Point", ["x", "y"])

MAX_NODE_LENGTH = 40


class Shape:
    """basic shape class that all shapes inherit from."""

    pass


class RectangleShape(Shape):
    """A rectangle shape."""

    ASCII_CHARACTERS = {"edge": ["+"] * 4, "horizontal": "-", "vertical": "|"}

    UNICODE_CHARACTERS = {"edge": ["┌", "└", "┐", "┘"], "horizontal": "─", "vertical": "│"}

    UNICODE_CHARACTERS_DOUBLE = {"edge": ["╔", "╚", "╗", "╝"], "horizontal": "═", "vertical": "║"}

    def __init__(self, start: Point, end: Point, double_border=False):
        self.start = start
        self.end = end
        self.double_border = double_border

    def draw(self, color: bool = True, ascii=False) -> Tuple[List[Tuple[int]], List[str]]:
        """Return the indices and values to draw this shape onto the canvas."""
        if not ascii and self.double_border:
            characters = self.UNICODE_CHARACTERS_DOUBLE
        else:
            characters = self.ASCII_CHARACTERS if ascii else self.UNICODE_CHARACTERS

        # first set corners
        xs = np.array([self.start.x, self.start.x, self.end.x - 1, self.end.x - 1])
        ys = np.array([self.start.y, self.end.y, self.start.y, self.end.y])
        vals = np.array(characters["edge"])

        # horizontal lines
        line_xs = np.arange(self.start.x + 1, self.end.x - 1)
        xs = np.append(xs, line_xs)
        ys = np.append(ys, np.array([self.start.y] * line_xs.size))
        vals = np.append(vals, np.array([characters["horizontal"]] * line_xs.size))
        xs = np.append(xs, line_xs)
        ys = np.append(ys, np.array([self.end.y] * line_xs.size))
        vals = np.append(vals, np.array([characters["horizontal"]] * line_xs.size))

        # vertical lines
        line_ys = np.arange(self.start.y + 1, self.end.y)
        xs = np.append(xs, np.array([self.start.x] * line_ys.size))
        ys = np.append(ys, line_ys)
        vals = np.append(vals, np.array([characters["vertical"]] * line_ys.size))
        xs = np.append(xs, np.array([self.end.x - 1] * line_ys.size))
        ys = np.append(ys, line_ys)
        vals = np.append(vals, np.array([characters["vertical"]] * line_ys.size))

        # fill with whitespace to force overwriting underlying text
        fill_xs, fill_ys = np.meshgrid(
            np.arange(self.start.x + 1, self.end.x - 1), np.arange(self.start.y + 1, self.end.y - 1)
        )
        xs = np.append(xs, fill_xs)
        ys = np.append(ys, fill_ys)
        vals = np.append(vals, np.array([" "] * fill_xs.size))

        return xs.astype(int), ys.astype(int), vals

    @property
    def extent(self) -> Tuple[Tuple[int]]:
        """The extent of this shape."""
        return self.start, self.end


class TextShape(Shape):
    """A text object."""

    def __init__(self, text: str, point: Point, bold=False):
        self.point = point
        self.text = text.splitlines()
        self.bold = bold

    def draw(self, color: bool = True, ascii=False) -> Tuple[List[Tuple[int]], List[str]]:
        """Return the indices and values to draw this shape onto the canvas."""
        xs = []
        ys = []
        vals = []

        current_x = self.point.x
        current_y = self.point.y

        for line in self.text:
            for char in line:
                xs.append(current_x)
                ys.append(current_y)
                if self.bold:
                    vals.append(style(char, bold=True))
                else:
                    vals.append(char)
                current_x += 1
            current_x = self.point.x
            current_y += 1

        return np.array(xs), np.array(ys), np.array(vals)

    @property
    def extent(self) -> Tuple[Tuple[int]]:
        """The extent of this shape."""
        max_line_len = max(len(line) for line in self.text)
        num_lines = len(self.text)
        return (self.point, Point(self.point.x + max_line_len, self.point.y + num_lines - 1))


class NodeShape(Shape):
    """An activity node shape."""

    def __init__(self, text: str, point: Point, double_border=False):
        self.point = Point(round(point.x), round(point.y - len(text.splitlines())))
        self.text_shape = TextShape(text, self.point, bold=double_border)

        text_extent = self.text_shape.extent
        self.box_shape = RectangleShape(
            Point(text_extent[0].x - 1, text_extent[0].y - 1),
            Point(text_extent[1].x + 1, text_extent[1].y + 1),
            double_border=double_border,
        )

        # move width/2 to the left to center on coordinate
        self.x_offset = round((text_extent[1].x - text_extent[0].x) / 2)

    def draw(self, color: bool = True, ascii=False) -> Tuple[List[Tuple[int]], List[str]]:
        """Return the indices and values to draw this shape onto the canvas."""
        xs, ys, vals = self.box_shape.draw(color, ascii)

        text_xs, text_ys, text_vals = self.text_shape.draw(color, ascii)

        self.actual_extent = (Point(xs.min() - self.x_offset, ys.min()), Point(xs.max() - self.x_offset, ys.max()))

        return np.append(xs, text_xs) - self.x_offset, np.append(ys, text_ys), np.append(vals, text_vals)

    @property
    def extent(self) -> Tuple[Tuple[int]]:
        """The extent of this shape."""
        box_extent = self.box_shape.extent
        return Point(box_extent[0].x - self.x_offset, box_extent[0].y), Point(
            box_extent[1].x - self.x_offset,
            box_extent[1].y,
        )


class EdgeShape(Shape):
    """An edge between two activities."""

    EDGE_COLORS = ["red", "green", "yellow", "blue", "magenta", "cyan"]
    CURRENT_COLOR = 0

    def __init__(self, start: Point, end: Point, color: str):
        self.start = Point(round(start.x), round(start.y))
        self.end = Point(round(end.x), round(end.y))
        self.color = color
        self.line_indices = self._line_indices(start, end)

    @staticmethod
    def next_color() -> str:
        """Get the next color in the color rotation."""
        EdgeShape.CURRENT_COLOR = (EdgeShape.CURRENT_COLOR + 1) % len(EdgeShape.EDGE_COLORS)
        return EdgeShape.EDGE_COLORS[EdgeShape.CURRENT_COLOR]

    def _line_indices(self, start: Point, end: Point):
        """Interpolate a line."""
        if abs(end.y - start.y) < abs(end.x - start.x):
            # swap x and y, then swap back
            xs, ys = self._line_indices(Point(start.y, start.x), Point(end.y, end.x))
            return (ys, xs)

        if start.y > end.y:
            # swap start and end
            return self._line_indices(end, start)

        x = np.arange(start.y, end.y + 1, dtype=float)
        y = x * (end.x - start.x) / (end.y - start.y) + (end.y * start.x - start.y * end.x) / (end.y - start.y)

        return (np.floor(y).astype(int), x.astype(int))

    def intersects_with(self, other_edge: "EdgeShape") -> bool:
        """Checks whether this edge intersects with other edges."""
        coordinates = set(map(tuple, np.column_stack(self.line_indices)))
        other_coordinates = set(map(tuple, np.column_stack(other_edge.line_indices)))

        return coordinates.intersection(other_coordinates)

    def draw(self, color: bool = True, ascii=False) -> Tuple[List[Tuple[int]], List[str]]:
        """Return the indices and values to draw this shape onto the canvas."""
        xs, ys = self.line_indices
        char = "*"

        if color:
            char = style(char, fg=self.color)

        return xs, ys, len(xs) * [char]

    @property
    def extent(self) -> Tuple[Tuple[int]]:
        """The extent of this shape."""
        return Point(min(self.start.x, self.end.x), min(self.start.y, self.end.y)), Point(
            max(self.start.x, self.end.x), max(self.start.y, self.end.y)
        )


class TextCanvas:
    """A canvas that can draw shapes to text."""

    def __init__(self):
        self.shapes = defaultdict(list)
        self._canvas = None

    def add_shape(self, shape: Shape, layer: int = 0) -> None:
        """Add a shape to the canvas."""
        self.shapes[layer].append(shape)

    def get_coordinates(self, point: Point):
        """Get actual, transformed coordinates of a node."""
        return Point(point.x - self.offset[1], point.y - self.offset[0])

    def render(self, color: bool = True, ascii=False):
        """Render contained shapes onto canvas."""
        extent = (Point(2 ** 64, 2 ** 64), Point(-(2 ** 64), -(2 ** 64)))

        layers = sorted(self.shapes.keys())

        for layer in layers:
            for shape in self.shapes[layer]:
                shape_extent = shape.extent
                extent = (
                    Point(min(extent[0].x, shape_extent[0].x), min(extent[0].y, shape_extent[0].y)),
                    Point(max(extent[1].x, shape_extent[1].x), max(extent[1].y, shape_extent[1].y)),
                )

        self.offset = (extent[0].y, extent[0].x)
        size = (extent[1].y - extent[0].y + 2, extent[1].x - extent[0].x + 2)
        self._canvas = np.chararray(size, unicode=True, itemsize=10)
        self._canvas[:] = " "

        for layer in layers:
            for shape in self.shapes[layer]:
                xs, ys, vals = shape.draw(color=color, ascii=ascii)
                self._canvas[ys - self.offset[0], xs - self.offset[1]] = vals

    @property
    def text(self) -> str:
        """Get the text of the canvas."""
        if self._canvas is None:
            raise ValueError("Call render() before getting text.")
        string_buffer = StringIO()
        np.savetxt(string_buffer, self._canvas, fmt="%1s", delimiter="")
        return string_buffer.getvalue()


class NodeViewer:
    """Gets width/height for a display node."""

    def __init__(self, text):
        lines = text.splitlines()
        self.w = max([len(line) for line in lines]) + 2
        self.h = len(lines) + 2


class ActivityGraphViewModel:
    """A view model for outputting a directed activity graph."""

    def __init__(self, graph: networkx.Graph):
        self.graph = graph

    def _format_vertex(self, node, columns) -> str:
        """Return vertex text for a node."""

        if isinstance(node, Activity):
            return "\n".join(shorten(c(node), MAX_NODE_LENGTH, placeholder="...") for c in columns), True, node

        return node, False, None

    def _subgraph_order_key(self, subgraph):
        """Return a sorting key for ordering subgraphs.

        Subgraphs are ordered by the ended_at_time of the lastest activits in them.
        """
        activity_times = [n.ended_at_time for n in subgraph.nodes() if isinstance(n, Activity)]

        return max(activity_times)

    def layout_graph(self, columns):
        """Create a Sugiyama layout of the graph."""
        columns = [ACTIVITY_GRAPH_COLUMNS[c] for c in columns.split(",")]

        self.layouts = []

        components = networkx.weakly_connected_components(self.graph)
        subgraphs = [self.graph.subgraph(component).copy() for component in components]
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
