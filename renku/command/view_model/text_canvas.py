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

from collections import defaultdict
from copy import deepcopy
from io import StringIO
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple

import numpy as np
from click import style

if TYPE_CHECKING:
    import numpy.typing as npt

Point = NamedTuple("Point", [("x", int), ("y", int)])

MAX_NODE_LENGTH = 40


class Shape:
    """basic shape class that all shapes inherit from."""

    pass


class RectangleShape(Shape):
    """A rectangle shape."""

    ASCII_CHARACTERS = {"edge": ["+"] * 4, "horizontal": "-", "vertical": "|"}

    UNICODE_CHARACTERS = {"edge": ["┌", "└", "┐", "┘"], "horizontal": "─", "vertical": "│"}

    UNICODE_CHARACTERS_DOUBLE = {"edge": ["╔", "╚", "╗", "╝"], "horizontal": "═", "vertical": "║"}

    def __init__(self, start: Point, end: Point, double_border=False, color: Optional[str] = None):
        self.start = start
        self.end = end
        self.double_border = double_border
        self.color = color

    def draw(
        self, color: bool = True, ascii=False
    ) -> Tuple["npt.NDArray[np.int32]", "npt.NDArray[np.int32]", "npt.NDArray[np.str_]"]:
        """Return the indices and values to draw this shape onto the canvas.

        Args:
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).

        Returns:
            Tuple[List[Tuple[int]],List[str]]: Tuple of list of coordinates and list if characters
                at those coordinates.

        """
        if not ascii and self.double_border:
            characters = deepcopy(self.UNICODE_CHARACTERS_DOUBLE)
        else:
            characters = deepcopy(self.ASCII_CHARACTERS if ascii else self.UNICODE_CHARACTERS)

        if color and self.color:
            # NOTE: Add color to border characters
            for key, value in list(characters.items()):
                if isinstance(value, list):
                    characters[key] = [style(v, fg=self.color) for v in value]
                else:
                    characters[key] = style(value, fg=self.color)

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
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        return self.start, self.end


class TextShape(Shape):
    """A text object."""

    def __init__(self, text: str, point: Point, bold: bool = False, color: Optional[str] = None):
        self.point = point
        self.text = text.splitlines()
        self.bold = bold
        self.color = color

    def draw(
        self, color: bool = True, ascii=False
    ) -> Tuple["npt.NDArray[np.int32]", "npt.NDArray[np.int32]", "npt.NDArray[np.str_]"]:
        """Return the indices and values to draw this shape onto the canvas.

        Args:
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).

        Returns:
            Tuple[List[Tuple[int]],List[str]]: Tuple of list of coordinates and list if characters
                at those coordinates.
        """
        xs: List[int] = []
        ys: List[int] = []
        vals = []

        current_x = self.point.x
        current_y = self.point.y

        for line in self.text:
            for char in line:
                xs.append(current_x)
                ys.append(current_y)
                kwargs: Dict[str, Any] = dict()
                if self.bold:
                    kwargs["bold"] = True
                if color and self.color:
                    kwargs["fg"] = self.color

                if kwargs:
                    vals.append(style(char, **kwargs))
                else:
                    vals.append(char)

                current_x += 1
            current_x = self.point.x
            current_y += 1

        return np.array(xs), np.array(ys), np.array(vals)

    @property
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        max_line_len = max(len(line) for line in self.text)
        num_lines = len(self.text)
        return (self.point, Point(self.point.x + max_line_len, self.point.y + num_lines - 1))


class NodeShape(Shape):
    """An activity node shape."""

    def __init__(self, text: str, point: Point, double_border=False, color: Optional[str] = None):
        self.point = Point(round(point.x), round(point.y - len(text.splitlines())))
        self.text_shape = TextShape(text, self.point, bold=double_border, color=color)

        text_extent = self.text_shape.extent
        self.box_shape = RectangleShape(
            Point(text_extent[0].x - 1, text_extent[0].y - 1),
            Point(text_extent[1].x + 1, text_extent[1].y + 1),
            double_border=double_border,
            color=color,
        )

        # move width/2 to the left to center on coordinate
        self.x_offset = round((text_extent[1].x - text_extent[0].x) / 2)

    def draw(
        self, color: bool = True, ascii=False
    ) -> Tuple["npt.NDArray[np.int32]", "npt.NDArray[np.int32]", "npt.NDArray[np.str_]"]:
        """Return the indices and values to draw this shape onto the canvas.

        Args:
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).

        Returns:
            Tuple[List[Tuple[int]],List[str]]: Tuple of list of coordinates and list if characters
                at those coordinates.
        """
        xs, ys, vals = self.box_shape.draw(color, ascii)

        text_xs, text_ys, text_vals = self.text_shape.draw(color, ascii)

        self.actual_extent = (Point(xs.min() - self.x_offset, ys.min()), Point(xs.max() - self.x_offset, ys.max()))

        return np.append(xs, text_xs) - self.x_offset, np.append(ys, text_ys), np.append(vals, text_vals)

    @property
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        box_extent = self.box_shape.extent
        return Point(box_extent[0].x - self.x_offset, box_extent[0].y), Point(
            box_extent[1].x - self.x_offset,
            box_extent[1].y,
        )


class EdgeShape(Shape):
    """An edge between two activities."""

    COLORS = ["red", "green", "yellow", "blue", "magenta", "cyan"]
    CURRENT_COLOR = 0

    def __init__(self, start: Point, end: Point, color: str):
        self.start = Point(round(start.x), round(start.y))
        self.end = Point(round(end.x), round(end.y))
        self.color = color
        self.line_indices = self._line_indices(start, end)

    @staticmethod
    def next_color() -> str:
        """Get the next color in the color rotation.

        Returns:
            Next color string to use.
        """
        EdgeShape.CURRENT_COLOR = (EdgeShape.CURRENT_COLOR + 1) % len(EdgeShape.COLORS)
        return EdgeShape.COLORS[EdgeShape.CURRENT_COLOR]

    def _line_indices(self, start: Point, end: Point):
        """Interpolate a line.

        Args:
            start(Point): Starting point of line.
            end(Point): Ending point of line.

        Returns:
            Tuple of all x,y coordinates of points in this line.
        """
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
        """Checks whether this edge intersects with other edges.

        Args:
            other_edge("EdgeShape"): Edge to check intersection with.

        Returns:
            bool: True if this edge intersects ``other_edge``, False otherwise.
        """
        coordinates = set(map(tuple, np.column_stack(self.line_indices)))
        other_coordinates = set(map(tuple, np.column_stack(other_edge.line_indices)))

        return coordinates.intersection(other_coordinates) is not None

    def draw(
        self, color: bool = True, ascii=False
    ) -> Tuple["npt.NDArray[np.int32]", "npt.NDArray[np.int32]", "npt.NDArray[np.str_]"]:
        """Return the indices and values to draw this shape onto the canvas.

        Args:
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).

        Returns:
            Tuple[List[Tuple[int]],List[str]]: Tuple of list of coordinates and list if characters
                at those coordinates.

        """
        xs, ys = self.line_indices
        char = "*"

        if color:
            char = style(char, fg=self.color)

        return xs, ys, np.array(len(xs) * [char])

    @property
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        return Point(min(self.start.x, self.end.x), min(self.start.y, self.end.y)), Point(
            max(self.start.x, self.end.x), max(self.start.y, self.end.y)
        )


class TextCanvas:
    """A canvas that can draw shapes to text."""

    def __init__(self):
        self.shapes = defaultdict(list)
        self._canvas = None

    def add_shape(self, shape: Shape, layer: int = 0) -> None:
        """Add a shape to the canvas.

        Args:
            shape(Shape): The shape to add.
            layer(int, optional): The layer to add the shape to (Default value = 0).
        """
        self.shapes[layer].append(shape)

    def get_coordinates(self, point: Point) -> Point:
        """Get actual, transformed coordinates of a node.

        Args:
            point(Point): The point to get coordinates for.

        Returns:
            Point: Coordinates in parent coordinate system.
        """
        return Point(point.x - self.offset[1], point.y - self.offset[0])

    def render(self, color: bool = True, ascii=False):
        """Render contained shapes onto canvas.

        Args:
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """
        extent = (Point(2**64, 2**64), Point(-(2**64), -(2**64)))

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
        """Get the text of the canvas.

        Returns:
            String containing rendered canvas.
        """
        if self._canvas is None:
            raise ValueError("Call render() before getting text.")
        string_buffer = StringIO()
        np.savetxt(string_buffer, self._canvas, fmt="%1s", delimiter="")
        return string_buffer.getvalue()
