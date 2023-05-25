# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

import abc
import math
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, Generator, NamedTuple, Optional, Tuple

from click import style


class Point(NamedTuple):
    """A point with coordinates for rendering."""

    x: int
    y: int


class Size(NamedTuple):
    """A size for a two dimensional matrix."""

    width: int
    height: int


class EdgeSet(NamedTuple):
    """A collection of edge characters for rendering boxes."""

    top_left: str
    bottom_left: str
    top_right: str
    bottom_right: str


@dataclass
class CharacterSet:
    """A rendering character set for rendering Ascii content."""

    edge: EdgeSet
    horizontal: str
    vertical: str

    def style(self, transform: Callable[[str], str]) -> None:
        """Apply a style to this character set (like bold).

        Args:
            transform(Callable[[str], str]): Function that applies a style to a string.
        """
        self.horizontal = transform(self.horizontal)
        self.vertical = transform(self.vertical)

        self.edge = EdgeSet(
            transform(self.edge.top_left),
            transform(self.edge.bottom_left),
            transform(self.edge.top_right),
            transform(self.edge.bottom_right),
        )


MAX_NODE_LENGTH = 40


def line_points(start: Point, end: Point) -> Generator[Point, None, None]:
    """Return all (discrete) points of a line from start to end.

    Args:
        start(Point): Starting point of the line.
        end(Point): End point of the line.
    Returns:
        Generator[Point, None, None]: All discrete points part of the line.
    """
    x_extent = end.x - start.x
    y_extent = end.y - start.y

    length = max(abs(x_extent), abs(y_extent)) + 1
    assert length > 0
    for i in range(length):
        position = i / length
        yield Point(math.ceil(start.x + position * x_extent), math.ceil(start.y + position * y_extent))


class TextMatrix:
    """A two dimensional matrix of strings.

    Indices need to be positive. To support handling negative indices, you can set x/y-offset to the lowest
    negative index you want to pass and indices will be shifted by this amount internally.

    Args:
        size(Tuple[int,int]): The width and height of the matrix.
        x_offset(int): Offset to apply to x indices.
        y_offset(int): Offset to apply to y indices.
    """

    def __init__(self, size: Size, x_offset: int = 0, y_offset: int = 0) -> None:
        assert size.width > 0
        assert size.height > 0
        self._size = size
        self.x_offset = x_offset
        self.y_offset = y_offset
        self._content = []

        for _ in range(self.size.height):
            row = []
            for _ in range(self.size.width):
                row.append(" ")
            self._content.append(row)

    @property
    def size(self) -> Size:
        """The size of the matrix."""
        return self._size

    def __getitem__(self, index: Tuple[int, int]) -> str:
        """Get element at width,height position."""
        pos = (index[0] + self.x_offset, index[1] + self.y_offset)
        assert 0 <= pos[0] < self.size.width
        assert 0 <= pos[1] < self.size.height
        return self._content[pos[1]][pos[0]]

    def __setitem__(self, index: Tuple[int, int], value: str) -> None:
        """Set element at width,height position."""
        pos = (index[0] + self.x_offset, index[1] + self.y_offset)
        assert 0 <= pos[0] < self.size.width
        assert 0 <= pos[1] < self.size.height
        self._content[pos[1]][pos[0]] = value

    def __str__(self) -> str:
        return "\n".join(["".join(row) for row in self._content])

    def draw_line(self, start: Point, end: Point, value: str) -> None:
        """Fill a line from start to end with value.

        Args:
            start(Point): The starting point.
            end(Point): The end point.
            value(str): The value (character) to use for representing the line.
        """
        for point in line_points(start, end):
            self[point.x, point.y] = value

    def draw(self, start: Point, value: "TextMatrix") -> None:
        """Write content from value with start being upper-left corner.

        Args:
            start(Point): The upper-left corner for where to place the content.
            value(TextMatrix): The content text matrix to place into this one.
        """
        for y in range(start.y, start.y + value.size.height):
            for x in range(start.x, start.x + value.size.width):
                self[x, y] = value[x - start.x, y - start.y]


class Shape(abc.ABC):
    """Basic shape class that all shapes inherit from."""

    def draw(self, matrix: TextMatrix, color: bool = True, ascii=False) -> None:
        """Draw self onto a text matrix..

        Args:
            matrix(TextMatrix): The text matrix to draw on.
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """
        raise NotImplementedError()


class RectangleShape(Shape):
    """A rectangle shape."""

    ASCII_CHARACTERS = CharacterSet(edge=EdgeSet("+", "+", "+", "+"), horizontal="-", vertical="|")

    UNICODE_CHARACTERS = CharacterSet(edge=EdgeSet("┌", "└", "┐", "┘"), horizontal="─", vertical="│")

    UNICODE_CHARACTERS_DOUBLE = CharacterSet(edge=EdgeSet("╔", "╚", "╗", "╝"), horizontal="═", vertical="║")

    def __init__(self, start: Point, end: Point, double_border=False, color: Optional[str] = None):
        self.start = start
        self.end = end
        self.double_border = double_border
        self.color = color

    def draw(self, matrix: TextMatrix, color: bool = True, ascii=False) -> None:
        """Draw self onto a text matrix..

        Args:
            matrix(TextMatrix): The text matrix to draw on.
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """
        if not ascii and self.double_border:
            characters = deepcopy(self.UNICODE_CHARACTERS_DOUBLE)
        else:
            characters = deepcopy(self.ASCII_CHARACTERS if ascii else self.UNICODE_CHARACTERS)

        if color and self.color:
            # NOTE: Add color to border characters
            characters.style(partial(style, fg=self.color))
        # horizontal lines
        matrix.draw_line(
            Point(self.start.x + 1, self.start.y), Point(self.end.x - 1, self.start.y), characters.horizontal
        )
        matrix.draw_line(Point(self.start.x + 1, self.end.y), Point(self.end.x - 1, self.end.y), characters.horizontal)

        # vertical lines
        matrix.draw_line(Point(self.start.x, self.start.y + 1), Point(self.start.x, self.end.y), characters.vertical)
        matrix.draw_line(Point(self.end.x, self.start.y + 1), Point(self.end.x, self.end.y), characters.vertical)

        # set corners
        matrix[self.start.x, self.start.y] = characters.edge.top_left
        matrix[self.start.x, self.end.y] = characters.edge.bottom_left
        matrix[self.end.x, self.start.y] = characters.edge.top_right
        matrix[self.end.x, self.end.y] = characters.edge.bottom_right

        # fill with whitespace to force overwriting underlying text
        content = TextMatrix(Size(self.end.x - self.start.x - 1, self.end.y - self.start.y - 1))
        matrix.draw(Point(self.start.x + 1, self.start.y + 1), content)

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

    def draw(self, matrix: TextMatrix, color: bool = True, ascii=False) -> None:
        """Draw self onto a text matrix..

        Args:
            matrix(TextMatrix): The text matrix to draw on.
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """

        for y, line in enumerate(self.text):
            for x, char in enumerate(line):
                kwargs: Dict[str, Any] = dict()
                if self.bold:
                    kwargs["bold"] = True
                if color and self.color:
                    kwargs["fg"] = self.color

                if kwargs:
                    char = style(char, **kwargs)

                matrix[self.point.x + x, self.point.y + y] = char

    @property
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        max_line_len = max(len(line) for line in self.text)
        num_lines = len(self.text)
        return (self.point, Point(self.point.x + max_line_len - 1, self.point.y + num_lines - 1))


class NodeShape(Shape):
    """An activity node shape."""

    def __init__(self, text: str, point: Point, double_border=False, color: Optional[str] = None):
        self.point = point
        self.text_shape = TextShape(text, Point(self.point.x + 1, self.point.y + 1), bold=double_border, color=color)
        text_extent = self.text_shape.extent
        self.box_shape = RectangleShape(
            self.point,
            Point(text_extent[1].x + 1, text_extent[1].y + 1),
            double_border=double_border,
            color=color,
        )

    def draw(self, matrix: TextMatrix, color: bool = True, ascii=False) -> None:
        """Draw self onto a text matrix..

        Args:
            matrix(TextMatrix): The text matrix to draw on.
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """
        self.actual_extent = self.extent
        self.box_shape.draw(matrix, color=color, ascii=ascii)
        self.text_shape.draw(matrix, color=color, ascii=ascii)

    @property
    def extent(self) -> Tuple[Point, Point]:
        """The extent of this shape.

        Returns:
            Bounds of this shape.
        """
        box_extent = self.box_shape.extent
        return Point(box_extent[0].x, box_extent[0].y), Point(
            box_extent[1].x,
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

    @staticmethod
    def next_color() -> str:
        """Get the next color in the color rotation.

        Returns:
            Next color string to use.
        """
        EdgeShape.CURRENT_COLOR = (EdgeShape.CURRENT_COLOR + 1) % len(EdgeShape.COLORS)
        return EdgeShape.COLORS[EdgeShape.CURRENT_COLOR]

    def intersects_with(self, other_edge: "EdgeShape") -> bool:
        """Checks whether this edge intersects with other edges.

        Args:
            other_edge("EdgeShape"): Edge to check intersection with.

        Returns:
            bool: True if this edge intersects ``other_edge``, False otherwise.
        """
        return not set(line_points(self.start, self.end)).intersection(set(line_points(self.start, self.end)))

    def draw(self, matrix: TextMatrix, color: bool = True, ascii=False) -> None:
        """Draw self onto a text matrix..

        Args:
            matrix(TextMatrix): The text matrix to draw on.
            color(bool, optional): Whether or not to render in color (Default value = True).
            ascii:  Whether to use ascii characters only or with UTF8 (Default value = False).
        """
        char = "*"

        if color:
            char = style(char, fg=self.color)

        matrix.draw_line(self.start, self.end, char)

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
        return Point(point.x + self.x_offset, point.y)

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
                    Point(
                        math.floor(min(extent[0].x, shape_extent[0].x)), math.floor(min(extent[0].y, shape_extent[0].y))
                    ),
                    Point(
                        math.ceil(max(extent[1].x, shape_extent[1].x)), math.ceil(max(extent[1].y, shape_extent[1].y))
                    ),
                )
        self.x_offset = -extent[0].x
        size = Size(extent[1].x - extent[0].x + 30, extent[1].y - extent[0].y + 2)
        self._canvas = TextMatrix(size, x_offset=self.x_offset, y_offset=-extent[0].y)

        for layer in layers:
            for shape in self.shapes[layer]:
                shape.draw(self._canvas, color=color, ascii=ascii)

    @property
    def text(self) -> str:
        """Get the text of the canvas.

        Returns:
            String containing rendered canvas.
        """
        if self._canvas is None:
            raise ValueError("Call render() before getting text.")

        return str(self._canvas)
