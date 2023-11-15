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
"""Renku TextCanvas tests."""

import pytest

from renku.command.view_model.text_canvas import NodeShape, Point, RectangleShape, Size, TextMatrix


def test_text_matrix():
    """Test that TextMatrix works."""

    matrix = TextMatrix(size=Size(5, 5))

    matrix[0, 0] = "a"
    matrix[2, 2] = "b"
    matrix[4, 4] = "c"
    matrix[0, 4] = "d"
    matrix[4, 0] = "e"

    assert matrix[0, 0] == "a"
    assert matrix[2, 2] == "b"
    assert matrix[4, 4] == "c"
    assert matrix[0, 4] == "d"
    assert matrix[4, 0] == "e"

    with pytest.raises(AssertionError):
        matrix[-1, 0] = "x"

    with pytest.raises(AssertionError):
        matrix[0, -1] = "x"

    with pytest.raises(AssertionError):
        matrix[-1, -1] = "x"

    with pytest.raises(AssertionError):
        matrix[5, 4] = "x"

    with pytest.raises(AssertionError):
        matrix[4, 5] = "x"

    with pytest.raises(AssertionError):
        matrix[5, 5] = "x"

    assert str(matrix) == "a   e\n     \n  b  \n     \nd   c"

    matrix2 = TextMatrix(size=Size(5, 5), x_offset=1, y_offset=2)
    matrix2[0, 0] = "a"
    matrix2[2, 2] = "b"
    matrix2[-1, -1] = "c"
    matrix2[-1, -2] = "d"

    assert matrix2[0, 0] == "a"
    assert matrix2[2, 2] == "b"
    assert matrix2[-1, -1] == "c"
    assert matrix2[-1, -2] == "d"

    with pytest.raises(AssertionError):
        matrix2[5, 4] = "x"

    with pytest.raises(AssertionError):
        matrix2[-2, -3] = "x"

    assert str(matrix2) == "d    \nc    \n a   \n     \n   b "


@pytest.mark.parametrize(
    "start,end,double,result",
    [
        (Point(1, 1), Point(7, 4), False, "        \n ┌─────┐\n │     │\n │     │\n └─────┘"),
        (Point(0, 0), Point(3, 3), False, "┌──┐    \n│  │    \n│  │    \n└──┘    \n        "),
        (Point(1, 1), Point(4, 3), True, "        \n ╔══╗   \n ║  ║   \n ╚══╝   \n        "),
    ],
)
def test_rectangle_shape(start, end, double, result):
    """Test rendering a rectangle shape."""
    matrix = TextMatrix(size=Size(8, 5))

    shape = RectangleShape(start, end, double_border=double)

    shape.draw(matrix)

    assert str(matrix) == result


@pytest.mark.parametrize(
    "text,point,result",
    [
        ("A", Point(4, 4), "        \n        \n        \n        \n    ┌─┐ \n    │A│ \n    └─┘ \n        "),
        ("AB DE", Point(1, 0), " ┌─────┐\n │AB DE│\n └─────┘\n        \n        \n        \n        \n        "),
        ("A B\nC D", Point(2, 2), "        \n        \n  ┌───┐ \n  │A B│ \n  │C D│ \n  └───┘ \n        \n        "),
    ],
)
def test_node_shape(text, point, result):
    """Test rendering a node shape."""
    matrix = TextMatrix(size=Size(8, 8))

    shape = NodeShape(text=text, point=point)

    shape.draw(matrix)

    assert str(matrix) == result
