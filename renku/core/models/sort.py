# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Process Git repository."""

from collections import deque

GRAY, BLACK = 0, 1


def topological(nodes):
    """Return nodes in a topological order."""
    order, enter, state = deque(), set(nodes), {}

    def dfs(node):
        """Visit nodes in depth-first order."""
        state[node] = GRAY
        for parent in nodes.get(node, ()):
            color = state.get(parent, None)
            if color == GRAY:
                raise ValueError("cycle")
            if color == BLACK:
                continue
            enter.discard(parent)
            dfs(parent)
        order.appendleft(node)
        state[node] = BLACK

    while enter:
        dfs(enter.pop())

    return order
