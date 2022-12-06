# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
"""Base classes for Model objects used in Python SDK."""

import os
from collections import deque
from pathlib import Path
from typing import Deque, List, Tuple


class DirectoryTree(dict):
    r"""Create a safe directory tree from paths.

    Example usage:

    >>> directory = DirectoryTree()
    >>> directory.add('a/b/c')
    >>> directory.add('a/b/c/d')
    >>> directory.add('x/y/z')
    >>> directory.add('x/y/zz')
    >>> print('\n'.join(sorted(directory)))
    a/b/c/d
    x/y/z
    x/y/zz
    >>> print('\n'.join(sorted(directory.get('x/y'))))
    z
    zz

    """

    @classmethod
    def from_list(cls, values):
        """Construct a tree from a list with paths."""
        self = cls()
        for value in values:
            self.add(value)
        return self

    def get(self, value, default=None):
        """Return a subtree if exists."""
        path = value if isinstance(value, Path) else Path(str(value))
        subtree = self
        for part in path.parts:
            try:
                subtree = subtree[part]
            except KeyError:
                return default
        return subtree

    def add(self, value):
        """Create a safe directory from a value."""
        path = value if isinstance(value, Path) else Path(str(value))
        if path and path != path.parent:
            destination = self
            for part in path.parts:
                destination = destination.setdefault(part, DirectoryTree())

    def __iter__(self):
        """Yield all stored directories."""
        filter = {
            os.path.sep,
        }
        queue: Deque[Tuple["DirectoryTree", List[str]]] = deque()
        queue.append((self, []))

        while queue:
            data, parents = queue.popleft()
            for key, value in dict.items(data):
                if key in filter:
                    continue
                if value:
                    queue.append((value, parents + [key]))
                else:
                    yield os.path.sep.join(parents + [key])
