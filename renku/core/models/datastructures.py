# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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


class Model(object):
    """Abstract response of a single object."""

    IDENTIFIER_KEY = "identifier"

    def __init__(self, response=None, client=None, collection=None):
        """Create a representation of an object on the server."""
        self._response = response if response is not None else {}
        self._client = client
        self._collection = collection

    @property
    def id(self):
        """The identifier of the object."""
        return self._response[self.IDENTIFIER_KEY]

    def __str__(self):
        """Format model."""
        return "<{0.__class__.__name__} '{0.id!s}'>".format(self)

    __repr__ = __str__


class Collection(object):
    """Abstract response of multiple objects."""

    class Meta:
        """Store information about the model."""

        model = None
        """Define the type of object this collection represents."""

        headers = "id"
        """Which fields to use as headers when printing the collection."""

    def __init__(self, client=None):
        """Create a representation of objects on the server."""
        self._client = client

    def list(self):
        """Return a list if the collection is iterable."""
        if not hasattr(self, "__iter__"):
            raise NotImplementedError("The collection is not iterable.")
        return list(self)


class LazyResponse(dict):
    """Lazy load object properties."""

    def __init__(self, getter, *args, **kwargs):
        """Initialize LazyRequest."""
        self._getter = getter
        self._called = False
        super(LazyResponse, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        """Implement KeyError check."""
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            if not self._called:
                self.update(**self._getter())
                self._called = True
                return dict.__getitem__(self, key)
            raise


class IndexedList(list):
    """List allowing to query items by id or by named index.

    Example:
    >>> from collections import namedtuple
    >>> Item = namedtuple('Item', 'key, value')
    >>> items = IndexedList(Item('a', 1), Item('b', 2), attr='key')
    >>> items[0].value
    1
    >>> items['a'].value
    1
    >>> items.b.value
    2
    >>> items[0] in items
    True
    >>> 'a' in items
    True
    >>> 'c' not in items
    True

    The attribute name must be always defined.

    >>> IndexedList()
    Traceback (most recent call last):
    ...
    ValueError: The attribute name must be defined.

    """

    __slots__ = ("_attr_name", "_prefix")

    def __new__(cls, *args, attr=None, prefix=""):
        """Call list constructor."""
        return super().__new__(cls)

    def __init__(self, *args, attr=None, prefix=""):
        """Store index information."""
        if attr is None:
            raise ValueError("The attribute name must be defined.")

        self._attr_name = attr
        self._prefix = prefix

        self.extend(args)

    def __contains__(self, attr):
        """Check existence of attribute value or object itself."""
        #: Check if the instance is in the list.
        rval = list.__contains__(self, attr)
        if rval:
            return rval

        #: Find item by attribute value.
        try:
            getattr(self, attr)
            return True
        except (AttributeError, TypeError):
            return False

    def __getattr__(self, attr):
        """Find item by named index."""
        attr_name = self._prefix + attr
        for item in self:
            #: Find object by attribute value.
            if getattr(item, self._attr_name) == attr_name:
                return item

        #: Return instance attrubutes.
        return list.__getattribute__(self, attr)

    def __getitem__(self, index):
        """Find item by named index."""
        if isinstance(index, int):
            return list.__getitem__(self, index)

        try:
            return getattr(self, index)
        except AttributeError:
            raise IndexError("No item found with id {0}".format(self._prefix + index))


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
        queue = deque()
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
