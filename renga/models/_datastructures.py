# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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


class Model(object):
    """Abstract response of a single object."""

    IDENTIFIER_KEY = 'identifier'

    def __init__(self, response=None, client=None, collection=None):
        """Create representation of object on the server."""
        self._response = response if response is not None else {}
        self._client = client
        self._collection = collection

    @property
    def id(self):
        """The identifier of the object."""
        return self._response[self.IDENTIFIER_KEY]

    def __str__(self):
        """Format model."""
        return '<{0.__class__.__name__} {0.id!r}>'.format(self)


class Collection(object):
    """Abstract response of multiple objects."""

    class Meta:
        """Store information about model."""

        model = None
        """Define type of object this collection represents."""

        headers = ('id')
        """Which fields to use as headers when printing the collection."""

    def __init__(self, client=None):
        """Create representation of objects on the server."""
        self._client = client

    def list(self):
        """Return list if the collection is iterable."""
        if not hasattr(self, '__iter__'):
            raise NotImplemented('The collection is not iterable.')
        return list(self)
