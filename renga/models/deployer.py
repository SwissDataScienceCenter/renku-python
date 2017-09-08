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
"""Model objects representing contexts and executions."""

from ._datastructures import Collection, Model


class Context(Model):
    """Represent a deployer context."""

    def __str__(self):
        """Format context for console output."""
        return '{0.id} - {0.spec}'.format(self)

    @property
    def spec(self):
        """Specification of the execution context."""
        return self._response.get('spec', {})


class ContextsCollection(Collection):
    """Represent projects on the server."""

    class Meta:
        """Information about individual projects."""

        model = Context

    def __iter__(self):
        """Return all contexts."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_contexts()
        )

    def create(self, spec=None, **kwargs):
        """Create new project."""
        data = self._client.api.create_context(spec)
        return self.Meta.model(data, client=self._client, collection=self)
