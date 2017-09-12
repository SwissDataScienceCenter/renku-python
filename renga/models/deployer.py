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

from werkzeug.datastructures import MultiDict

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

    @property
    def labels(self):
        """Return the context labels."""
        return _dict_from_labels(self.spec.get('labels', []))

    @property
    def image(self):
        """Image used for the executions."""
        return self.spec['image']

    @property
    def vertex_id(self):
        """Graph vertex id."""
        labels = self.spec.get('labels', {})
        return self.labels.get('renga.execution_context.vertex_id')

    def run(self, **kwargs):
        """Execute the context."""
        execution = self._client.api.create_execution(self.id, **kwargs)
        execution['context_id'] = self.id
        return Execution(execution, client=self._client, collection=self)

    @property
    def executions(self):
        """Return the collection of context executions."""
        return ExecutionsCollection(self.id, client=self._client)

    @property
    def lineage(self):
        """Return the lineage of this context."""
        return self._client.api.get_context_lineage(
            self.labels.get('renga.execution_context.vertex_id'))


class ContextsCollection(Collection):
    """Represent projects on the server."""

    class Meta:
        """Information about individual projects."""

        model = Context

        headers = ('id', 'vertex_id', 'image')

    def __iter__(self):
        """Return all contexts."""
        return (self.Meta.model(data, client=self._client, collection=self)
                for data in self._client.api.list_contexts())

    def __getitem__(self, context_id):
        """Return the context definition."""
        return self.Meta.model(
            self._client.api.get_context(context_id),
            client=self._client,
            collection=self)

    def create(self, spec=None, **kwargs):
        """Create new project."""
        data = self._client.api.create_context(spec)
        return self.Meta.model(data, client=self._client, collection=self)


class Execution(Model):
    """Represent a context execution."""

    @property
    def context_id(self):
        """Return the associated context id."""
        return self._collection.id

    @property
    def engine(self):
        """Return the execution engine."""
        return self._response.get('engine', {})

    @property
    def ports(self):
        """Return runtime port mapping."""
        return self._client.api.execution_ports(self.context_id, self.id)

    @property
    def url(self):
        """Return a URL for accessing the running container."""
        ports = self.ports
        token = self.context.labels.get('renga.notebook.token', '')
        if token:
            token = '/?token={0}'.format(token)
        return 'http://{host}:{exposed}{token}'.format(
            token=token, **ports[0])

    @property
    def context(self):
        """Return the related context."""
        return self._client.contexts[self.context_id]

    def logs(self, **kwargs):
        """Get logs from this execution."""
        return self._client.api.execution_logs(self.context_id, self.id,
                                               **kwargs)

    def stop(self):
        """Stop running execution."""
        return self._client.api.stop_execution(self.context_id, self.id)


class ExecutionsCollection(Collection):
    """Represent projects on the server."""

    class Meta:
        """Information about individual projects."""

        model = Execution

        headers = ('id', 'context_id', 'engine', 'ports')

    def __init__(self, context_id, **kwargs):
        """Initialize the collection of context executions."""
        super().__init__(**kwargs)
        self.id = context_id

    def __iter__(self):
        """Return all contexts."""
        return (self.Meta.model(data, client=self._client, collection=self)
                for data in self._client.api.list_executions(self.id))


def _dict_from_labels(labels, separator='='):
    """Create a multidict from label string."""
    return MultiDict(((label[0].strip(), label[1].strip())
                      for label in (raw.split(separator, 1)
                                    for raw in labels)))
