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

import os

from dateutil.parser import parse as dateparse
from werkzeug.datastructures import MultiDict

from renku.errors import APIError, RenkuException

from ._datastructures import Collection, Model
from .storage import File


class SlotCollection(Collection):
    """Represent input and output slots attached to a context."""

    class Meta:
        """Information about individual inputs."""

        model = File

        headers = ('id', 'filename')

    @property
    def _names(self):
        """Build the collection from labels."""
        return {
            key[len(self._prefix):]: value
            for key, value in self._context.labels.items()
            if key.startswith(self._prefix)
        }

    def __init__(self, context, prefix=None, env_tpl=None, **kwargs):
        """Initialize a collection of context inputs."""
        self._context = context
        self._prefix = prefix or 'renku.context.inputs.'
        self._env_tpl = env_tpl or 'RENKU_CONTEXT_INPUTS_{0}'
        super(SlotCollection, self).__init__(**kwargs)

    def __contains__(self, name):
        """Check if a name is defined."""
        env = getattr(self._client, '_environment', os.environ)
        file_id = env.get(
            self._env_tpl.format(name.upper()), self._names[name]
        )
        return file_id is not None

    def __getitem__(self, name):
        """Return a file object."""
        env = getattr(self._client, '_environment', os.environ)
        file_id = env.get(
            self._env_tpl.format(name.upper()), self._names[name]
        )
        if file_id is None:
            raise KeyError(name)

        return self.Meta.model(
            self._client.api.get_file(file_id),
            client=self._client,
            collection=self
        )

    def __setitem__(self, name, value):
        """Set a file object reference."""
        if name in self._names:  # pragma: no cover
            raise RenkuException(
                'Can not modify an existing slot "{0}"'.format(name)
            )

        if isinstance(value, self.Meta.model):
            value = value.id

        self._context.spec['labels'].append(
            '{0}{1}{2}'.format(
                self._prefix, name, '={0}'.format(value)
                if value is not None else ''
            )
        )


class Context(Model):
    """Represent a deployer context."""

    def __str__(self):
        """Format context for console output."""
        return '{0.id} - {0.spec}'.format(self)

    @property
    def created(self):
        """Return the creation timestamp."""
        dt = self._response.get('created', None)
        return dateparse(dt) if dt else dt

    @property
    def spec(self):
        """Specification of the execution context."""
        self._response.setdefault('spec', {})
        return self._response['spec']

    @property
    def labels(self):
        """Return the context labels."""
        self.spec.setdefault('labels', [])
        return _dict_from_labels(self.spec['labels'])

    @property
    def inputs(self):
        """Return the context input objects."""
        return SlotCollection(self, client=self._client)

    @property
    def outputs(self):
        """Return the context output objects."""
        return SlotCollection(
            self,
            prefix='renku.context.outputs.',
            env_tpl='RENKU_CONTEXT_OUTPUTS_{0}',
            client=self._client
        )

    @property
    def image(self):
        """Image used for the executions."""
        return self.spec['image']

    @property
    def vertex_id(self):
        """Graph vertex id."""
        return self.labels.get('renku.execution_context.vertex_id')

    def run(self, inputs=None, outputs=None, **kwargs):
        """Execute the context.

        Optionally provide new values for input and output slots.  Following
        example shows how to create new execution from the current context with
        different files attached to input and output slots.

        .. code-block:: python

            execution = client.current_context.run(
                engine='docker',
                inputs={
                    'notebook': client.buckets[1234].file[9876].clone(),
                },
                outputs={
                    'plot': client.buckets[1234].create('plot.png'),
                },
            )
            print(execution.url)

        """
        inputs = inputs or {}
        outputs = outputs or {}

        # Make sure that the given environment is updated.
        kwargs.setdefault('environment', {})
        environment = kwargs['environment']

        client_environment = getattr(self._client, '_environment', os.environ)

        def update_env(environment, slots, values):
            """Update environment with values not used in slots."""
            for name, value in slots._names.items():
                new_value = values.get(name, value)

                # Support identifier or a File instance.
                if isinstance(new_value, File):
                    new_value = new_value.id

                # Update only if they are different.
                if new_value != value:
                    environment[self.inputs._env_tpl.format(name.upper())
                                ] = new_value

        try:
            self._client._environment = {}
            update_env(environment, self.inputs, inputs)
            update_env(environment, self.outputs, outputs)
        finally:
            self._client._environment = client_environment

        execution = self._client.api.create_execution(self.id, **kwargs)
        execution['context_id'] = self.id
        return Execution(execution, client=self._client, collection=self)

    @property
    def executions(self):
        """Return the collection of context executions."""
        return ExecutionCollection(self.id, client=self._client)

    @property
    def lineage(self):
        """Return the lineage of this context."""
        return self._client.api.get_context_lineage(
            self.labels.get('renku.execution_context.vertex_id')
        )


class ContextCollection(Collection):
    """Represent a collection of contexts."""

    class Meta:
        """Information about an individual context."""

        model = Context

        headers = ('id', 'created', 'vertex_id', 'image')

    def __iter__(self):
        """Return all contexts."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_contexts()
        )

    def __getitem__(self, context_id):
        """Return the context definition."""
        return self.Meta.model(
            self._client.api.get_context(context_id),
            client=self._client,
            collection=self
        )

    def create(self, spec=None, **kwargs):
        """Create a new context."""
        data = self._client.api.create_context(spec)
        return self.Meta.model(data, client=self._client, collection=self)


class Execution(Model):
    """Represent a context execution."""

    @property
    def context_id(self):
        """Return the associated context id."""
        return self._collection.id

    @property
    def created(self):
        """Return the creation timestamp."""
        dt = self._response.get('created', None)
        return dateparse(dt) if dt else dt

    @property
    def engine(self):
        """Return the execution engine."""
        return self._response.get('engine', {})

    @property
    def environment(self):
        """Return the execution environment variables."""
        return self._response.get('environment', {})

    @property
    def ports(self):
        """Return runtime port mapping."""
        try:
            return self._client.api.execution_ports(self.context_id, self.id)
        except APIError:
            return None

    @property
    def state(self):
        """Return the state of the execution."""
        return self._response.get('state', '')

    @property
    def url(self):
        """Return a URL for accessing the running container."""
        ports = self.ports
        if ports:
            token = self.context.labels.get('renku.notebook.token', '')
            try:
                # FIXME use edge when defined
                env = getattr(self._client, '_environment', os.environ)
                self._client._environment = self.environment
                filename = self._client.contexts[self.context_id
                                                 ].inputs['notebook'].filename
                filename = 'notebooks/current_context/inputs/notebook'
            except Exception:  # pragma: no cover
                # TODO add logging
                filename = ''
            finally:
                self._client._environment = env

            if token:
                token = '?token={0}'.format(token)
            return 'http://{host}:{exposed}/{filename}{token}'.format(
                token=token, filename=filename, **ports[0]
            )

    @property
    def context(self):
        """Return the related context."""
        return self._client.contexts[self.context_id]

    def logs(self, **kwargs):
        """Get logs from this execution."""
        return self._client.api.execution_logs(
            self.context_id, self.id, **kwargs
        )

    def stop(self):
        """Stop a running execution."""
        return self._client.api.stop_execution(self.context_id, self.id)


class ExecutionCollection(Collection):
    """Represent a collection of executions."""

    class Meta:
        """Information about an individual execution."""

        model = Execution

        headers = ('id', 'created', 'engine', 'ports', 'url', 'state')

    def __init__(self, context_id, **kwargs):
        """Initialize the collection of context executions."""
        super(ExecutionCollection, self).__init__(**kwargs)
        self.id = context_id

    def __iter__(self):
        """Return all executions."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_executions(self.id)
        )

    def __getitem__(self, execution_id):
        """Return the execution definition."""
        return self.Meta.model(
            self._client.api.get_execution(self.id, execution_id),
            client=self._client,
            collection=self
        )


def _dict_from_labels(labels, separator='='):
    """Create a multidict from label string."""
    return MultiDict(
        ((label[0].strip(), label[1].strip() if len(label) > 1 else None)
         for label in (raw.split(separator, 1) for raw in labels))
    )
