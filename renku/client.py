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
"""Python SDK client for Renku platform."""

import os

from .api import APIClient


class RenkuClient(object):
    """A client for communicating with a Renku platform.

    Example:

        >>> import renku
        >>> client = renku.RenkuClient('http://localhost')

    """

    def __init__(self, *args, **kwargs):
        """Create a Renku API client."""
        self.api = APIClient(*args, **kwargs)

    @classmethod
    def from_env(cls, environment=None):
        """Return a client configured from environment variables.

        .. envvar:: RENKU_ENDPOINT

            The URL to the Renku platform.

        .. envvar:: RENKU_ACCESS_TOKEN

            An access token obtained from Renku authentication service.

        Example:

            >>> import renku
            >>> client = renku.from_env()

        """
        if not environment:
            environment = os.environ

        endpoint = environment.get('RENKU_ENDPOINT', '')
        access_token = environment.get('RENKU_ACCESS_TOKEN')
        client = cls(endpoint=endpoint, token={'access_token': access_token})
        client._environment = environment

        # FIXME temporary solution until the execution id is moved to the token
        execution_id = environment.get('RENKU_VERTEX_ID')
        if execution_id:
            client.api.headers['Renku-Deployer-Execution'] = execution_id

        return client

    @property
    def current_context(self):
        """Return the current context as defined in the environment.

        .. envvar:: RENKU_CONTEXT_ID

            The context identifier used by the REST API.

        See the :doc:`context object documentation <context-object>` for
        full details.
        """
        env = getattr(self, '_environment', os.environ)
        return self.contexts[env['RENKU_CONTEXT_ID']]

    @property
    def contexts(self):
        """An object for managing contexts on the server.

        See the :doc:`contexts documentation <contexts>` for full details.
        """
        from .models.deployer import ContextCollection
        return ContextCollection(client=self)

    @property
    def projects(self):
        """An object for managing projects on the server.

        See the :doc:`projects documentation <projects>` for full details.
        """
        from .models.projects import ProjectCollection
        return ProjectCollection(client=self)

    @property
    def buckets(self):
        """An object for managing buckets on the server.

        See the :doc:`buckets documentation <buckets>` for full details.
        """
        from .models.storage import BucketCollection
        return BucketCollection(client=self)


from_env = RenkuClient.from_env
