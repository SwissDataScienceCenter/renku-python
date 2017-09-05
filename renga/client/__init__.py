# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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
"""Renga platform python clients."""

import os

import requests
from werkzeug.utils import cached_property

from renga.client._datastructures import AccessTokenMixin, EndpointMixin


class RengaClient(EndpointMixin, AccessTokenMixin):
    """A client for communicating with a Renga platform.

    Example:

        >>> import renga
        >>> client = renga.RengaClient('http://localhost', '** TOKEN **')

    """

    def __init__(self, endpoint, access_token=None):
        """Create a storage client."""
        EndpointMixin.__init__(self, endpoint)
        AccessTokenMixin.__init__(self, access_token)

    @classmethod
    def from_env(cls, environment=None):
        """Return a client configured from environment variables.

        .. envvar:: RENGA_ENDPOINT

            The URL to the Renga platform.

        .. envvar:: RENGA_ACCESS_TOKEN

            An access token obtained from Renga authentication service.

        Example:

            >>> import renga
            >>> client = renga.from_env()

        """
        if not environment:
            environment = os.environ

        endpoint = environment.get('RENGA_ENDPOINT')
        access_token = environment.get('RENGA_ACCESS_TOKEN')
        return cls(endpoint=endpoint, access_token=access_token)

    @cached_property
    def deployer(self):
        """Return a deployer client."""
        from .deployer import DeployerClient
        return DeployerClient(self.endpoint + '/api/deployer',
                              self.access_token)

    @cached_property
    def projects(self):
        """Return a deployer client."""
        from .projects import ProjectsClient
        return ProjectsClient(self.endpoint + '/api/projects',
                              self.access_token)

    @cached_property
    def storage(self):
        """Return a deployer client."""
        from .storage import StorageClient
        return StorageClient(self.endpoint + '/api/storage', self.access_token)

    def swagger(self):
        """Return Swagger definition for all services."""
        specs = (requests.get(service.endpoint + '/swagger.json').json()
                 for service in (self.deployer, self.storage))
        spec = merge(*specs)
        # TODO add title and other spec details
        return spec


from_env = RengaClient.from_env
