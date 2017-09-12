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
"""HTTP client for Renga platform."""

import requests

from .authorization import AuthorizationMixin
from .deployer import ContextsApiMixin
from .explorer import ExplorerApiMixin
from .projects import ProjectsApiMixin
from .storage import BucketsApiMixin, FilesApiMixin


class APIClient(
        AuthorizationMixin,
        BucketsApiMixin,
        ContextsApiMixin,
        ExplorerApiMixin,
        FilesApiMixin,
        ProjectsApiMixin, ):
    """A low-level client for communicating with a Renga Platform API.

    Example:

        >>> import renga
        >>> client = renga.APIClient('http://localhost')

    """

    __attrs__ = requests.Session.__attrs__ + ['access_token', 'endpoint']

    def __init__(self, endpoint, access_token=None, **kwargs):
        """Create a storage client."""
        self.endpoint = endpoint

        super(APIClient, self).__init__(**kwargs)

        if access_token:
            # NOTE used by storage service
            self.access_token = access_token

    def _url(self, url, *args, **kwargs):
        """Format url for endpoint."""
        return (self.endpoint.rstrip('/') + '/' + url.format(
            *args, **kwargs).lstrip('/'))
