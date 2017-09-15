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

import functools
import os
import warnings

import requests

from renga import errors

from .authorization import AuthorizationMixin
from .deployer import ContextsApiMixin
from .explorer import ExplorerApiMixin
from .projects import ProjectsApiMixin
from .storage import BucketsApiMixin, FilesApiMixin


def check_status_code(f):
    """Check status code of the response."""
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        """Check for ``expected_status_code``."""
        expected_status_code = kwargs.pop('expected_status_code',
                                          range(200, 300))
        return errors.UnexpectedStatusCode.return_or_raise(
            f(*args, **kwargs), expected_status_code)

    return decorator


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

    def __init__(self, endpoint=None, **kwargs):
        """Create a storage client."""
        self.endpoint = endpoint
        super(APIClient, self).__init__(**kwargs)

    @property
    def endpoint(self):
        """Return endpoint value."""
        return getattr(self, '_endpoint', None)

    @endpoint.setter
    def endpoint(self, endpoint):
        """Set and validate endpoint."""
        self._endpoint = endpoint

        if endpoint is not None and endpoint.startswith('http:'):
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'FIXME'
            warnings.warn('Using insecure trasnport protocol, use HTTPS')

    def _url(self, url, *args, **kwargs):
        """Format url for endpoint."""
        return (self.endpoint.rstrip('/') + '/' + url.format(
            *args, **kwargs).lstrip('/'))

    @check_status_code
    def get(self, *args, **kwargs):
        """Perform the ``GET`` request and check its status code."""
        return super().get(*args, **kwargs)

    @check_status_code
    def post(self, *args, **kwargs):
        """Perform the ``GET`` request and check its status code."""
        return super().post(*args, **kwargs)

    @check_status_code
    def delete(self, *args, **kwargs):
        """Perform the ``GET`` request and check its status code."""
        return super().delete(*args, **kwargs)
