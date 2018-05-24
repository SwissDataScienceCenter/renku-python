# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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
"""HTTP client for Renku platform."""

import functools
import os
import warnings

import attr
import requests

from renku import errors
from renku._compat import Path

from .authorization import AuthorizationMixin
from .datasets import DatasetsApiMixin
from .deployer import ContextsApiMixin
from .explorer import ExplorerApiMixin
from .projects import ProjectsApiMixin
from .repository import RepositoryApiMixin
from .storage import BucketsApiMixin, FilesApiMixin


def check_status_code(f):
    """Check status code of the response."""
    # ignore D202

    @functools.wraps(f)
    def decorator(*args, **kwargs):
        """Check for ``expected_status_code``."""
        expected_status_code = kwargs.pop(
            'expected_status_code', range(200, 300)
        )
        return errors.UnexpectedStatusCode.return_or_raise(
            f(*args, **kwargs), expected_status_code
        )

    return decorator


class APIClient(
    AuthorizationMixin,
    BucketsApiMixin,
    ContextsApiMixin,
    ExplorerApiMixin,
    FilesApiMixin,
    ProjectsApiMixin,
):
    """A low-level client for communicating with a Renku Platform API.

    Example:

        >>> import renku
        >>> client = renku.APIClient('http://localhost')

    """

    __attrs__ = requests.Session.__attrs__ + ['endpoint']

    def __init__(self, endpoint=None, **kwargs):
        """Create a remote API client."""
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
            warnings.warn('Using insecure transport protocol, use HTTPS')

    def _url(self, url, *args, **kwargs):
        """Format url for endpoint."""
        return (
            self.endpoint.rstrip('/') + '/' + url.format(*args, **
                                                         kwargs).lstrip('/')
        )

    @check_status_code
    def get(self, *args, **kwargs):
        """Perform the ``GET`` request and check its status code."""
        return super(APIClient, self).get(*args, **kwargs)

    @check_status_code
    def post(self, *args, **kwargs):
        """Perform the ``POST`` request and check its status code."""
        return super(APIClient, self).post(*args, **kwargs)

    @check_status_code
    def put(self, *args, **kwargs):
        """Perform the ``PUT`` request and check its status code."""
        return super(APIClient, self).put(*args, **kwargs)

    @check_status_code
    def delete(self, *args, **kwargs):
        """Perform the ``DELETE`` request and check its status code."""
        return super(APIClient, self).delete(*args, **kwargs)


@attr.s
class LocalClient(
    RepositoryApiMixin,
    DatasetsApiMixin,
):
    """A low-level client for communicating with a local Renku repository.

    Example:

        >>> import renku
        >>> client = renku.LocalClient('.')

    """

    path = attr.ib(converter=lambda arg: Path(arg).resolve().absolute())

    @path.default
    def _default_path(self):
        """Return default repository path."""
        from git import InvalidGitRepositoryError
        from renku.cli._git import get_git_home

        try:
            return get_git_home()
        except InvalidGitRepositoryError:
            return '.'

    @path.validator
    def _check_path(self, _, value):
        """Check the path exists and it is a directory."""
        if not (value.exists() and value.is_dir()):
            raise ValueError('Define an existing directory.')
