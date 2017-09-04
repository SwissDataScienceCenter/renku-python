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
"""Client related datastructures."""

import collections


def namedtuple_with_defaults(typename, field_names, default_values=()):
    """Create namedtuple with defaults."""
    type_ = collections.namedtuple(typename, field_names)
    type_.__new__.__defaults__ = (None, ) * len(type_._fields)
    if isinstance(default_values, collections.Mapping):
        prototype = type_(**default_values)
    else:
        prototype = type_(*default_values)
    type_.__new__.__defaults__ = tuple(prototype)
    return type_


namedtuple = namedtuple_with_defaults


class AccessTokenMixin(object):
    """Store access token and provide _headers property."""

    def __init__(self, access_token):
        """Store access token."""
        self.access_token = access_token

    @property
    def headers(self):
        """Return default headers."""
        return {'Authorization': 'Bearer {0}'.format(self.access_token)}


class EndpointMixin(object):
    """Default API endpoint mixin."""

    def __init__(self, endpoint):
        """Store endpoint."""
        self.endpoint = endpoint


class Endpoint(object):
    """Define REST API URL properties based on endpoint.

    Example:

    .. code-block:python
        class API:
            api_url = Endpoint('/api')
    """

    def __init__(self, suffix=None):
        """Store URL formatting."""
        self.suffix = suffix or ''

    def __get__(self, obj, objtype=None):
        """Return formatted URL."""
        assert isinstance(obj, EndpointMixin) or hasattr(obj, 'endpoint')
        return '/'.join([obj.endpoint.rstrip('/'), self.suffix.lstrip('/')])
