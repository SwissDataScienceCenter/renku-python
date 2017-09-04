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
"""Client for storage service."""

import time

import requests
from werkzeug.utils import cached_property

from renga.client._datastructures import AccessTokenMixin, Endpoint, \
    EndpointMixin

from ._datastructures import namedtuple

CreateBucket = namedtuple(
    'CreateBucket', ['name', 'backend', 'request_type'],
    default_values={'request_type': 'create_bucket'})
"""Storage create bucket request."""

CreateFile = namedtuple(
    'CreateFile', ['bucket_id', 'file_name', 'request_type'],
    default_values={'request_type': 'create_file'})
"""Storage create bucket request."""


class StorageClient(EndpointMixin, AccessTokenMixin):
    """Client for handling storage."""

    backends_url = Endpoint('/io/backends')
    create_bucket_url = Endpoint('/authorize/create_bucket')
    create_file_url = Endpoint('/authorize/create_file')
    read_file_url = Endpoint('/authorize/read_file')
    io_write_url = Endpoint('/io/write')
    io_read_url = Endpoint('/io/read')

    def __init__(self, endpoint, access_token):
        """Create a storage client."""
        EndpointMixin.__init__(self, endpoint)
        AccessTokenMixin.__init__(self, access_token)

    @cached_property
    def backends(self):
        """Return list of all available backends."""
        resp = requests.get(
            self.backends_url,
            headers=self.headers, )
        return resp.json()

    def create_bucket(self, bucket):
        """Create a bucket."""
        if bucket.backend not in self.backends:
            raise ValueError(
                'Unsupported bucket backend {0}'.format(bucket.backend))

        resp = requests.post(
            self.create_bucket_url,
            headers=self.headers,
            json=bucket._asdict()).json()
        return resp['id']

    def create_file(self, file_):
        """Create a file."""
        resp = requests.post(
            self.create_file_url, headers=self.headers,
            json=file_._asdict()).json()
        return resp
