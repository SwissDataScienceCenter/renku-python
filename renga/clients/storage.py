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

from renga.clients._datastructures import Endpoint, EndpointMixin

from ._datastructures import namedtuple

CreateBucket = namedtuple(
    'CreateBucket', ['name', 'backend', 'request_type'],
    default_values={'request_type': 'create_bucket'})
"""Storage create bucket request."""


class StorageClient(EndpointMixin):
    """Client for handling storage."""

    create_bucket_url = Endpoint('/api/storage/authorize/create_bucket')
    create_file_url = Endpoint('/api/storage/authorize/create_file')
    read_file_url = Endpoint('/api/storage/authorize/read_file')
    io_write_url = Endpoint('/api/storage/io/write')
    io_read_url = Endpoint('/api/storage/io/read')

    def create_bucket(self, bucket, access_token):
        """Create a bucket."""
        pass
