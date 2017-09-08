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
"""Model objects representing buckets and file objects."""

from ._datastructures import Collection, Model


class Bucket(Model):
    """Represent a storage bucket."""

    IDENTIFIER_KEY = 'id'

    def create_file(self, file_name=None):
        """Create an empty file in this bucket."""
        resp = self._client.api.create_file(
            bucket_id=self.id,
            file_name=file_name,
            request_type='create_file', )
        access_token = resp.pop('access_token')
        client = self._client.__class__(
            self._client.api.endpoint, access_token=access_token)
        return File({'id': resp['id']}, client=client, collection=self)


class BucketsCollection(Collection):
    """Represent storage buckets on the server."""

    class Meta:
        """Information about individual projects."""

        model = Bucket

    def create(self, name=None, backend=None, **kwargs):
        """Create new project."""
        data = self._client.api.create_bucket(name=name, backend=backend)
        return self.Meta.model(data, client=self._client, collection=self)


class File(Model):
    """Represent a file object."""

    IDENTIFIER_KEY = 'id'
