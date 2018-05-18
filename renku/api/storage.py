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
"""Client for storage service."""


class BucketsApiMixin(object):
    """Client for handling storage buckets."""

    def storage_info(self):
        """Return information about available bucket backends."""
        resp = self.get(
            self._url('/api/storage/io/backends'),
            headers=self.headers,
        )
        return resp.json()

    def create_bucket(self, **kwargs):
        """Create a new storage bucket."""
        # TODO add validation
        kwargs['request_type'] = 'create_bucket'
        resp = self.post(
            self._url('/api/storage/authorize/create_bucket'), json=kwargs
        )
        return resp.json()

    def storage_bucket_metadata_replace(self, resource_id, data):
        """Replace resource metadata."""
        return self.put(
            self._url('/api/storage/bucket/{0}', resource_id),
            json=data,
            expected_status_code=200,
        ).json()


class FilesApiMixin(object):
    """Client for handling file objects in a bucket."""

    def create_file(self, **kwargs):
        """Create a new file object."""
        # TODO add validation
        kwargs['request_type'] = 'create_file'
        resp = self.post(
            self._url('/api/storage/authorize/create_file'), json=kwargs
        )
        return resp.json()

    def storage_authorize(self, resource_id=None, request_type=None):
        """Request authorization token for performing file handle request."""
        action = request_type.split('_')[0]
        resp = self.post(
            self._url('/api/storage/authorize/{0}', action),
            json={
                'resource_id': resource_id,
                'request_type': request_type
            }
        )
        return resp.json()

    def storage_copy_file(self, resource_id=None, file_name=None, **kwargs):
        """Request a file copy."""
        assert resource_id
        assert file_name
        kwargs['resource_id'] = resource_id
        kwargs['file_name'] = file_name
        kwargs['request_type'] = 'copy_file'
        resp = self.post(
            self._url('/api/storage/authorize/copy_file'),
            json={
                key: value
                for key, value in kwargs.items() if value is not None
            },
        )
        return resp.json()

    def storage_file_metadata_replace(self, resource_id, data):
        """Replace resource metadata."""
        return self.put(
            self._url('/api/storage/file/{0}', resource_id),
            json=data,
            expected_status_code=200,
        ).json()

    def storage_io_write(self, data):
        """Write data to the file.

        .. note:: Use only with ``access_token`` issued by storage service.
        """
        self.post(
            self._url('/api/storage/io/write'),
            data=data,
            headers={'Content-Type': 'application/octet-stream'}
        )

    def storage_io_read(self, *args, **kwargs):
        """Write data to the file.

        .. note:: Use only with ``access_token`` issued by storage service.
        """
        resp = self.get(self._url('/api/storage/io/read'), stream=True)
        return resp.raw.read(*args, **kwargs)
