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

import os
from datetime import datetime

import requests

from renku import errors

from ._datastructures import Collection, LazyResponse, Model


class Bucket(Model):
    """Represent a storage bucket."""

    IDENTIFIER_KEY = 'id'

    @property
    def backend(self):
        """The backend of this bucket."""
        return self._properties.get('resource:bucket_backend')

    @property
    def name(self):
        """The bucket name."""
        return self._properties.get('resource:bucket_name')

    @name.setter
    def name(self, value):
        """Modify the name."""
        # FIXME check when the API is fixed
        self._client.api.storage_bucket_metadata_replace(
            self.id, {
                'file_name': value,
            }
        )

        # Update if the service replace works
        self._properties['resource:bucket_name'] = value

    @property
    def _properties(self):
        """The internal bucket properties."""
        if self._response.get('properties') is None:
            self._response = self._client.api.get_bucket(self.id)
            assert self._response['properties']
        return self._response.get('properties')

    @property
    def files(self):
        """The :class:`~renku.models.storage.FileCollection` instance."""
        return FileCollection(self, client=self._client)


class BucketCollection(Collection):
    """Represent storage buckets on the server."""

    class Meta:
        """Information about individual buckets."""

        model = Bucket

        headers = ('id', 'name', 'backend')

    @property
    def backends(self):
        """Return a list of the enabled backends."""
        return self._client.api.storage_info()

    def create(self, name=None, backend=None, **kwargs):
        """Create a new :class:`~renku.models.storage.Bucket` instance.

        **Example**

        >>> bucket = client.buckets.create('bucket1')
        >>> bucket.name
        'bucket1'

        :param name: Bucket name.
        :param backend: Name of backend used to store data in the bucket.
                        Defaults to the :envvar:`RENKU_STORAGE_BUCKET_BACKEND`
                        environment variable if it is defined otherwise
                        uses the first item from
                        :meth:`renku.models.storage.BucketCollection.backends`
                        property.

        .. envvar:: RENKU_STORAGE_BUCKET_BACKEND

            The default backend for newly created buckets.
        """
        backend = backend or os.environ.get(
            'RENKU_STORAGE_BUCKET_BACKEND', self.backends[0]
        )
        data = self._client.api.create_bucket(name=name, backend=backend)
        return self.Meta.model(data, client=self._client, collection=self)

    def __getitem__(self, bucket_id):
        """Find a bucket by its ``id``."""
        return self.Meta.model(
            self._client.api.get_bucket(bucket_id),
            client=self._client,
            collection=self
        )

    def __iter__(self):
        """Iterate through all buckets as returned by the Explorer."""
        return (
            self.Meta.model(data, client=self._client, collection=self)
            for data in self._client.api.list_buckets()
        )


class FileMixin(object):
    """Generic file mixin."""

    @property
    def _properties(self):
        """The internal file properties."""
        return self._response['properties']

    def open(self, mode='r'):
        """Return the :class:`~renku.models.storage.FileHandle` instance."""
        file_handle = {
            'resource_id': self.id,
            'request_type': FileHandle.REQUEST_TYPE[mode],
        }
        token = self._client.api.storage_authorize(**file_handle)
        client = self._client.__class__(self._client.api.endpoint, token=token)
        if 'Renku-Deployer-Execution' in self._client.api.headers:
            client.api.headers[
                'Renku-Deployer-Execution'
            ] = self._client.api.headers['Renku-Deployer-Execution']

        return FileHandle(file_handle, client=client)


class File(Model, FileMixin):
    """Represent a file object."""

    IDENTIFIER_KEY = 'id'

    @property
    def access_token(self):
        """The access token for performing file operations."""
        # FIXME make sure that bucket endpoint returns name
        return self._response.get(
            'access_token', self._client.api.access_token
        )

    @property
    def name(self):
        """Name of the file."""
        return self._properties.get('resource:file_name')

    @name.setter
    def name(self, value):
        """Modify the name."""
        self._client.api.storage_file_metadata_replace(
            self.id, {
                'file_name': value,
            }
        )

        # Update if the service replace works
        self._properties['resource:file_name'] = value

    filename = name

    def clone(self, name=None, filename=None, bucket=None):
        """Create an instance of the file for independent version tracking."""
        resp = self._client.api.storage_copy_file(
            resource_id=self.id,
            file_name=name or filename or 'clone_' + self.name,
            bucket_id=bucket.id if isinstance(bucket, Bucket) else bucket,
        )
        return self.__class__(
            LazyResponse(lambda: self._client.api.get_file(resp['id']), resp),
            client=self._client,
            collection=self._collection
        )

    @property
    def versions(self):
        """An object for managing file versions.

        :return: The collection of file versions.
        :rtype: renku.models.storage.FileVersionCollection
        """
        return FileVersionCollection(self, client=self._client)


class FileCollection(Collection):
    """Represent files in a bucket on the server."""

    class Meta:
        """Information about individual files."""

        model = File

        headers = ('id', 'name')

    def __init__(self, bucket, **kwargs):
        """Initialize collection of files in the bucket."""
        self.bucket = bucket
        super(FileCollection, self).__init__(**kwargs)

    def __getitem__(self, file_id):
        """Return a file object."""
        return self.Meta.model(
            self._client.api.get_file(file_id),
            client=self._client,
            collection=self
        )

    def __iter__(self):
        """Return all files in this bucket."""
        return (
            self.Meta.model(f, client=self._client, collection=self)
            for f in self._client.api.get_bucket_files(self.bucket.id)
        )

    def open(self, name=None, filename=None, mode='w'):
        """Create an empty file in this bucket."""
        if mode != 'w':
            raise NotImplemented('Only mode "w" is currently supported')

        resp = self._client.api.create_file(
            bucket_id=self.bucket.id,
            file_name=name or filename,
            request_type='create_file',
        )

        access_token = resp.pop('access_token')
        client = self._client.__class__(
            self._client.api.endpoint, token={'access_token': access_token}
        )

        if 'Renku-Deployer-Execution' in self._client.api.headers:
            client.api.headers[
                'Renku-Deployer-Execution'
            ] = self._client.api.headers['Renku-Deployer-Execution']

        file_handle = {
            'resource_id': resp['id'],
            'request_type': FileHandle.REQUEST_TYPE[mode],
        }
        return FileHandle(file_handle, client=client)

    def create(self, name=None, filename=None):
        """Create an empty file in this bucket."""
        resp = self._client.api.create_file(
            bucket_id=self.bucket.id,
            file_name=name or filename,
            request_type='create_file',
        )
        return self.Meta.model(
            LazyResponse(lambda: self._client.api.get_file(resp['id']), resp),
            client=self._client,
            collection=self
        )

    def from_url(self, url, name=None, filename=None):
        """Create a file with data from the streamed GET response.

        **Example**

        >>> bucket = client.buckets.create('bucket1')
        >>> file_ = bucket.files.from_url(
        ...     'https://example.com/tests/data', name='hello')
        >>> file_.id
        9876
        >>> client.buckets[bucket.id].files[file_.id].open('r').read()
        b'hello world'

        """
        with self.open(name=name or filename or url, mode='w') as fp:
            fp.from_url(url)
        return self.__getitem__(fp.id)


class FileHandle(Model):
    """An object exposing a pythonic file-oriented API.

    Depending on the bucket to which it belongs to, a file object can mediate
    access to different storage backends (local, Swift, etc).
    """

    IDENTIFIER_KEY = 'resource_id'

    REQUEST_TYPE = {
        'r': 'read_file',
        'w': 'write_file',
    }

    def __enter__(self):
        """Simulate a context manager."""
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Clean up the context if necessary."""

    @property
    def can_write(self):
        """Check if the file handle is writable."""
        return self._response['request_type'] == self.REQUEST_TYPE['w']

    @property
    def can_read(self):
        """Check if the file handle is readable."""
        return self._response['request_type'] == self.REQUEST_TYPE['r']

    def write(self, data):
        """Write data to the file."""
        if not self.can_write:
            raise errors.InvalidFileOperation('File is not writable.')

        self._client.api.storage_io_write(data)

    def from_url(self, url):
        """Write data from the streamed GET response from the given URL.

        **Example**

        >>> file_ = client.buckets.create('bucket1').files.create('data')
        >>> with file_.open('w') as fp:
        ...     fp.from_url('https://example.com/tests/data')

        """
        with requests.get(url, stream=True) as r:
            self.write(r.iter_content())

    def read(self, *args, **kwargs):
        """Read data from the file."""
        if not self.can_read:
            raise errors.InvalidFileOperation('File is not writable.')

        return self._client.api.storage_io_read(*args, **kwargs)


class FileVersion(Model, FileMixin):
    """Represent a file version object."""

    IDENTIFIER_KEY = 'id'

    @property
    def name(self):
        """Name of the file."""
        return self._properties.get(
            'resource:file_name', self._collection.file.name
        )

    filename = name

    @property
    def created(self):
        """Return file creation date and time."""
        return datetime.utcfromtimestamp(
            int(self._properties.get('system:creation_time')) // 1000
        )  # thank you Java


class FileVersionCollection(Collection):
    """Represent file versions of a file stored on the server."""

    class Meta:
        """Information about individual file versions."""

        model = FileVersion

        headers = ('id', 'name', 'created')

    def __init__(self, file_, **kwargs):
        """Initialize a collection of file versions."""
        self.file = file_
        super(FileVersionCollection, self).__init__(**kwargs)

    def __getitem__(self, file_id):
        """Return a file object."""
        return self.Meta.model(
            self._client.api.get_file(file_id),
            client=self._client,
            collection=self
        )

    def __iter__(self):
        """Return all versions of this file."""
        return iter(
            sorted((
                self.Meta.model(data, client=self._client, collection=self)
                for data in self._client.api.get_file_versions(self.file.id)
            ),
                   key=lambda file_: file_.created,
                   reverse=True)
        )
