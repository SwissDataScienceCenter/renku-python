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

from contextlib import contextmanager
from datetime import datetime

import requests

from renga import errors

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

    @property
    def _properties(self):
        """The internal bucket properties."""
        return self._response.get('properties')

    @property
    def files(self):
        """The :class:`~renga.models.storage.FileCollection` instance."""
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

    def create(self, name=None, backend='local', **kwargs):
        """Create a new :class:`~renga.models.storage.Bucket` instance."""
        data = self._client.api.create_bucket(name=name, backend=backend)
        return self.Meta.model(data, client=self._client, collection=self)

    def __getitem__(self, bucket_id):
        """Find a bucket by its ``id``."""
        return self.Meta.model(
            self._client.api.get_bucket(bucket_id),
            client=self._client,
            collection=self)

    def __iter__(self):
        """Iterate through all buckets as returned by the Explorer."""
        return (self.Meta.model(data, client=self._client, collection=self)
                for data in self._client.api.list_buckets())


class FileMixin(object):
    """Generic file mixin."""

    @property
    def _properties(self):
        """The internal file properties."""
        return self._response['properties']

    def open(self, mode='r'):
        """Return the :class:`~renga.models.storage.FileHandle` instance."""
        file_handle = {
            'resource_id': self.id,
            'request_type': FileHandle.REQUEST_TYPE[mode],
        }
        token = self._client.api.storage_authorize(**file_handle)
        client = self._client.__class__(self._client.api.endpoint, token=token)
        if 'Renga-Deployer-Execution' in self._client.api.headers:
            client.api.headers[
                'Renga-Deployer-Execution'] = self._client.api.headers[
                    'Renga-Deployer-Execution']

        return FileHandle(file_handle, client=client)


class File(Model, FileMixin):
    """Represent a file object."""

    IDENTIFIER_KEY = 'id'

    @property
    def access_token(self):
        """The access token for performing file operations."""
        # FIXME make sure that bucket endpoint returns name
        return self._response.get('access_token',
                                  self._client.api.access_token)

    @property
    def filename(self):
        """Filename of the file."""
        return self._properties.get('resource:file_name')

    @filename.setter
    def filename(self, value):
        """Modify the filename value."""
        labels = self._properties.get('resource:labels', [])
        self._client.api.storage_file_metadata_replace(self.id, {
            'file_name': value,
            'labels': labels,
        })

        # Update if the service replace works
        self._properties['resource:file_name'] = value

    def clone(self, filename=None):
        """Create an instance of the file for independent version tracking."""
        if not hasattr(self._collection, 'create'):  # pragma: no cover
            raise NotImplemented(
                'Only files accessed via bucket can be cloned.')

        with self.open('r') as source:
            filename = filename or 'clone_' + self.filename
            cloned_file = self._collection.create(filename)
            with cloned_file.open('w') as dest:
                dest.write(source.read())
        return cloned_file

    @property
    def versions(self):
        """An object for managing file versions.

        :return: The collection of file versions.
        :rtype: renga.models.storage.FileVersionCollection
        """
        return FileVersionCollection(self, client=self._client)


class FileCollection(Collection):
    """Represent files in a bucket on the server."""

    class Meta:
        """Information about individual files."""

        model = File

        headers = ('id', 'filename')

    def __init__(self, bucket, **kwargs):
        """Initialize collection of files in the bucket."""
        self.bucket = bucket
        super(FileCollection, self).__init__(**kwargs)

    def __getitem__(self, file_id):
        """Return a file object."""
        return self.Meta.model(
            self._client.api.get_file(file_id),
            client=self._client,
            collection=self)

    def __iter__(self):
        """Return all files in this bucket."""
        return (self.Meta.model(f, client=self._client, collection=self)
                for f in self._client.api.get_bucket_files(self.bucket.id))

    def open(self, filename=None, mode='w'):
        """Create an empty file in this bucket."""
        if mode != 'w':
            raise NotImplemented('Only mode "w" is currently supported')

        resp = self._client.api.create_file(
            bucket_id=self.bucket.id,
            file_name=filename,
            request_type='create_file', )

        access_token = resp.pop('access_token')
        client = self._client.__class__(
            self._client.api.endpoint, token={'access_token': access_token})

        if 'Renga-Deployer-Execution' in self._client.api.headers:
            client.api.headers[
                'Renga-Deployer-Execution'] = self._client.api.headers[
                    'Renga-Deployer-Execution']

        file_handle = {
            'resource_id': resp['id'],
            'request_type': FileHandle.REQUEST_TYPE[mode],
        }
        return FileHandle(file_handle, client=client)

    def create(self, filename=None):
        """Create an empty file in this bucket."""
        resp = self._client.api.create_file(
            bucket_id=self.bucket.id,
            file_name=filename,
            request_type='create_file', )
        return self.Meta.model(
            LazyResponse(lambda: self._client.api.get_file(resp['id']), resp),
            client=self._client,
            collection=self)

    def from_url(self, url, filename=None):
        """Create a file with data from the streamed GET response.

        **Example**

        >>> file_ = client.buckets[1234].files.from_url(
        ...     'https://example.com/tests/data', filename='hello')
        >>> file_.id
        9876
        >>> client.buckets[1234].files[9876].open('r').read()
        b'hello world'

        """
        with self.open(filename=filename or url, mode='w') as fp:
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
            raise error.InvalidFileOperation('File is not writable.')

        self._client.api.storage_io_write(data)

    def from_url(self, url):
        """Write data from the streamed GET response from the given URL.

        **Example**

        >>> with client.buckets[1234].files[9876].open('w') as fp:
        ...     fp.from_url('https://example.com/tests/data')

        """
        with requests.get(url, stream=True) as r:
            self.write(r.iter_content())

    def read(self, *args, **kwargs):
        """Read data from the file."""
        if not self.can_read:
            raise error.InvalidFileOperation('File is not writable.')

        return self._client.api.storage_io_read(*args, **kwargs)


class FileVersion(Model, FileMixin):
    """Represent a file version object."""

    IDENTIFIER_KEY = 'id'

    @property
    def filename(self):
        """Filename of the file."""
        return self._properties.get('resource:file_name',
                                    self._collection.file.filename)

    @property
    def created(self):
        """Return file creation date and time."""
        return datetime.utcfromtimestamp(
            int(self._properties.get('system:creation_time')) //
            1000)  # thank you Java


class FileVersionCollection(Collection):
    """Represent file versions of a file stored on the server."""

    class Meta:
        """Information about individual file versions."""

        model = FileVersion

        headers = ('id', 'filename', 'created')

    def __init__(self, file_, **kwargs):
        """Initialize a collection of file versions."""
        self.file = file_
        super(FileVersionCollection, self).__init__(**kwargs)

    def __getitem__(self, file_id):
        """Return a file object."""
        return self.Meta.model(
            self._client.api.get_file(file_id),
            client=self._client,
            collection=self)

    def __iter__(self):
        """Return all versions of this file."""
        return iter(
            sorted(
                (self.Meta.model(data, client=self._client, collection=self)
                 for data in self._client.api.get_file_versions(self.file.id)),
                key=lambda file_: file_.created,
                reverse=True))
