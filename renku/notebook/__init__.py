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
"""Custom hooks for Jupyter notebook."""

from __future__ import absolute_import

import datetime
import os
from binascii import hexlify

import nbformat
from notebook import notebookapp  # noqa: F401
from notebook.services.contents.filecheckpoints import GenericFileCheckpoints
from notebook.services.contents.manager import ContentsManager
from tornado import web

import renku


class Path(object):
    """Simple abstraction for path traversal."""

    def __init__(self, path, obj=None, resolver=None):
        """Store references to current path, object and getter."""
        self._path = path
        self._obj = obj
        self._resolver = resolver

    def __div__(self, path):
        """Join path components."""
        obj, resolver = self._resolver(self._obj, path)
        return self.__class__(
            self._path + '/' + path, obj=obj, resolver=resolver
        )

    __truediv__ = __div__

    def to_model(self, **kwargs):
        """Serialize path object to the notebook model."""
        name = self._obj.__class__.__name__.lower()
        return getattr(self, '_{0}_to_model'.format(name))(**kwargs)

    def _renkuclient_to_model(self):
        content = [
            {
                'name': 'Buckets',
                'path': 'buckets',
                'type': 'directory'
            },
            {
                'name': 'Current Context',
                'path': 'current_context',
                'type': 'directory'
            },
        ]
        model = {
            'name': 'Renku',
            'path': '',
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'content': content,
            'type': 'directory',
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _context_to_model(self):
        content = [
            {
                'name': 'Inputs',
                'path': '{0}/inputs'.format(self._path),
                'type': 'directory'
            },
            {
                'name': 'Outputs',
                'path': '{0}/outputs'.format(self._path),
                'type': 'directory'
            },
        ]
        model = {
            'name': 'Current Context',
            'path': self._path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'content': content,
            'type': 'directory',
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _slotcollection_to_model(self):
        collection = self._obj
        path = self._path
        name = path.split('/')[-1].capitalize()
        content = []
        for slot in collection._names.keys():
            if slot in collection:
                file_ = collection[slot]
                content.append(
                    Path(path + '/' + slot, obj=file_)._file_to_model(
                        name='[{0}] {1}'.format(slot, file_.filename)
                    )
                )

        model = {
            'name': name,
            'path': path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'type': 'directory',
            'content': content,
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _bucketcollection_to_model(self):
        content = [
            Path(self._path + '/' + str(bucket.id),
                 obj=bucket)._bucket_to_model(content=False)
            for bucket in self._obj
        ]
        model = {
            'name': 'Buckets',
            'path': self._path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'content': content,
            'type': 'directory',
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _bucket_to_model(self, content=True):
        bucket = self._obj
        path = self._path
        if content:
            content = [
                Path(path + '/' + str(file_.id), obj=file_)._file_to_model()
                for file_ in bucket.files
            ]
        else:
            content = []
        model = {
            'name': bucket.name,
            'path': path,
            'last_modified': datetime.datetime.utcnow(
            ),  # key.last_modified,  will be used in an HTTP header
            'created': None,  # key.last_modified,
            'type': 'directory',
            'content': content,
            'mimetype': None,
            'writable': bool(bucket.id),
            'format': 'json',
        }
        return model

    def _file_to_model(self, name=None):
        type_ = 'notebook' if self._obj.filename.endswith('.ipynb') else 'file'
        # FIME use file metadata when available
        versions = self._obj.versions.list()
        model = {
            'content': None,
            'name': name or self._obj.filename,
            'path': self._path,
            'last_modified': versions[0].created if versions else None,
            'created': versions[-1].created if versions else None,
            'type': type_,
            'mimetype': None,
            'writable': True,
            'format': None,
        }
        return model


def _buckets_resolver(obj, path):
    """Resolve bucket paths."""
    bucket = obj[int(path)]
    return bucket, lambda obj, path: (obj.files[int(path)], None)


def _default_section_resolver(obj, path):
    """Resolve inputs and outputs."""
    return obj[path], None


def _current_context_resolver(obj, path):
    """Resolve current context paths."""
    sections = {
        'inputs': _default_section_resolver,
        'outputs': _default_section_resolver
    }
    return getattr(obj, path), sections[path]


def _section_resolver(obj, path):
    """Resolve top level paths."""
    sections = {
        'buckets': _buckets_resolver,
        'current_context': _current_context_resolver
    }
    return getattr(obj, path), sections[path]


class RenkuStorageManager(ContentsManager):
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RenkuStorageManager, self).__init__(*args, **kwargs)
        self._renku_client = renku.from_env()
        self._path = Path('', self._renku_client, _section_resolver)
        self.checkpoints_kwargs['root_dir'] = '.checkpoints'

    def _checkpoints_class_default(self):  # pragma: no cover
        return GenericFileCheckpoints

    def _save_notebook(self, path, nb):
        """Save a notebook to the storage service."""
        file_ = self._resolve_path(path)._obj

        with file_.open('w') as fp:
            fp.write(
                nbformat.writes(nb,
                                version=nbformat.NO_CONVERT).encode('utf-8')
            )

    def _save_file(self, path, content, format=None):
        """Save a file to the storage service."""
        if format != 'text':
            raise TypeError('Can not save format: {}'.format(format))

        file_ = self._resolve_path(path)._obj

        with file_.open('w') as fp:
            if isinstance(content, bytes):
                fp.write(content)
            else:
                fp.write(content.encode('utf-8'))

    def _resolve_path(self, path):
        """Return a resource based on the specified path."""
        path = path.strip('/')
        resource = self._path
        if path:
            for section in path.split('/'):
                resource = resource / section
        return resource

    def is_hidden(self, path):  # pragma: no cover
        """Return true if the path is hidden."""
        return False

    def dir_exists(self, path):
        """Check if the directory exists."""
        try:
            self._resolve_path(path)
            return True
        except (AttributeError, KeyError, ValueError):
            return False

    def file_exists(self, path):
        """Check if the file exists."""
        try:
            resource = self._resolve_path(path)
            return resource._resolver is None
        except (AttributeError, KeyError, ValueError):
            return False

    def exists(self, path):  # pragma: no cover
        """Check if the path exists."""
        try:
            self._resolve_path(path)
            return True
        except (AttributeError, KeyError, ValueError):
            return False

    def get(self, path, content=True, type=None, format=None):
        """Get buckets, files and their content."""
        resource = self._resolve_path(path)
        model = resource.to_model()
        if content and model['type'] == 'notebook':
            with resource._obj.open('r') as f:
                nb = nbformat.reads(f.read().decode('utf-8'), as_version=4)

            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)
        elif content and model['type'] == 'file':
            with resource._obj.open('r') as f:
                model['content'] = f.read().decode('utf-8')
            model['format'] = 'text'
        elif not content and model['type'] == 'directory':
            model['format'] = format

        return model

    def save(self, model, path):
        """Save a model in the given path."""
        self.log.debug('save: %s', locals())

        if 'type' not in model:
            raise web.HTTPError(400, u'No file type provided')
        if 'content' not in model and model['type'] != 'directory':
            raise web.HTTPError(400, u'No file content provided')

#        self.run_pre_save_hook(model=model, path=path)

        if model['type'] == 'notebook':
            nb = nbformat.from_dict(model['content'])

            if not self.file_exists(path):
                items = path.strip('/').split('/')
                resource = self._resolve_path('/'.join(items[:-1]))

                if not hasattr(resource._obj, 'files'):
                    raise web.HTTPError(
                        403, "Notebook can only be created in a bucket."
                    )

                new_file = resource._obj.files.create(items[-1])
                path = (resource / str(new_file.id))._path

            self.check_and_sign(nb, path)
            self._save_notebook(path, nb)

        elif model['type'] == 'file':
            if not self.file_exists(path):
                items = path.strip('/').split('/')
                resource = self._resolve_path('/'.join(items[:-1]))

                if not hasattr(resource._obj, 'files'):
                    raise web.HTTPError(
                        403, "File can only be created in a bucket."
                    )

                new_file = resource._obj.files.create(items[-1])
                path = (resource / str(new_file.id))._path

            self._save_file(path, model['content'], model.get('format'))
        elif model['type'] == 'directory':
            if not self.dir_exists(path):
                items = path.strip('/').split('/')
                resource = self._resolve_path('/'.join(items[:-1]))

                if not hasattr(resource._obj, 'create'):
                    raise web.HTTPError(
                        403, "Buckets can only be created from buckets view."
                    )

                new_directory = resource._obj.create(items[-1])
                path = (resource / str(new_directory.id))._path
            else:
                raise NotImplemented('Can not rename buckets.')

            model['format'] = None
        else:
            raise web.HTTPError(
                400, "Unhandled contents type: %s" % model['type']
            )

        validation_message = None
        if model['type'] == 'notebook':  # pragma: no cover
            self.validate_notebook_model(model)
            validation_message = model.get('message', None)

        model = self.get(path, content=False, type=model['type'])
        if validation_message:  # pragma: no cover
            model['message'] = validation_message


#        self.run_post_save_hook(model=model, os_path=path)

        model['content'] = None

        return model

    def rename_file(self, old_path, path):
        """Rename object from old_path with suffix of path."""
        old_sections = old_path.split('/')
        sections = path.split('/')

        if old_sections[:-1] != sections[:-1]:
            raise RuntimeError('Can not move file between buckets')

        resource = self._resolve_path(old_path)
        model = resource.to_model()

        if model['type'] in {'notebook', 'file'}:
            resource._obj.filename = sections[-1]

        return resource.to_model()

    def update(self, model, path):
        """Update the file's path."""
        path = path.strip('/')
        new_path = model.get('path', path).strip('/')
        if path != new_path:
            # FIXME model = self.rename(path, new_path)
            model = self.rename_file(path, new_path)
        return model


def generate_notebook_token(size=24):
    """Generate a notebook access token."""
    return hexlify(os.urandom(24)).decode('ascii')


def generate_launch_args(token=None, ip='*', base_url=None):
    """Generate notebook launch arguments."""
    token = token or generate_notebook_token()

    args = [
        'start-notebook.sh',
        '--ContentsManager.untitled_notebook=notebook',
        '--NotebookApp.ip="{0}"'.format(ip),
        '--NotebookApp.token={0}'.format(token),
    ]

    if base_url:
        args.append('--NotebookApp.base_url={0}'.format(base_url))

    return args
