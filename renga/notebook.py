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
"""Custom hooks for Jupyter notebook."""

from __future__ import absolute_import

import codecs
import collections
import datetime
import os
import tempfile

import nbformat
from notebook import notebookapp  # needed for translation setup
from notebook.services.contents.filecheckpoints import GenericFileCheckpoints
from notebook.services.contents.largefilemanager import LargeFileManager
from notebook.services.contents.manager import ContentsManager
from tornado import web

import renga


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
            self._path + '/' + path, obj=obj, resolver=resolver)

    __truediv__ = __div__


def _buckets_resolver(obj, path):  # pragma: no cover
    """Resolve bucket paths."""
    bucket = obj[int(path)]
    return bucket, lambda obj, path: (obj.files[int(path)], None)


def _current_context_resolver(obj, path):  # pragma: no cover
    """Resolve current context paths."""
    def _section_resolver(obj, path):
        """Resolve inputs and outputs."""
        return obj[path], None

    sections = {'inputs': _section_resolver, 'outputs': _section_resolver}
    assert path in sections
    return getattr(obj, path), sections[path]


def _section_resolver(obj, path):  # pragma: no cover
    """Resolve top level paths."""
    sections = {
        'buckets': _buckets_resolver,
        'current_context': _current_context_resolver
    }
    assert path in sections
    return getattr(obj, path), sections[path]


class RengaStorageManager(ContentsManager):  # pragma: no cover
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RengaStorageManager, self).__init__(*args, **kwargs)
        self._renga_client = renga.from_env()
        self._path = Path('', self._renga_client, _section_resolver)
        self.checkpoints_kwargs['root_dir'] = '.checkpoints'

    def _checkpoints_class_default(self):
        return GenericFileCheckpoints

    def _save_notebook(self, path, nb):
        """Save a notebook to the storage service."""
        _, file_ = self._path_to_model(path)

        with file_.open('w') as fp:
            fp.write(nbformat.writes(nb, version=nbformat.NO_CONVERT))

    def _rengaclient_to_model(self, client, path=''):
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
            'name': 'Renga',
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

    def _context_to_model(self, context, path=''):
        content = [
            {
                'name': 'Inputs',
                'path': '{0}/inputs'.format(path),
                'type': 'directory'
            },
            {
                'name': 'Outputs',
                'path': '{0}/outputs'.format(path),
                'type': 'directory'
            },
        ]
        model = {
            'name': 'Current Context',
            'path': path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'content': content,
            'type': 'directory',
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _slotcollection_to_model(self, collection, path=''):
        name = path.split('/')[-1].capitalize()
        content = []
        for slot in collection._names.keys():
            if slot in collection:
                file_ = collection[slot]
                content.append(
                    self._file_to_model(
                        file_,
                        path=path + '/' + slot,
                        name='[{0}] {1}'.format(slot, file_.filename)))

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

    def _bucketcollection_to_model(self, collection, path=''):
        content = [
            self._bucket_to_model(
                bucket, path=path + '/' + str(bucket.id), content=False)
            for bucket in collection
        ]
        model = {
            'name': 'Buckets',
            'path': path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,  # key.last_modified,
            'content': content,
            'type': 'directory',
            'mimetype': None,
            'writable': False,
            'format': 'json',
        }
        return model

    def _bucket_to_model(self, bucket, path='', content=True):
        if content:
            content = [
                self._file_to_model(file_, path=path + '/' + str(file_.id))
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

    def _file_to_model(self, file_, path='', name=None):
        model = {
            'content': None,
            'name': name or file_.filename,
            'path': path,
            'last_modified': datetime.datetime.utcnow(),
            'created': None,
            'type': 'notebook'
            if file_.filename.endswith('.ipynb') else 'file',
            'mimetype': None,
            'writable': True,
            'format': None,
        }
        return model

    def _path_to_model(self, path):
        """Return a model based on the specified path."""
        path = path.strip('/')
        resource = self._path
        if path:
            for section in path.split('/'):
                resource = resource / section
        obj = resource._obj
        name = obj.__class__.__name__.lower()
        return (getattr(self, '_{0}_to_model'.format(name))(obj, path=path),
                obj)

    def is_hidden(self, path):
        """Return true if the path is hidden."""
        return False

    def dir_exists(self, path):
        """Check if the directory exists."""
        return True
        if path == '':
            return True
        self.log.debug('dir_exists: %s', locals())
        path = int(path)
        return path in self._renga_client.buckets

    def file_exists(self, path):
        """Check if the file exists."""
        return True

    def exists(self, path):
        """Check if the path exists."""
        return True

    def get(self, path, content=True, type=None, format=None):
        """Get buckets, files and their content."""
        self.log.debug('get: %s', locals())

        model, obj = self._path_to_model(path)
        if content and model['type'] == 'notebook':
            with tempfile.NamedTemporaryFile() as t:
                with obj.open('r') as f:
                    import json
                    data = json.dumps(json.loads(f.read().decode('utf-8')))
                    # read with utf-8 encoding
                    with codecs.open(t.name, mode='r', encoding='utf-8') as f:
                        nb = nbformat.reads(data, as_version=4)

            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)

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
            self.check_and_sign(nb, path)
            self._save_notebook(path, nb)
        elif model['type'] == 'file':
            self._save_file(path, model['content'], model.get('format'))
        elif model['type'] == 'directory':
            pass  # keep symmetry with filemanager.save
        else:
            raise web.HTTPError(400,
                                "Unhandled contents type: %s" % model['type'])

        validation_message = None
        if model['type'] == 'notebook':
            self.validate_notebook_model(model)
            validation_message = model.get('message', None)

        model = self.get(path, content=False, type=model['type'])
        if validation_message:
            model['message'] = validation_message

#        self.run_post_save_hook(model=model, os_path=path)

        model['content'] = None

        return model
