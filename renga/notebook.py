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


class RengaStorageManager(ContentsManager):  # pragma: no cover
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RengaStorageManager, self).__init__(*args, **kwargs)
        self._renga_client = renga.from_env()
        self.checkpoints_kwargs['root_dir'] = '.checkpoints'

    def _checkpoints_class_default(self):
        return GenericFileCheckpoints

    def _save_notebook(self, path, nb):
        """Save a notebook to the storage service."""
        _, file_ = self._path_to_model(path)

        with file_.open('w') as fp:
            fp.write(nbformat.writes(nb, version=nbformat.NO_CONVERT))

    def _bucket_to_model(self, bucket, prefix=''):
        self.log.debug("_bucket_to_model: %s: %s", bucket, bucket.name)
        model = {
            'name': bucket.name,
            'path': prefix + str(bucket.id),
            'last_modified': datetime.datetime.utcnow(
            ),  # key.last_modified,  will be used in an HTTP header
            'created': None,  # key.last_modified,
            'type': 'directory',
            'content': [],
            'mimetype': None,
            'writable': bool(bucket.id),
            'format': 'json',
        }
        return model

    def _file_to_model(self, file_, prefix=''):
        self.log.debug("_file_to_model: %s: %s", file_, file_.filename)
        model = {
            'content': None,
            'name': file_.filename,
            'path': prefix + str(file_.id),
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
        builder = collections.namedtuple('Path', 'id,name')
        file_builder = collections.namedtuple('File', 'id,filename')

        root = builder('', '')
        sections = {
            'buckets': builder('buckets', 'Buckets'),
            'current_context': builder('current_context', 'Current Context'),
        }

        if not path:
            model = self._bucket_to_model(root)
            model['content'] = [
                self._bucket_to_model(item) for item in sections.values()
            ]
            return model, root

        items = path.split('/')
        section, values = items[0], items[1:]

        if section == 'buckets':
            buckets = self._renga_client.buckets
            if not values:
                model = self._bucket_to_model(sections[section])
                model['content'] = [
                    self._bucket_to_model(bucket, prefix=section + '/')
                    for bucket in buckets
                ]
                return model, sections[section]
            elif len(values) == 1:
                bucket = buckets[int(values[0])]
                model = self._bucket_to_model(bucket, prefix=section + '/')
                model['content'] = [
                    self._file_to_model(
                        file_, prefix=section + '/' + values[0] + '/')
                    for file_ in bucket.files
                ]
                return model, bucket
            elif len(values) == 2:
                file_ = buckets[int(values[0])].files[int(values[1])]
                model = self._file_to_model(
                    file_, prefix=section + '/' + values[0] + '/')
                return model, file_

        elif section == 'current_context':
            context = self._renga_client.current_context
            context_sections = {
                'inputs': builder('{0}/inputs'.format(section), 'Inputs'),
                'outputs': builder('{0}/outputs'.format(section), 'Outputs'),
            }
            if not values:
                model = self._bucket_to_model(
                    sections[section], prefix=section + '/')
                model['content'] = [
                    self._bucket_to_model(context_section)
                    for context_section in context_sections.values()
                ]
                return model, sections[section]
            elif len(values) == 1:
                key = values[0]
                model = self._bucket_to_model(context_sections[key])
                file_objects = getattr(context, key)
                files = [file_objects[file_] for file_ in file_objects._names]

                def build_file(file_key):
                    """Build file object."""
                    file_ = file_objects[file_key]
                    return file_builder(file_key, '[{0}] {1}'.format(
                        file_key, file_.filename))

                model['content'] = [
                    self._file_to_model(
                        build_file(file_key), prefix=section + '/' + key + '/')
                    for file_key in file_objects._names
                ]
                return model, context_sections[key]
            elif len(values) == 2:
                key = values[0]
                file_ = getattr(context, key)[values[1]]
                model = self._file_to_model(
                    file_builder(values[1], file_.filename),
                    prefix=section + '/' + key + '/')
                return model, file_

        raise ValueError(path)

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
