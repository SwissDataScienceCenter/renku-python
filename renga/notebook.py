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
import datetime
import tempfile
import os

import nbformat
from notebook import notebookapp  # needed for translation setup
from notebook.services.contents.largefilemanager import LargeFileManager
from notebook.services.contents.manager import ContentsManager
from tornado import web

import renga


class RengaFileManager(LargeFileManager):
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RengaFileManager, self).__init__(*args, **kwargs)
        self._renga_client = renga.from_env()

    def _save_notebook(self, os_path, nb):
        """Save a notebook to the storage service."""
        result = super(RengaFileManager, self)._save_notebook(os_path, nb)

        try:
            with self._renga_client.current_context.inputs['notebook'].open(
                    'w') as fp:
                fp.write(nbformat.writes(nb, version=nbformat.NO_CONVERT))
        except KeyError:  # pragma: no cover
            self.log.info('Notebook "{0}" is not tracked.'.format(os_path))

        return result


class RengaStorageManager(LargeFileManager):
    """Upload a notebook changes to the storage service."""

    def __init__(self, *args, **kwargs):
        """Store API client."""
        super(RengaStorageManager, self).__init__(*args, **kwargs)
        self._renga_client = renga.from_env()

    def _save_notebook(self, path, nb):
        """Save a notebook to the storage service."""
        print(path)
        bucket_id, file_id = (int(i) for i in path.strip('/').split('/')[-2:])

        bucket = self._renga_client.buckets[bucket_id]
        file_ = bucket.files[file_id]

        with file_.open('w') as fp:
            fp.write(nbformat.writes(nb, version=nbformat.NO_CONVERT))

    def _bucket_to_model(self, bucket):
        self.log.debug("_bucket_to_model: %s: %s", bucket, bucket.name)
        model = {
            'name': bucket.name,
            'path': str(bucket.id),
            'last_modified': datetime.datetime.utcnow(), # key.last_modified,  will be used in an HTTP header
            'created': None, # key.last_modified,
            'type': 'directory',
            'content': [],
            'mimetype': None,
            'writable': bool(bucket.id),
            'format': None,
        }
        return model

    def _file_to_model(self, file_, bucket):
        self.log.debug("_file_to_model: %s: %s", file_, file_.filename)
        model = {
            'content': None,
            'name': file_.filename,
            'path': str(bucket.id) + '/' + str(file_.id),
            'last_modified': datetime.datetime.utcnow(),
            'created': None,
            'type': 'notebook' if file_.filename.endswith('.ipynb') else 'file',
            'mimetype': None,
            'writable': True,
            'format': None,
        }
        return model

    def is_hidden(self, path):
        return False

    def dir_exists(self, path):
        return True
        if path == '':
            return True
        self.log.debug('dir_exists: %s', locals())
        path = int(path)
        return path in self._renga_client.buckets

    def file_exists(self, path):
        return True

    def exists(self, path):
        return True

    def get(self, path, content=True, type=None, format=None):
        """Get buckets, files and their content."""
        self.log.debug('get: %s', locals())

        if type == 'directory':
            if path == '':
                class Root:
                    id = ''
                    name = ''

                model = self._bucket_to_model(Root)
                if content:
                    model['content'] = [self._bucket_to_model(b) for b in self._renga_client.buckets]
                    model['format'] = 'json'
                return model
            else:
                path = int(path.strip('/'))
                bucket = self._renga_client.buckets[path]
                model = self._bucket_to_model(bucket)
                model['content'] = [self._file_to_model(f, bucket) for f in bucket.files]
                model['format'] = 'json'
                return model

        elif True:
        # elif type == 'notebook' or (type is None and path.endswith('.ipynb')):
            bucket_id, file_id = (int(i) for i in path.strip('/').split('/'))

            bucket = self._renga_client.buckets[bucket_id]
            file_ = bucket.files[file_id]
            model = self._file_to_model(file_, bucket)
            if content:
                with tempfile.NamedTemporaryFile() as t:
                    with file_.open('r') as f:
                        import json
                        data = json.dumps(json.loads(f.read()))
                        # read with utf-8 encoding
                        with codecs.open(t.name, mode='r', encoding='utf-8') as f:
                            nb = nbformat.reads(data, as_version=4)

                self.mark_trusted_cells(nb, path)
                model['content'] = nb
                model['format'] = 'json'
                self.validate_notebook_model(model)
            return model
        else: # assume that it is file
            key = self._path_to_s3_key(path)
            k = self.bucket.get_key(key)

            model = self._s3_key_file_to_model(k, timeformat=S3_TIMEFORMAT_GET_KEY)

            if content:
                try:
                    model['content'] = k.get_contents_as_string()
                except Exception as e:
                    raise web.HTTPError(400, u"Unreadable file: %s %s" % (path, e))

                model['mimetype'] = 'text/plain'
                model['format'] = 'text'

            return model
