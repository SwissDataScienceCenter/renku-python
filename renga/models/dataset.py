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
"""Model objects representing datasets."""

import json
import os
import shutil
import stat
import uuid
import warnings
from datetime import datetime
from urllib import error, parse, request

import requests
from marshmallow import Schema, fields, post_load, pre_dump

try:
    from pathlib import Path
except ImportError:  # pragma: no cover
    from pathlib2 import Path


class DatasetSchema(Schema):
    """Schema for the Dataset objects."""

    name = fields.String()
    identifier = fields.UUID(default=str(uuid.uuid4()))
    files = fields.List(fields.String)
    date_created = fields.DateTime()

    @post_load
    def make_dataset(self, data):
        """Return a dataset instance."""
        return Dataset(loading=True, **data)


class Dataset(object):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https')

    def __init__(self, name, datadir=None, loading=False, repo=None, **kwargs):
        """Create a Dataset instance."""
        self.name = name
        self.identifier = uuid.uuid4()

        if datadir:
            self.datadir = Path(datadir).absolute()
        elif repo:
            self.datadir = Path(
                os.path.dirname(repo.git_dir)).joinpath('data').absolute()
        else:
            self.datadir = Path('./data').absolute()

        self.path = self.datadir.joinpath(name)
        self.repo = repo

        self.files = []

        if not loading:
            try:
                os.makedirs(self.path)
            except FileExistsError:
                raise FileExistsError('This dataset already exists.')

            self.files = []

            self.date_created = datetime.now()
            self.write_metadata()

            if repo:
                self.commit_to_repo()

    def add_data(self, url, datadir=None, git=False, targets=None, **kwargs):
        """Import the data into the data directory."""
        datadir = datadir or self.datadir
        git = os.path.splitext(url)[1] == '.git' or git

        if not isinstance(url, list):
            url = [url]

        if git:
            new_files = (self._add_from_git(self.path, u, targets, **kwargs)
                         for u in url)
        else:
            new_files = (self._add_from_url(self.path, u, **kwargs)
                         for u in url)

        self.files.extend(new_files)
        self.write_metadata()

    def write_metadata(self):
        """Write the dataset metadata to disk."""
        with open(self.path.joinpath('metadata.json'), 'w') as f:
            f.write(self.json)
        return self.json

    @property
    def json(self):
        """Dump the json for this dataset."""
        return DatasetSchema().dumps(self).data

    def to_dict(self):
        """Return a dictionary with the metadata for this dataset."""
        return DatasetSchema().dump(self).data

    def _add_from_url(self, path, url, nocopy=False):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError('{} URLs are not supported'.format(
                u.scheme))

        dst = path.joinpath(os.path.basename(url)).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()
            try:
                os.stat(src)
            except FileNotFoundError:
                raise FileNotFoundError
            if nocopy:
                try:
                    raise
                    os.link(src, dst)
                except:
                    warnings.warn("[renga] Could not create hard link - "
                                  "symlinking instead.")
                    os.symlink(src, dst)
            else:
                shutil.copy(src, dst)
        elif u.scheme in ('http', 'https'):
            try:
                response = requests.get(url)
                dst.write_bytes(response.content)
            except error.HTTPError as e:  # pragma nocover
                raise e

        # make the added file read-only
        mode = dst.stat().st_mode & 0o777
        dst.chmod(mode & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
        return dst.absolute()

    @staticmethod
    def from_json(metadata_file):
        """Return a Dataset object deserialized from json on disk."""
        with open(metadata_file) as f:
            return DatasetSchema().load(json.load(f)).data

    @staticmethod
    def load(name, repo=None, datadir='data'):
        """Return an existing dataset."""
        metadata_file = os.path.join(
            os.path.dirname(repo.git_dir)
            if repo else '.', datadir, name, 'metadata.json')
        return Dataset.from_json(metadata_file)

    def commit_to_repo(self, message=None):
        """Commit the dataset files to the git repository."""
        repo = self.repo

        repo.index.add([
            x.as_posix()
            for x in self.files + [self.path.joinpath('metadata.json')]
        ])

        if not message:
            message = "[renga] commiting changes to {} dataset".format(
                self.name)
        repo.index.commit(message)
