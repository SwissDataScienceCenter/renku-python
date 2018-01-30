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

import dateutil
import json
import os
import re
import shutil
import stat
import uuid
import warnings
from datetime import datetime
from dateutil.parser import parse as parse_date
from functools import partial
from urllib import error, parse, request

import attr
import git
import requests
from attr.validators import instance_of

try:
    from pathlib import Path
except ImportError:  # pragma: no cover
    from pathlib2 import Path

NoneType = type(None)

_path_attr = partial(
    attr.ib,
    converter=lambda x: Path(x) if isinstance(x, str) else x,
    validator=lambda i, arg, val: val.stat() if isinstance(val, Path) else True
)


def _deserialize_list(l, cls, options=None):
    """Deserialize a list of dicts into classes."""
    if not all(isinstance(x, dict) for x in l):
        raise ValueError('Dicts required for deserialization.')
    if options:
        return [cls(**options, **x) for x in l]
    return [cls(**x) for x in l]


@attr.s
class Creator(object):
    """Represent the creator."""

    name = attr.ib(validator=instance_of(str))
    email = attr.ib()
    affiliation = attr.ib(default=None)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if not (isinstance(value, str) or re.match(r"[^@]+@[^@]+\.[^@]+",
                                                   value)):
            raise ValueError('Email address is invalid.')


@attr.s
class DatasetFile(object):
    """Represent a file in a dataset."""

    path = _path_attr()
    origin = attr.ib(converter=lambda x: str(x))
    creator = attr.ib(
        converter=lambda arg: arg if isinstance(arg, Creator)
        else Creator(**arg))
    dataset = attr.ib(default=None)
    date_added = attr.ib(default=attr.Factory(datetime.now))

_deserialize_files = partial(_deserialize_list, cls=DatasetFile)


@attr.s
class Dataset(object):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https')

    name = attr.ib(type='string')
    created_at = attr.ib(
        default=attr.Factory(datetime.now),
        converter=lambda arg: arg if isinstance(arg, datetime)
        else parse_date(arg))
    identifier = attr.ib(
        default=attr.Factory(uuid.uuid4),
        converter=lambda x: uuid.UUID(x) if isinstance(x, str)
        else x)
    repo = attr.ib(
        default=None,
        converter=lambda arg: arg if isinstance(
            arg, (git.Repo, NoneType)) else git.Repo(arg)
    )
    creator = attr.ib(
        converter=lambda arg: arg if isinstance(arg, Creator)
        else Creator(**arg))
    datadir = _path_attr(default='data')
    files = attr.ib(default=attr.Factory(list), converter=_deserialize_files)

    @creator.default
    def set_creator_from_git(self):
        """Set the creator name and email from the git repo if present."""
        if not self.repo:
            raise RuntimeError('Outside of a git repository '
                               '- unable to determine creator information.')
        git_config = self.repo.config_reader()
        return {
            'name': git_config.get('user', 'name'),
            'email': git_config.get('user', 'email')
        }

    def __attrs_post_init__(self):
        """Finalize initialization of Dataset instance."""
        if self.repo:
            self.datadir = Path(os.path.dirname(self.repo.git_dir)).joinpath(
                'data').absolute()
        else:
            self.datadir = Path(self.datadir).absolute()

    @property
    def path(self):
        """Path to this Dataset."""
        return self.datadir.joinpath(self.name)

    def meta_init(self):
        """Initialize the directories and metadata."""
        try:
            os.mkdir(self.path)
        except FileExistsError:
            raise FileExistsError('This dataset already exists.')
        self.write_metadata()
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
        self.commit_to_repo()

    def _add_from_url(self, path, url, nocopy=False):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme))

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
        return DatasetFile(
            str(dst.absolute()), url, creator=self.creator, dataset=self.name)

    def write_metadata(self):
        """Write the dataset metadata to disk."""
        with open(self.path.joinpath('metadata.json'), 'w') as f:
            f.write(self.to_json())
        return self.to_json()

    def commit_to_repo(self, message=None):
        """Commit the dataset files to the git repository."""
        repo = self.repo
        if repo:
            repo.index.add(
                [Path(x.path).absolute().as_posix() for x in self.files])
            repo.index.add(
                [self.path.joinpath('metadata.json').absolute().as_posix()])
            if not message:
                message = "[renga] commiting changes to {} dataset".format(
                    self.name)
            repo.index.commit(message)

    def to_json(self):
        """Dump the json for this dataset."""
        d = attr.asdict(self)

        # convert unserializable values to str
        for k in ('created_at', 'identifier', 'datadir'):
            if d[k]:
                d[k] = str(d[k])

        # serialize file list
        files = []
        for f in d['files']:
            f['path'] = f['path'].absolute().as_posix()
            f['date_added'] = str(f['date_added'])
            files.append(f)

        # serialize repo path
        if d.get('repo'):
            d['repo'] = d['repo'].git_dir

        return json.dumps(d)

    def to_dict(self):
        """Return a dictionary serialization of the Dataset."""
        return attr.asdict(self)

    @staticmethod
    def from_json(metadata_file):
        """Return a Dataset object deserialized from json on disk."""
        with open(metadata_file) as f:
            return Dataset(**json.load(f))

    @staticmethod
    def create(*args, **kwargs):
        """Create a new dataset and create its directories and metadata."""
        d = Dataset(*args, **kwargs)
        d.meta_init()
        return d

    @staticmethod
    def load(name, repo=None, datadir='data'):
        """Return an existing dataset."""
        metadata_file = os.path.join(
            os.path.dirname(repo.git_dir)
            if repo else '.', datadir, name, 'metadata.json')
        return Dataset.from_json(metadata_file)
