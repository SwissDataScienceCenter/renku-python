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
import re
import shutil
import stat
import uuid
import warnings
from datetime import datetime
from functools import partial
from urllib import error, parse, request

import requests

import attr
import dateutil
import git
from attr.validators import instance_of
from dateutil.parser import parse as parse_date

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


def _deserialize_set(s, cls, options=None):
    """Deserialize a list of dicts into classes."""
    if not all(isinstance(x, dict) for x in s):
        raise ValueError('Dicts required for deserialization.')
    if options:
        return (cls(**options, **x) for x in s)
    return set(cls(**x) for x in s)


def _deserialize_dict(d, cls, options=None):
    """Deserialize a list of dicts into classes."""
    if not all(isinstance(x, dict) for x in d.values()):
        raise ValueError('Dicts required for deserialization.')
    if options:
        return {k: cls(**options, **v) for (k, v) in d.items()}
    return {k: cls(**v) for (k, v) in d.items()}


@attr.s(frozen=True)
class Author(object):
    """Represent the author of a resource."""

    name = attr.ib(validator=instance_of(str))
    email = attr.ib()
    affiliation = attr.ib(default=None)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if not (isinstance(value, str) or re.match(
                r"[^@]+@[^@]+\.[^@]+", value)):
            raise ValueError('Email address is invalid.')


def _deserialize_authors(authors):
    """Deserialize authors in various forms."""
    if isinstance(authors, dict):
        return set([Author(**authors)])
    elif isinstance(authors, Author):
        return set([authors])
    elif isinstance(authors, (set, list)):
        if all(isinstance(x, dict) for x in authors):
            return _deserialize_set(authors, Author)
        elif all(isinstance(x, Author) for x in authors):
            return authors

    raise ValueError('Authors must be a set of dicts or Author.')


@attr.s
class DatasetFile(object):
    """Represent a file in a dataset."""

    path = _path_attr()
    origin = attr.ib(converter=lambda x: str(x))
    authors = attr.ib(
        default=attr.Factory(set),
        converter=_deserialize_authors)
    dataset = attr.ib(default=None)
    date_added = attr.ib(default=attr.Factory(datetime.now))


_deserialize_files = partial(_deserialize_dict, cls=DatasetFile)


@attr.s
class Dataset(object):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https')

    name = attr.ib(type='string')
    created_at = attr.ib(
        default=attr.Factory(datetime.now),
        converter=lambda arg: arg if isinstance(
            arg, datetime) else parse_date(arg))
    identifier = attr.ib(
        default=attr.Factory(uuid.uuid4),
        converter=lambda x: uuid.UUID(x) if isinstance(x, str) else x)
    repo = attr.ib(
        default=None,
        converter=lambda arg: arg if isinstance(
            arg, (git.Repo, NoneType)) else git.Repo(arg)
    )
    authors = authors = attr.ib(converter=_deserialize_authors)
    datadir = _path_attr(default='data')
    files = attr.ib(default=attr.Factory(dict), converter=_deserialize_files)

    @authors.default
    def set_author_from_git(self):
        """Set the author name and email from the git repo if present."""
        if not self.repo:
            raise RuntimeError('Outside of a git repository '
                               '- unable to determine author information.')
        git_config = self.repo.config_reader()
        return Author(
            name=git_config.get('user', 'name'),
            email=git_config.get('user', 'email'))

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

    def add_data(self, url, datadir=None, git=False, **kwargs):
        """Import the data into the data directory."""
        datadir = datadir or self.datadir
        git = os.path.splitext(url)[1] == '.git' or git

        targets = kwargs.get('targets')

        if git:
            if not targets:
                raise ValueError(
                    'Need to specify target files to add from git dataset.')
            if isinstance(url, list):
                raise ValueError(
                    'Can only import from one git repo at a time.')
            if not isinstance(targets, list):
                targets = [targets]
            for target in targets:
                self.files.update(self._add_from_git(self.path, url, target))
        else:
            self.files.update(
                self._add_from_url(self.path, url, **kwargs)
            )

        self.write_metadata()
        self.commit_to_repo()

    def _add_from_url(self, path, url, nocopy=False, **kwargs):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme))

        dst = path.joinpath(os.path.basename(url)).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            if not src.exists():
                raise FileNotFoundError(
                    '{} not a valid path.'.format(src.as_posix()))

            # check if we have a directory and check for .git
            if src.is_dir():
                if src.joinpath('.git').exists():
                    targets = kwargs.get('targets', [])
                    new_files = {}
                    for target in targets:
                        new_files.update(self._add_from_git(
                            path, src.as_uri(), target))
                    return new_files

            if nocopy:
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
        return {
            dst.relative_to(self.path).as_posix():
            DatasetFile(
                dst.absolute().as_posix(),
                url,
                authors=self.authors,
                dataset=self.name)
        }

    def _add_from_git(self, path, url, target):
        """Process adding resources from anoth git repository.

        The submodules are placed in .renga/vendors and linked
        to the *path* specified by the user.

        """
        # create the submodule
        u = parse.urlparse(url)
        submodule_name = os.path.splitext(os.path.basename(u.path))[0]
        submodule_path = Path(self.repo.git_dir).parent.joinpath(
            '.renga',
            'vendors',
            u.netloc or 'local',
            os.path.dirname(u.path).lstrip('/'),
            submodule_name)

        # FIXME: do a proper check that the repos are not the same
        if submodule_name not in (s.name for s in self.repo.submodules):
            # new submodule to add
            submodule = self.repo.create_submodule(
                name=submodule_name, path=submodule_path.as_posix(), url=url)

        # link the target into the data directory
        dst = self.path.joinpath(target)
        src = submodule_path.joinpath(target)
        os.symlink(src, dst)

        # grab all the authors from the commit history
        repo = git.Repo(submodule_path.absolute().as_posix())
        authors = set(
            Author(name=commit.author.name, email=commit.author.email)
            for commit in repo.iter_commits(paths=target)
        )

        return {
            dst.relative_to(self.path).as_posix():
            DatasetFile(
                dst.absolute().as_posix(),
                '{}/{}'.format(url, target),
                authors=authors)
        }

    def write_metadata(self):
        """Write the dataset metadata to disk."""
        with open(self.path.joinpath('metadata.json'), 'w') as f:
            f.write(self.to_json())
        return self.to_json()

    def commit_to_repo(self, message=None):
        """Commit the dataset files to the git repository."""
        repo = self.repo
        if repo:
            repo.index.add([
                Path(x.path).absolute().as_posix()
                for x in self.files.values()
            ])
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

        # serialize file dict
        files = {}
        for k, v in d['files'].items():
            v['path'] = v['path'].absolute().as_posix()
            v['date_added'] = str(v['date_added'])
            files.update({k: v})

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
