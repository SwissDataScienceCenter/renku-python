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

import datetime
import json
import os
import re
import shutil
import stat
import uuid
import warnings
from functools import partial
from urllib import error, parse, request

import attr
import dateutil
import git
import requests
import yaml
from attr.validators import instance_of
from dateutil.parser import parse as parse_date

from renga._compat import Path
from renga.cli._repo import Repo

from . import _jsonld as jsonld

NoneType = type(None)

_path_attr = partial(
    jsonld.ib,
    converter=Path,
    validator=lambda i, arg, val: Path(val).absolute().is_file()
)


def _deserialize_set(s, cls):
    """Deserialize a list of dicts into classes."""
    return set(
        cls.from_jsonld(x) if hasattr(cls, 'from_jsonld') else cls(**x)
        for x in s
    )


def _deserialize_dict(d, cls):
    """Deserialize a list of dicts into classes."""
    return {
        k: cls.from_jsonld(v) if hasattr(cls, 'from_jsonld') else cls(**v)
        for k, v in d.items()
    }


@jsonld.s(
    type='dcterms:creator',
    context={
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'dcterms': 'http://purl.org/dc/terms/',
        'scoro': 'http://purl.org/spar/scoro/',
    },
    frozen=True,
)
class Author(object):
    """Represent the author of a resource."""

    name = jsonld.ib(validator=instance_of(str), context='dcterms:name')
    email = jsonld.ib(context='dcterms:email')
    affiliation = jsonld.ib(default=None, context='scoro:affiliate')

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if not (
            isinstance(value, str) and re.match(r"[^@]+@[^@]+\.[^@]+", value)
        ):
            raise ValueError('Email address is invalid.')

    @classmethod
    def from_git(cls, git):
        """Create an instance from a Git repo."""
        git_config = git.config_reader()
        return cls(
            name=git_config.get('user', 'name'),
            email=git_config.get('user', 'email'),
        )


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

    raise ValueError(
        'Authors must be a dict or '
        'set or list of dicts or Author.'
    )


@jsonld.s
class DatasetFile(object):
    """Represent a file in a dataset."""

    path = _path_attr()
    origin = attr.ib(converter=lambda x: str(x))
    authors = jsonld.ib(
        default=attr.Factory(set),
        converter=_deserialize_authors,
    )
    dataset = attr.ib(default=None)
    added = jsonld.ib(context='http://schema.org/dateCreated', )

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()


_deserialize_files = partial(_deserialize_dict, cls=DatasetFile)


def _parse_date(value):
    """Convert date to datetime."""
    if isinstance(value, datetime.datetime):
        return value
    return parse_date(value)


@jsonld.s(
    type='dctypes:Dataset',
    context={
        'dcterms': 'http://purl.org/dc/terms/',
        'dctypes': 'http://purl.org/dc/dcmitypes/',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'prov': 'http://www.w3.org/ns/prov#',
        'scoro': 'http://purl.org/spar/scoro/',
    },
)
class Dataset(object):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https')

    name = jsonld.ib(type='string', context='foaf:name')

    created = jsonld.ib(
        converter=_parse_date,
        context='http://schema.org/dateCreated',
    )

    identifier = jsonld.ib(
        default=attr.Factory(uuid.uuid4),
        converter=lambda x: uuid.UUID(str(x)),
        context={
            '@id': 'dctypes:Dataset',
            '@type': '@id',
        },
    )
    authors = jsonld.ib(
        default=attr.Factory(set),  # FIXME should respect order
        converter=_deserialize_authors,
        context={
            '@id': Author._jsonld_type,
            '@container': '@list',
        },
    )
    files = jsonld.ib(
        default=attr.Factory(dict),
        converter=_deserialize_files,
        context={
            '@id': DatasetFile._jsonld_type,
            '@container': '@index',
        },
    )

    datadir = _path_attr(default='data')

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()

    @property
    def path(self):
        """Path to this Dataset."""
        return self.datadir / self.name

    def meta_init(self):
        """Initialize the directories and metadata."""
        try:
            os.makedirs(self.path)
        except FileExistsError:
            raise FileExistsError('This dataset already exists.')

    def add_data(self, repo, url, datadir=None, git=False, **kwargs):
        """Import the data into the data directory."""
        datadir = datadir or self.datadir
        git = git or check_for_git_repo(url)

        target = kwargs.get('target')

        if git:
            if isinstance(target, (str, NoneType)):
                self.files.update(
                    self._add_from_git(repo, self.path, url, target)
                )
            else:
                for t in target:
                    self.files.update(
                        self._add_from_git(repo, self.path, url, t)
                    )
        else:
            self.files.update(
                self._add_from_url(repo, self.path, url, **kwargs)
            )

    def _add_from_url(self, repo, path, url, nocopy=False, **kwargs):
        """Process an add from url and return the location on disk."""
        u = parse.urlparse(url)

        if u.scheme not in Dataset.SUPPORTED_SCHEMES:
            raise NotImplementedError(
                '{} URLs are not supported'.format(u.scheme)
            )

        dst = path.joinpath(os.path.basename(url)).absolute()

        if u.scheme in ('', 'file'):
            src = Path(u.path).absolute()

            # if we have a directory, recurse
            if src.is_dir():
                files = {}
                os.mkdir(dst)
                for f in src.iterdir():
                    files.update(
                        self._add_from_url(
                            repo, dst, f.absolute().as_posix(), nocopy=nocopy
                        )
                    )
                return files
            if nocopy:
                try:
                    os.link(src, dst)
                except Exception as e:
                    raise Exception(
                        'Could not create hard link '
                        '- retry without nocopy.'
                    ) from e
            else:
                shutil.copy(src, dst)

        else:
            try:
                response = requests.get(url)
                dst.write_bytes(response.content)
            except error.HTTPError as e:  # pragma nocover
                raise e

        # make the added file read-only
        mode = dst.stat().st_mode & 0o777
        dst.chmod(mode & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))

        repo.track_paths_in_storage(dst.relative_to(repo.path))
        result = dst.relative_to(repo.path / self.path).as_posix()
        return {
            result:
                DatasetFile(
                    result, url, authors=self.authors, dataset=self.name
                )
        }

    def _add_from_git(self, repo, path, url, target):
        """Process adding resources from another git repository.

        The submodules are placed in .renga/vendors and linked
        to the *path* specified by the user.
        """
        # create the submodule
        u = parse.urlparse(url)
        submodule_path = repo.renga_path / 'vendors' / (u.netloc or 'local')

        if u.scheme in ('', 'file'):
            # determine where is the base repo path
            r = git.Repo(url, search_parent_directories=True)
            src_repo_path = Path(r.git_dir).parent
            submodule_name = os.path.basename(src_repo_path)
            submodule_path = submodule_path / str(src_repo_path).lstrip('/')

            # if repo path is a parent, rebase the paths and update url
            if src_repo_path != Path(u.path):
                top_target = Path(
                    u.path
                ).resolve().absolute().relative_to(src_repo_path)
                if target:
                    target = top_target / target
                else:
                    target = top_target
                url = src_repo_path.as_posix()
        elif u.scheme in ('http', 'https'):
            submodule_name = os.path.splitext(os.path.basename(u.path))[0]
            submodule_path = submodule_path.joinpath(
                os.path.dirname(u.path).lstrip('/'), submodule_name
            )
        else:
            raise NotImplementedError(
                'Scheme {} not supported'.format(u.scheme)
            )

        # FIXME: do a proper check that the repos are not the same
        if submodule_name not in (s.name for s in repo.git.submodules):
            # new submodule to add
            submodule = repo.git.create_submodule(
                name=submodule_name, path=submodule_path.as_posix(), url=url
            )

        # link the target into the data directory
        dst = repo.path / self.path / submodule_name / (target or '')
        src = submodule_path / (target or '')

        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)
        # if we have a directory, recurse
        if src.is_dir():
            files = {}
            os.mkdir(dst)
            for f in src.iterdir():
                files.update(
                    self._add_from_git(
                        repo, path, url, target=f.relative_to(submodule_path)
                    )
                )
            return files

        os.symlink(os.path.relpath(src, dst.parent), dst)

        # grab all the authors from the commit history
        git_repo = git.Repo(submodule_path.absolute().as_posix())
        authors = set(
            Author(name=commit.author.name, email=commit.author.email)
            for commit in git_repo.iter_commits(paths=target)
        )

        result = dst.relative_to(repo.path / self.path).as_posix()
        return {
            result:
                DatasetFile(
                    result, '{}/{}'.format(url, target), authors=authors
                )
        }

    @classmethod
    def create(cls, *args, **kwargs):
        """Create a new dataset and create its directories and metadata."""
        d = cls(*args, **kwargs)
        d.meta_init()
        return d


def check_for_git_repo(url):
    """Check if a url points to a git repository."""
    u = parse.urlparse(url)
    is_git = False

    if os.path.splitext(u.path)[1] == '.git':
        is_git = True
    elif u.scheme in ('', 'file'):
        try:
            r = git.Repo(u.path, search_parent_directories=True)
            is_git = True
        except git.InvalidGitRepositoryError:
            is_git = False
    return is_git
