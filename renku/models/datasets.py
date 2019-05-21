# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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

import configparser
import datetime
import os
import re
import uuid
from functools import partial

import attr
from attr.validators import instance_of
from dateutil.parser import parse as parse_date

from renku import errors
from renku._compat import Path
from renku.api.repository import default_path

from . import _jsonld as jsonld

NoneType = type(None)

_path_attr = partial(
    jsonld.ib,
    converter=Path,
)


@jsonld.s(
    type='schema:author',
    context={'schema': 'http://schema.org/'},
    slots=True,
)
class Author(object):
    """Represent the author of a resource."""

    name = jsonld.ib(validator=instance_of(str), context='schema:name')

    email = jsonld.ib(context='schema:email')

    affiliation = jsonld.ib(default=None, context='schema:affiliation')

    _id = jsonld.ib(context='@id', default=None)

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
        try:
            name = git_config.get_value('user', 'name', None)
            email = git_config.get_value('user', 'email', None)
        except (
            configparser.NoOptionError, configparser.NoSectionError
        ):  # pragma: no cover
            raise errors.ConfigurationError(
                'The user name and email are not configured. '
                'Please use the "git config" command to configure them.\n\n'
                '\tgit config --global --add user.name "John Doe"\n'
                '\tgit config --global --add user.email '
                '"john.doe@example.com"\n'
            )

        # Check the git configuration.
        if name is None:  # pragma: no cover
            raise errors.MissingUsername()
        if email is None:  # pragma: no cover
            raise errors.MissingEmail()

        return cls(name=name, email=email)

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email)

    def __attrs_post_init__(self):
        """Post-Init hook to set _id field."""
        # TODO: make it an orcid ID
        if not self._id:
            self._id = self.email


@attr.s
class AuthorsMixin:
    """Mixin for handling authors container."""

    author = jsonld.container.list(
        Author, kw_only=True, context='schema:author'
    )

    @property
    def authors_csv(self):
        """Comma-separated list of authors associated with dataset."""
        return ','.join(author.name for author in self.author)


@jsonld.s(
    type='schema:DigitalDocument',
    slots=True,
    context={'schema': 'http://schema.org/'}
)
class DatasetFile(AuthorsMixin):
    """Represent a file in a dataset."""

    path = _path_attr(kw_only=True)
    url = jsonld.ib(default=None, context='schema:url', kw_only=True)
    author = jsonld.container.list(
        Author, kw_only=True, context='schema:author'
    )
    dataset = attr.ib(default=None, kw_only=True)
    added = jsonld.ib(context='schema:dateCreated', kw_only=True)

    _id = jsonld.ib(kw_only=True, context='@id', default=None)

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()

    @property
    def full_path(self):
        """Return full path in the current reference frame."""
        return Path(
            os.path.realpath(str(self.__reference__.parent / self.path))
        )

    def __attrs_post_init__(self):
        """Post-Init hook to set _id field."""
        if not self._id:
            self._id = str(self.url)


def _parse_date(value):
    """Convert date to datetime."""
    if isinstance(value, datetime.datetime):
        return value
    return parse_date(value)


def _convert_dataset_files(value):
    """Convert dataset files."""
    coll = value

    if isinstance(value, dict):  # compatibility with previous versions
        coll = value.values()

    return [DatasetFile.from_jsonld(v) for v in coll]


def _convert_dataset_author(value):
    """Convert dataset authors."""
    coll = value

    if isinstance(value, dict):  # compatibility with previous versions
        coll = value.values()

    return [Author.from_jsonld(v) for v in coll]


@jsonld.s(
    type='schema:Dataset',
    context={'schema': 'http://schema.org/'},
)
class Dataset(AuthorsMixin):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https', 'git+https', 'git+ssh')

    name = jsonld.ib(type=str, context='schema:name')

    created = jsonld.ib(
        converter=_parse_date,
        context='schema:dateCreated',
    )

    identifier = jsonld.ib(default=None, context='schema:identifier')

    author = jsonld.container.list(
        Author, converter=_convert_dataset_author, context='schema:author'
    )

    files = jsonld.container.list(
        DatasetFile,
        converter=_convert_dataset_files,
        context='schema:DigitalDocument'
    )

    _id = jsonld.ib(context='@id', default=None)

    @property
    def uid(self):
        """UUID part of identifier."""
        return self._id.split('/')[-1]

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()

    @property
    def short_id(self):
        """Shorter version of identifier."""
        return str(self.uid)[:8]

    @property
    def authors_csv(self):
        """Comma-separated list of authors associated with dataset."""
        return ','.join(author.name for author in self.author)

    def find_file(self, file_path, return_index=False):
        """Find a file in files container."""
        for index, file_ in enumerate(self.files):
            if str(file_.path) == str(file_path):
                if return_index:
                    return index
                return file_

    def update_files(self, files):
        """Update files with collection of DatasetFile objects."""
        to_insert = [
            new_file
            for new_file in files if not self.find_file(new_file.path)
        ]
        self.files += to_insert

    def rename_files(self, rename):
        """Rename files using the path mapping function."""
        files = []

        for file_ in self.files:
            new_path = rename(file_.path)
            new_file = attr.evolve(file_, path=new_path)
            if not self.find_file(new_file.path):
                files.append(new_file)
            else:
                raise FileExistsError

        renamed = attr.evolve(self, files=files)
        setattr(renamed, '__reference__', self.__reference__)

        if self.__source__:
            setattr(renamed, '__source__', self.__source__.copy())

        return renamed

    def unlink_file(self, file_path):
        """Unlink a file from dataset.

        :param file_path: Relative path used as key inside files container.
        """
        index = self.find_file(file_path, return_index=True)
        return self.files.pop(index)

    def __attrs_post_init__(self):
        """Post-Init hook to set _id field."""
        if not self.identifier:
            self.identifier = str(uuid.uuid4())

        if not self._id:
            self._id = '{0}/datasets/{1}'.format(
                Path(default_path()).name, self.identifier
            )
            self.identifier = self._id
