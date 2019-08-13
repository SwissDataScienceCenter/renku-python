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
import re
import uuid
from functools import partial

import attr
from attr.validators import instance_of
from dateutil.parser import parse as parse_date

from renku import errors
from renku._compat import Path
from renku.models.provenance.entities import Entity
from renku.utils.doi import is_doi

from . import _jsonld as jsonld

NoneType = type(None)

_path_attr = partial(
    jsonld.ib,
    converter=Path,
)


@jsonld.s(
    type='schema:Person',
    context={'schema': 'http://schema.org/'},
    slots=True,
)
class Creator(object):
    """Represent the creator of a resource."""

    client = attr.ib(default=None, kw_only=True)

    affiliation = jsonld.ib(
        default=None, kw_only=True, context='schema:affiliation'
    )

    email = jsonld.ib(default=None, kw_only=True, context='schema:email')

    alternate_name = jsonld.ib(
        default=None, kw_only=True, context='schema:alternateName'
    )

    name = jsonld.ib(
        default=None,
        kw_only=True,
        validator=instance_of(str),
        context='schema:name'
    )

    _id = jsonld.ib(default=None, kw_only=True, context='@id')

    @property
    def short_name(self):
        """Gives full name in short form."""
        names = self.name.split()
        if len(names) == 1:
            return self.name

        last_name = names[-1]
        initials = [name[0] for name in names]
        initials.pop()

        return '{0}.{1}'.format('.'.join(initials), last_name)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if self.email and not (
            isinstance(value, str) and re.match(r'[^@]+@[^@]+\.[^@]+', value)
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
        if not self._id:
            self._id = 'mailto:{self.email}'.format(self=self)


@attr.s
class CreatorsMixin:
    """Mixin for handling creators container."""

    creator = jsonld.container.list(
        Creator, kw_only=True, context='schema:creator'
    )

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ','.join(creator.name for creator in self.creator)


@jsonld.s(
    type='schema:Language',
    context={'schema': 'http://schema.org/'},
    slots=True,
)
class Language:
    """Represent a language of an object."""

    alternate_name = jsonld.ib(
        default=None, kw_only=True, context='schema:alternateName'
    )
    name = jsonld.ib(default=None, kw_only=True, context='schema:name')


def _convert_dataset_files_creators(value):
    """Convert dataset files creators."""
    coll = value

    if isinstance(coll, dict):
        return [Creator.from_jsonld(coll)]

    if isinstance(coll, list):
        return [Creator.from_jsonld(c) for c in coll]


@jsonld.s(
    type='schema:DigitalDocument',
    slots=True,
    context={'schema': 'http://schema.org/'}
)
class DatasetFile(Entity, CreatorsMixin):
    """Represent a file in a dataset."""

    creator = jsonld.container.list(
        Creator,
        converter=_convert_dataset_files_creators,
        kw_only=True,
        context='schema:creator'
    )

    added = jsonld.ib(context='schema:dateCreated', kw_only=True)

    checksum = attr.ib(default=None, kw_only=True)

    dataset = jsonld.ib(context='schema:isPartOf', default=None, kw_only=True)

    filename = attr.ib(kw_only=True)

    filesize = attr.ib(default=None, kw_only=True)

    filetype = attr.ib(default=None, kw_only=True)

    url = jsonld.ib(default=None, context='schema:url', kw_only=True)

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()

    @filename.default
    def default_filename(self):
        """Generate default filename based on path."""
        return Path(self.path).name

    @property
    def full_path(self):
        """Return full path in the current reference frame."""
        path = Path(self.path)
        if self.client:
            return (self.client.path / path).resolve()
        return path.resolve()

    @property
    def size_in_mb(self):
        """Return file size in megabytes."""
        return self.filesize * 1e-6


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


def _convert_dataset_creator(value):
    """Convert dataset creators."""
    if isinstance(value, dict):  # compatibility with previous versions
        return [Creator.from_jsonld(value)]

    if isinstance(value, list):
        return [Creator.from_jsonld(v) for v in value]


def _convert_language(obj):
    """Convert language object."""
    if isinstance(obj, dict):
        language = Language.from_jsonld(obj)
        return language


def _convert_keyword(keywords):
    """Convert keywords collection."""
    if isinstance(keywords, list):
        return keywords

    if isinstance(keywords, dict):
        return keywords.keys()


@jsonld.s(
    type='schema:Dataset',
    context={
        'added': 'schema:dateCreated',
        'affiliation': 'schema:affiliation',
        'alternate_name': 'schema:alternateName',
        'email': 'schema:email',
        'name': 'schema:name',
        'schema': 'http://schema.org/',
        'url': 'schema:url'
    },
)
class Dataset(Entity, CreatorsMixin):
    """Repesent a dataset."""

    SUPPORTED_SCHEMES = ('', 'file', 'http', 'https', 'git+https', 'git+ssh')

    EDITABLE_FIELDS = [
        'creator', 'date_published', 'description', 'in_language', 'keywords',
        'license', 'name', 'url', 'version', 'created', 'files'
    ]

    _id = jsonld.ib(default=None, context='@id', kw_only=True)
    _label = jsonld.ib(default=None, context='rdfs:label', kw_only=True)

    creator = jsonld.container.list(
        Creator,
        converter=_convert_dataset_creator,
        context='schema:creator',
        kw_only=True
    )

    date_published = jsonld.ib(
        default=None, context='schema:datePublished', kw_only=True
    )

    description = jsonld.ib(
        default=None, context='schema:description', kw_only=True
    )

    identifier = jsonld.ib(
        default=attr.Factory(uuid.uuid4),
        context='schema:identifier',
        kw_only=True,
        converter=str
    )

    in_language = jsonld.ib(
        type=Language,
        default=None,
        converter=_convert_language,
        context='schema:inLanguage',
        kw_only=True
    )

    keywords = jsonld.container.list(
        str,
        converter=_convert_keyword,
        context='schema:keywords',
        kw_only=True
    )

    license = jsonld.ib(default=None, context='schema:license', kw_only=True)

    name = jsonld.ib(
        default=None, type=str, context='schema:name', kw_only=True
    )

    url = jsonld.ib(default=None, context='schema:url', kw_only=True)

    version = jsonld.ib(default=None, context='schema:version', kw_only=True)

    created = jsonld.ib(
        converter=_parse_date, context='schema:dateCreated', kw_only=True
    )

    files = jsonld.container.list(
        DatasetFile,
        default=None,
        converter=_convert_dataset_files,
        context='schema:hasPart',
        kw_only=True
    )

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.utcnow()

    @property
    def display_name(self):
        """Get dataset display name."""
        name = re.sub(' +', ' ', self.name.lower()[:24])

        def to_unix(el):
            """Parse string to unix friendly name."""
            parsed_ = re.sub('[^a-zA-Z0-9]', '', re.sub(' +', ' ', el))
            parsed_ = re.sub(' .+', '.', parsed_.lower())
            return parsed_

        short_name = [to_unix(el) for el in name.split()]

        if self.version:
            version = to_unix(self.version)
            name = '{0}_{1}'.format('_'.join(short_name), version)
            return name

        return '.'.join(short_name)

    @property
    def uid(self):
        """UUID part of identifier."""
        return self.identifier.split('/')[-1]

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self._id):
            return self._id
        return str(self.uid)[:8]

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ','.join(creator.short_name for creator in self.creator)

    @property
    def editable(self):
        """Subset of attributes which user can edit."""
        obj = self.asjsonld()
        data = {field_: obj.pop(field_) for field_ in self.EDITABLE_FIELDS}
        return data

    def find_file(self, filename, return_index=False):
        """Find a file in files container."""
        for index, file_ in enumerate(self.files):
            if str(file_.path) == str(filename):
                if return_index:
                    return index
                return file_

    def update_metadata(self, other_dataset):
        """Updates instance attributes with other dataset attributes.

        :param other_dataset: `Dataset`
        :return: self
        """
        if is_doi(other_dataset.identifier):
            self._id = other_dataset.identifier

        for field_ in self.EDITABLE_FIELDS:
            val = getattr(other_dataset, field_)
            if val:
                setattr(self, field_, val)

        return self

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
        self._id = self.identifier

        if not self._label:
            self._label = self.identifier

        if not self.path:
            self.path = str(self.client.renku_datasets_path / str(self.uid))

        if self.files:
            for datasetfile in self.files:
                if datasetfile.client is None:
                    client, _, _ = self.client.resolve_in_submodules(
                        self.client.find_previous_commit(
                            datasetfile.path, revision='HEAD'
                        ),
                        datasetfile.path,
                    )

                    datasetfile.client = client
