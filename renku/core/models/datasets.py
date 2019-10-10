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
import pathlib
import re
import urllib
import uuid
from functools import partial
from pathlib import Path

import attr
from attr.validators import instance_of

from renku.core import errors
from renku.core.models.provenance.entities import Entity
from renku.core.utils.datetime8601 import parse_date
from renku.core.utils.doi import extract_doi, is_doi

from . import jsonld as jsonld

NoneType = type(None)

_path_attr = partial(
    jsonld.ib,
    converter=Path,
)


def _extract_doi(value):
    """Return either a string or the doi part of a URL."""
    value = str(value)
    if is_doi(value):
        return extract_doi(value)
    return value


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

    _id = jsonld.ib(kw_only=True, context='@id')

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
        if not name:  # pragma: no cover
            raise errors.MissingUsername()
        if not email:  # pragma: no cover
            raise errors.MissingEmail()

        return cls(name=name, email=email)

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email)

    @_id.default
    def default_id(self):
        """Set the default id."""
        if self.email:
            return 'mailto:{email}'.format(email=self.email)
        return '_:{}'.format(str(uuid.uuid4()))

    def __attrs_post_init__(self):
        """Finish object initialization."""
        # handle the case where ids were improperly set
        if self._id == 'mailto:None':
            self._id = self.default_id()


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
    type='schema:PublicationEvent',
    context={'schema': 'http://schema.org/'},
    frozen=True,
    slots=True,
)
class DatasetTag(object):
    """Represents a Tag of an instance of a dataset."""

    client = attr.ib(default=None, kw_only=True)

    name = jsonld.ib(
        default=None,
        kw_only=True,
        validator=instance_of(str),
        context='schema:name'
    )

    description = jsonld.ib(
        default=None,
        kw_only=True,
        validator=instance_of(str),
        context='schema:description'
    )

    commit = jsonld.ib(
        default=None,
        kw_only=True,
        validator=instance_of(str),
        context='schema:location'
    )

    created = jsonld.ib(
        converter=parse_date, context='schema:startDate', kw_only=True
    )

    dataset = jsonld.ib(context='schema:about', default=None, kw_only=True)

    _id = jsonld.ib(kw_only=True, context='@id')

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @_id.default
    def default_id(self):
        """Define default value for id field."""
        return '{0}@{1}'.format(self.name, self.commit)


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
    context={
        'schema': 'http://schema.org/',
    }
)
class DatasetFile(Entity, CreatorsMixin):
    """Represent a file in a dataset."""

    creator = jsonld.container.list(
        Creator,
        converter=_convert_dataset_files_creators,
        kw_only=True,
        context='schema:creator'
    )

    added = jsonld.ib(
        converter=parse_date, context='schema:dateCreated', kw_only=True
    )

    checksum = attr.ib(default=None, kw_only=True)

    dataset = jsonld.ib(context='schema:isPartOf', default=None, kw_only=True)

    filename = attr.ib(kw_only=True, converter=lambda x: Path(x).name)

    name = jsonld.ib(context='schema:name', kw_only=True, default=None)

    filesize = attr.ib(default=None, kw_only=True)

    filetype = attr.ib(default=None, kw_only=True)

    url = jsonld.ib(default=None, context='schema:url', kw_only=True)

    based_on = jsonld.ib(
        default=None, context='schema:isBasedOn', kw_only=True
    )

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @filename.default
    def default_filename(self):
        """Generate default filename based on path."""
        if self.path:
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

    def __attrs_post_init__(self):
        """Set the property "name" after initialization."""
        super().__attrs_post_init__()

        if not self.name:
            self.name = self.filename


def _convert_dataset_files(value):
    """Convert dataset files."""
    coll = value

    if isinstance(coll, dict):  # compatibility with previous versions
        if any([key.startswith('@') for key in coll.keys()]):
            return [DatasetFile.from_jsonld(coll)]
        else:
            coll = value.values()

    return [DatasetFile.from_jsonld(v) for v in coll]


def _convert_dataset_tags(value):
    """Convert dataset tags."""
    return [DatasetTag.from_jsonld(v) for v in value]


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
        'affiliation': 'schema:affiliation',
        'alternate_name': 'schema:alternateName',
        'email': 'schema:email',
        'schema': 'http://schema.org/',
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
        default='', context='schema:description', kw_only=True
    )

    identifier = jsonld.ib(
        default=attr.Factory(uuid.uuid4),
        context='schema:identifier',
        kw_only=True,
        converter=_extract_doi
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

    based_on = jsonld.ib(
        default=None, context='schema:isBasedOn', kw_only=True
    )

    license = jsonld.ib(default=None, context='schema:license', kw_only=True)

    name = jsonld.ib(
        default=None, type=str, context='schema:name', kw_only=True
    )

    url = jsonld.ib(default=None, context='schema:url', kw_only=True)

    version = jsonld.ib(default=None, context='schema:version', kw_only=True)

    created = jsonld.ib(
        converter=parse_date, context='schema:dateCreated', kw_only=True
    )

    files = jsonld.container.list(
        DatasetFile,
        default=None,
        converter=_convert_dataset_files,
        context='schema:hasPart',
        kw_only=True
    )

    tags = jsonld.container.list(
        DatasetTag,
        default=None,
        converter=_convert_dataset_tags,
        context='schema:subjectOf',
        kw_only=True
    )

    same_as = jsonld.ib(context='schema:sameAs', default=None, kw_only=True)

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

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
        if is_doi(self.identifier):
            return self.identifier
        return self.identifier.split('/')[-1]

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self.identifier):
            return self.identifier
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
            self.same_as = urllib.parse.urljoin(
                'https://doi.org', other_dataset.identifier
            )

        for field_ in self.EDITABLE_FIELDS:
            val = getattr(other_dataset, field_)
            if val:
                setattr(self, field_, val)

        return self

    def update_files(self, files):
        """Update files with collection of DatasetFile objects."""
        to_insert = []

        for new_file in files:
            existing_file = self.find_file(new_file.path)
            if existing_file is None:
                to_insert.append(new_file)
            else:
                existing_file.commit = new_file.commit
                existing_file._label = new_file._label

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
        """Post-Init hook."""
        from urllib.parse import quote

        super().__attrs_post_init__()

        # Determine the hostname for the resource URIs.
        # If RENKU_DOMAIN is set, it overrides the host from remote.
        # Default is localhost.
        host = 'localhost'
        if self.client:
            host = self.client.remote.get('host') or host
        host = os.environ.get('RENKU_DOMAIN') or host

        # always set the id by the identifier
        self._id = urllib.parse.urljoin(
            'https://{host}'.format(host=host),
            pathlib.posixpath.join(
                '/datasets', quote(self.identifier, safe='')
            )
        )

        # if `date_published` is set, we are probably dealing with
        # an imported dataset so `created` is not needed
        if self.date_published:
            self.created = None

        self._label = self.identifier

        if not self.path:
            self.path = str(
                self.client.renku_datasets_path /
                quote(str(self.uid), safe='')
            )

        if self.files:
            for dataset_file in self.files:
                file_exists = Path(dataset_file.path).exists()

                if dataset_file.client is None and file_exists:
                    client, _, _ = self.client.resolve_in_submodules(
                        self.client.find_previous_commit(
                            dataset_file.path, revision='HEAD'
                        ),
                        dataset_file.path,
                    )

                    dataset_file.client = client

        try:
            if self.client:
                self.commit = self.client.find_previous_commit(
                    self.path, revision=self.commit or 'HEAD'
                )
        except KeyError:
            # if with_dataset is used, the dataset is not committed yet
            pass
