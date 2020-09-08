# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
import os
import pathlib
import re
import uuid
from pathlib import Path
from urllib.parse import ParseResult, quote, urljoin, urlparse

import attr
from attr.validators import instance_of
from marshmallow import EXCLUDE, pre_dump

from renku.core import errors
from renku.core.models import jsonld as jsonld
from renku.core.models.calamus import JsonLDSchema, Nested, fields, rdfs, renku, schema
from renku.core.models.entities import Entity, EntitySchema
from renku.core.models.locals import ReferenceMixin
from renku.core.models.provenance.agents import Person, PersonSchema
from renku.core.models.refs import LinkReference
from renku.core.utils.datetime8601 import parse_date
from renku.core.utils.doi import extract_doi, is_doi

NoneType = type(None)


@attr.s
class Url:
    """Represents a schema URL reference."""

    client = attr.ib(default=None, kw_only=True)

    url = attr.ib(default=None, kw_only=True)

    url_str = attr.ib(default=None, kw_only=True)
    url_id = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)

    def default_id(self):
        """Define default value for id field."""
        if self.url_str:
            parsed_result = urlparse(self.url_str)
            id_ = ParseResult("", *parsed_result[1:]).geturl()
        elif self.url_id:
            parsed_result = urlparse(self.url_id)
            id_ = ParseResult("", *parsed_result[1:]).geturl()
        else:
            id_ = str(uuid.uuid4())

        host = "localhost"
        if self.client:
            host = self.client.remote.get("host") or host
        host = os.environ.get("RENKU_DOMAIN") or host

        return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/urls", quote(id_, safe="")))

    def default_url(self):
        """Define default value for url field."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return {"@id": self.url_id}
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")

    @property
    def value(self):
        """Returns the url value as string."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return self.url_id
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")

    def __attrs_post_init__(self):
        """Post-initialize attributes."""
        if not self.url:
            self.url = self.default_url()

        if not self._id:
            self._id = self.default_id()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return UrlSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return UrlSchema().dump(self)


def _convert_creators(value):
    """Convert creators."""
    if isinstance(value, dict):  # compatibility with previous versions
        return [Person.from_jsonld(value)]

    if isinstance(value, list):
        return [Person.from_jsonld(v) for v in value]

    return value


@attr.s
class CreatorMixin:
    """Mixin for handling creators container."""

    creators = attr.ib(kw_only=True, converter=_convert_creators)

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ", ".join(creator.name for creator in self.creators)

    @property
    def creators_full_csv(self):
        """Comma-separated list of creators with full identity."""
        return ", ".join(creator.full_identity for creator in self.creators)


def _extract_doi(value):
    """Return either a string or the doi part of a URL."""
    value = str(value)
    if is_doi(value):
        return extract_doi(value)
    return value


@attr.s(slots=True,)
class DatasetTag(object):
    """Represents a Tag of an instance of a dataset."""

    client = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, kw_only=True, validator=instance_of(str),)

    description = attr.ib(default=None, kw_only=True, validator=instance_of(str),)

    commit = attr.ib(default=None, kw_only=True, validator=instance_of(str),)

    created = attr.ib(converter=parse_date, kw_only=True)

    dataset = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)

    @created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    def default_id(self):
        """Define default value for id field."""

        host = "localhost"
        if self.client:
            host = self.client.remote.get("host") or host
        host = os.environ.get("RENKU_DOMAIN") or host

        name = "{0}@{1}".format(self.name, self.commit)

        return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/datasettags", quote(name, safe="")))

    def __attrs_post_init__(self):
        """Post-Init hook."""
        if not self._id:
            self._id = self.default_id()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return DatasetTagSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return DatasetTagSchema().dump(self)


@attr.s(slots=True,)
class Language:
    """Represent a language of an object."""

    alternate_name = attr.ib(default=None, kw_only=True)
    name = attr.ib(default=None, kw_only=True)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return LanguageSchema().load(data)


def convert_filename_path(p):
    """Return name of the file."""
    if p:
        return Path(p).name


def convert_based_on(v):
    """Convert based_on to DatasetFile."""
    if v:
        return DatasetFile.from_jsonld(v)


@attr.s(slots=True)
class DatasetFile(Entity):
    """Represent a file in a dataset."""

    added = attr.ib(converter=parse_date, kw_only=True)

    checksum = attr.ib(default=None, kw_only=True)

    filename = attr.ib(kw_only=True, converter=convert_filename_path)

    name = attr.ib(kw_only=True, default=None)

    filesize = attr.ib(default=None, kw_only=True)

    filetype = attr.ib(default=None, kw_only=True)

    url = attr.ib(default=None, kw_only=True)

    based_on = attr.ib(default=None, kw_only=True, converter=convert_based_on)

    external = attr.ib(default=False, kw_only=True)

    source = attr.ib(default=None, kw_only=True)

    @added.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @filename.default
    def default_filename(self):
        """Generate default filename based on path."""
        if self.path:
            return Path(self.path).name

    def default_url(self):
        """Generate default url based on project's ID."""
        return generate_dataset_file_url(client=self.client, filepath=self.path)

    @property
    def full_path(self):
        """Return full path in the current reference frame."""
        path = self.client.path / self.path if self.client else self.path
        return Path(os.path.abspath(path))

    @property
    def size_in_mb(self):
        """Return file size in megabytes."""
        return None if self.filesize is None else self.filesize * 1e-6

    def __attrs_post_init__(self):
        """Set the property "name" after initialization."""
        super().__attrs_post_init__()

        if not self.name:
            self.name = self.filename

        parsed_id = urlparse(self._id)

        if not parsed_id.scheme:
            self._id = "file://{}".format(self._id)

        if not self.url and self.client:
            self.url = self.default_url()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return DatasetFileSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return DatasetFileSchema().dump(self)


def _convert_dataset_files(value):
    """Convert dataset files."""
    coll = value

    if isinstance(coll, dict):  # compatibility with previous versions
        if any([key.startswith("@") for key in coll.keys()]):
            return [DatasetFile.from_jsonld(coll)]
        else:
            coll = value.values()

    return [DatasetFile.from_jsonld(v) for v in coll]


def _convert_dataset_tags(value):
    """Convert dataset tags."""
    if isinstance(value, dict):  # compatibility with previous versions
        value = [value]

    return [DatasetTag.from_jsonld(v) for v in value]


def _convert_language(obj):
    """Convert language object."""
    return Language.from_jsonld(obj) if isinstance(obj, dict) else obj


def _convert_keyword(keywords):
    """Convert keywords collection."""
    if isinstance(keywords, (list, tuple)):
        return keywords

    if isinstance(keywords, dict):
        return keywords.keys()


@attr.s
class Dataset(Entity, CreatorMixin, ReferenceMixin):
    """Represent a dataset."""

    SUPPORTED_SCHEMES = ("", "file", "http", "https", "git+https", "git+ssh")

    EDITABLE_FIELDS = [
        "creators",
        "date_published",
        "description",
        "in_language",
        "keywords",
        "license",
        "title",
        "url",
        "version",
        "date_created",
        "files",
    ]

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    date_published = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    identifier = attr.ib(default=attr.Factory(uuid.uuid4), kw_only=True, converter=_extract_doi)

    in_language = attr.ib(default=None, converter=_convert_language, kw_only=True)

    keywords = attr.ib(converter=_convert_keyword, kw_only=True, default=None)

    license = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, type=str, kw_only=True)

    url = attr.ib(default=None, kw_only=True)

    version = attr.ib(default=None, kw_only=True)

    date_created = attr.ib(converter=parse_date, kw_only=True)

    files = attr.ib(factory=list, converter=_convert_dataset_files, kw_only=True)

    tags = attr.ib(factory=list, converter=_convert_dataset_tags, kw_only=True)

    same_as = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, kw_only=True)

    @date_created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @name.validator
    def name_validator(self, attribute, value):
        """Validate name."""
        # name might have been scaped and have '%' in it
        if value and not is_dataset_name_valid(value):
            raise errors.ParameterError('Invalid "name": {}'.format(value))

    @property
    def uid(self):
        """UUID part of identifier."""
        if is_doi(self.identifier):
            return self.identifier
        return self.identifier.split("/")[-1]

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self.identifier):
            return self.identifier
        return str(self.uid)[:8]

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ", ".join(creator.name for creator in self.creators)

    @property
    def keywords_csv(self):
        """Comma-separated list of keywords associated with dataset."""
        return ", ".join(self.keywords)

    @property
    def tags_csv(self):
        """Comma-separated list of tags associated with dataset."""
        return ",".join(tag.name for tag in self.tags)

    @property
    def editable(self):
        """Subset of attributes which user can edit."""
        obj = self.as_jsonld()
        data = {field_: obj.pop(field_) for field_ in self.EDITABLE_FIELDS}
        return data

    @property
    def data_dir(self):
        """Directory where dataset files are stored."""
        if self.client:
            return Path(self.client.data_dir) / self.name
        return ""

    def contains_any(self, files):
        """Check if files are already within a dataset."""
        for file_ in files:
            if self.find_file(file_["path"]):
                return True
        return False

    def find_files(self, paths):
        """Return all paths that are in files container."""
        files_paths = {str(self.client.path / f.path) for f in self.files}
        return {p for p in paths if str(p) in files_paths}

    def find_file(self, filename, return_index=False):
        """Find a file in files container."""
        for index, file_ in enumerate(self.files):
            if str(file_.path) == str(filename):
                if return_index:
                    return index
                file_.client = self.client
                return file_

    def update_metadata(self, other_dataset):
        """Updates instance attributes with other dataset attributes.

        :param other_dataset: `Dataset`
        :return: self
        """
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
                existing_file.based_on = new_file.based_on

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
        setattr(renamed, "__reference__", self.__reference__)

        if self.__source__:
            setattr(renamed, "__source__", self.__source__.copy())

        return renamed

    def unlink_file(self, file_path):
        """Unlink a file from dataset.

        :param file_path: Relative path used as key inside files container.
        """
        index = self.find_file(file_path, return_index=True)
        return self.files.pop(index)

    def __attrs_post_init__(self):
        """Post-Init hook."""
        super().__attrs_post_init__()

        self._id = generate_dataset_id(client=self.client, identifier=self.identifier)
        self._label = self.identifier
        self.url = self._id

        # if `date_published` is set, we are probably dealing with
        # an imported dataset so `date_created` is not needed
        if self.date_published:
            self.date_created = None

        if not self.path and self.client:
            self.path = str(self.client.renku_datasets_path / self.uid)

        if self.files and self.client is not None:
            for dataset_file in self.files:
                path = Path(dataset_file.path)
                file_exists = path.exists() or (path.is_symlink() and os.path.lexists(path))

                if dataset_file.client is None and file_exists:
                    client, _, _ = self.client.resolve_in_submodules(
                        self.client.find_previous_commit(dataset_file.path, revision="HEAD"), dataset_file.path,
                    )

                    dataset_file.client = client

        try:
            if self.client:
                self.commit = self.client.find_previous_commit(self.path, revision=self.commit or "HEAD")
        except KeyError:
            # if with_dataset is used, the dataset is not committed yet
            pass

        if not self.name:
            self.name = generate_default_name(self.title, self.version)

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Return an instance from a YAML file."""
        data = jsonld.read_yaml(path)

        self = cls.from_jsonld(data=data, client=client, commit=commit)
        self.__reference__ = path

        return self

    @classmethod
    def from_jsonld(cls, data, client=None, commit=None, schema_class=None):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        schema_class = schema_class or DatasetSchema
        return schema_class(client=client, commit=commit).load(data)

    def to_yaml(self):
        """Write an instance to the referenced YAML file."""
        data = DatasetSchema().dump(self)
        jsonld.write_yaml(path=self.__reference__, data=data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return DatasetSchema().dump(self)


class CreatorMixinSchema(JsonLDSchema):
    """CreatorMixin schema."""

    class Meta:
        """Meta class."""

        unknown = EXCLUDE

    creators = Nested(schema.creator, PersonSchema, many=True)


class UrlSchema(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    url = fields.Uri(schema.url, missing=None)
    _id = fields.Id(init_name="id", missing=None)


class DatasetTagSchema(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    name = fields.String(schema.name)
    description = fields.String(schema.description)
    commit = fields.String(schema.location)
    created = fields.DateTime(schema.startDate, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    dataset = fields.String(schema.about)
    _id = fields.Id(init_name="id")

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        object.__setattr__(obj, "created", self._fix_timezone(obj.created))
        return obj


class LanguageSchema(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class DatasetFileSchema(EntitySchema):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.DigitalDocument
        model = DatasetFile
        unknown = EXCLUDE

    added = fields.DateTime(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
    name = fields.String(schema.name, missing=None)
    url = fields.String(schema.url, missing=None)
    based_on = Nested(schema.isBasedOn, "DatasetFileSchema", missing=None, propagate_client=False)
    external = fields.Boolean(renku.external, missing=False)
    source = fields.String(renku.source, missing=None)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.added = self._fix_timezone(obj.added)
        return obj


class DatasetSchema(EntitySchema, CreatorMixinSchema):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    _id = fields.Id(init_name="id", missing=None)
    _label = fields.String(rdfs.label, init_name="label", missing=None)
    date_published = fields.DateTime(
        schema.datePublished,
        missing=None,
        allow_none=True,
        format="%Y-%m-%d",
        extra_formats=("iso", "%Y-%m-%dT%H:%M:%S"),
    )
    description = fields.String(schema.description, missing=None)
    identifier = fields.String(schema.identifier)
    in_language = Nested(schema.inLanguage, LanguageSchema, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None, allow_none=True)
    license = fields.Uri(schema.license, missing=None, allow_none=True)
    title = fields.String(schema.name)
    url = fields.String(schema.url)
    version = fields.String(schema.version, missing=None)
    date_created = fields.DateTime(
        schema.dateCreated, missing=None, allow_none=True, format="iso", extra_formats=("%Y-%m-%d",)
    )
    files = Nested(schema.hasPart, DatasetFileSchema, many=True)
    tags = Nested(schema.subjectOf, DatasetTagSchema, many=True)
    same_as = Nested(schema.sameAs, UrlSchema, missing=None)
    name = fields.String(schema.alternateName)

    @pre_dump
    def fix_datetimes(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_datetimes(o, many=False, **kwargs) for o in obj]
        obj.date_published = self._fix_timezone(obj.date_published)
        obj.date_created = self._fix_timezone(obj.date_created)
        return obj


def is_dataset_name_valid(name):
    """A valid name is a valid Git reference name with no /."""
    # TODO make name an RFC 3986 compatible and migrate old projects
    return name and LinkReference.check_ref_format(name, no_slashes=True) and "/" not in name


def generate_default_name(dataset_title, dataset_version):
    """Get dataset name."""
    # For compatibility with older versions use name as name
    # if it is valid; otherwise, use encoded name
    if is_dataset_name_valid(dataset_title):
        return dataset_title

    name = re.sub(r"\s+", " ", dataset_title)
    name = name.lower()[:24]

    def to_unix(el):
        """Parse string to unix friendly name."""
        parsed_ = re.sub("[^a-zA-Z0-9]", "", re.sub(r"\s+", " ", el))
        parsed_ = re.sub(" .+", ".", parsed_.lower())
        return parsed_

    name = [to_unix(el) for el in name.split()]
    name = [el for el in name if el]

    if dataset_version:
        version = to_unix(dataset_version)
        return "{0}_{1}".format("_".join(name), version)

    return "_".join(name)


def generate_dataset_id(client, identifier):
    """Generate @id field."""
    # Determine the hostname for the resource URIs.
    # If RENKU_DOMAIN is set, it overrides the host from remote.
    # Default is localhost.
    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    # always set the id by the identifier
    return urljoin(f"https://{host}", pathlib.posixpath.join("/datasets", quote(identifier, safe="")))


def generate_dataset_file_url(client, filepath):
    """Generate url for DatasetFile."""
    if not client or not client.project:
        return

    project_id = urlparse(client.project._id)
    filepath = quote(filepath, safe="/")
    path = pathlib.posixpath.join(project_id.path, "files", "blob", filepath)
    project_id = project_id._replace(path=path)

    return project_id.geturl()
