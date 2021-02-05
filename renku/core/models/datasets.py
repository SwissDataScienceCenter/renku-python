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
import marshmallow
from attr.validators import instance_of
from marshmallow import EXCLUDE, pre_dump

from renku.core import errors
from renku.core.models import jsonld as jsonld
from renku.core.models.calamus import DateTimeList, JsonLDSchema, Nested, Uri, fields, prov, rdfs, renku, schema
from renku.core.models.entities import Entity, EntitySchema
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
        return generate_url_id(client=self.client, url_str=self.url_str, url_id=self.url_id)

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
        elif isinstance(self.url, dict):
            if "_id" in self.url:
                self.url["@id"] = self.url.pop("_id")
            self.url_id = self.url["@id"]
        elif isinstance(self.url, str):
            self.url_str = self.url

        if not self._id or self._id.startswith("_:"):
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
        return generate_dataset_tag_id(client=self.client, name=self.name, commit=self.commit)

    def __attrs_post_init__(self):
        """Post-Init hook."""
        if not self._id or self._id.startswith("_:"):
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

    is_lfs = attr.ib(default=False, kw_only=True)

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
    def commit_sha(self):
        """Return commit hash."""
        return self.commit.hexsha if self.commit else ""

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

    def update_commit(self, commit):
        """Set commit and update associated fields."""
        self.commit = commit
        self._id = self.default_id()
        self._label = self.default_label()

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
class Dataset(Entity, CreatorMixin):
    """Represent a dataset."""

    SUPPORTED_SCHEMES = ("", "file", "http", "https", "git+https", "git+ssh")

    EDITABLE_FIELDS = [
        "creators",
        "date_created",
        "date_published",
        "description",
        "files",
        "in_language",
        "keywords",
        "license",
        "title",
        "url",
        "version",
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

    derived_from = attr.ib(default=None, kw_only=True)

    immutable = attr.ib(default=False, kw_only=True)

    _modified = attr.ib(default=False, init=False)

    _mutated = attr.ib(default=False, init=False)

    _metadata_path = attr.ib(default=None, init=False)

    @date_created.default
    def _now(self):
        """Define default value for datetime fields."""
        return datetime.datetime.now(datetime.timezone.utc)

    @name.validator
    def name_validator(self, attribute, value):
        """Validate name."""
        # name might have been escaped and have '%' in it
        if value and not is_dataset_name_valid(value):
            raise errors.ParameterError('Invalid "name": {}'.format(value))

    @property
    def short_id(self):
        """Shorter version of identifier."""
        if is_doi(self.identifier):
            return self.identifier
        return str(self.identifier)[:8]

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

    @property
    def original_identifier(self):
        """Return the first identifier of the dataset."""
        if self.path:
            return Path(self.path).name

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

    def find_file(self, path, return_index=False):
        """Find a file in files container using its relative path."""
        for index, file_ in enumerate(self.files):
            if str(file_.path) == str(path):
                if return_index:
                    return index
                file_.client = self.client
                return file_

    def update_metadata(self, **kwargs):
        """Updates instance attributes."""
        for attribute, value in kwargs.items():
            if value and value != getattr(self, attribute):
                self._modified = True
                setattr(self, attribute, value)

        return self

    def update_metadata_from(self, other_dataset):
        """Updates instance attributes with other dataset attributes.

        :param other_dataset: `Dataset`
        :return: self
        """
        for field_ in self.EDITABLE_FIELDS:
            val = getattr(other_dataset, field_)
            if val:
                self._modified = True
                setattr(self, field_, val)

        self.same_as = other_dataset.same_as

        return self

    def update_files(self, files):
        """Update files with collection of DatasetFile objects."""
        new_files = []

        for new_file in files:
            old_file = self.find_file(new_file.path)
            if not old_file:
                new_files.append(new_file)
            elif new_file.commit != old_file.commit or new_file.added != old_file.added:
                self.unlink_file(new_file.path)
                new_files.append(new_file)

        if not new_files:
            return

        self._modified = True
        self.files += new_files

        self._update_files_metadata()

    def rename_files(self, rename):
        """Rename files using the path mapping function."""
        files = []

        for file_ in self.files:
            self._modified = True
            new_path = rename(file_.path)
            new_file = attr.evolve(file_, path=new_path)
            if not self.find_file(new_file.path):
                files.append(new_file)
            else:
                raise errors.InvalidFileOperation(f"Destination file already exists: {new_file.path}")

        self.files = files

    def unlink_file(self, path, missing_ok=False):
        """Unlink a file from dataset.

        :param path: Relative path used as key inside files container.
        """
        index = self.find_file(path, return_index=True)
        if index is not None:
            self._modified = True
            return self.files.pop(index)

        if not missing_ok:
            raise errors.InvalidFileOperation(f"File cannot be found: {path}")

    def mutate(self):
        """Update mutation history and assign a new identifier.

        Do not mutate more than once before committing the metadata or otherwise there would be missing links in the
        chain of changes.
        """
        if self.immutable:
            raise errors.OperationError(f"Cannot mutate an immutable dataset: {self.name}")

        # As a safetynet, we only allow one mutation during lifetime of a dataset object; this is not 100% error-proof
        # because one can create a new object from a mutated but uncommitted metadata file.
        if self._mutated:
            return
        self._mutated = True

        self.same_as = None
        self.derived_from = Url(url_id=self._id)

        if self.client:
            mutator = Person.from_git(self.client.repo)
            if not any(c for c in self.creators if c.email == mutator.email):
                self.creators.append(mutator)

        self.date_created = self._now()
        self.date_published = None

        self._replace_identifier(new_identifier=str(uuid.uuid4()))

    def _replace_identifier(self, new_identifier):
        """Replace identifier and update all related fields."""
        self.identifier = new_identifier
        self._set_id()
        self.url = self._id
        self._label = self.identifier

    def _get_host(self):
        # Determine the hostname for the resource URIs.
        # If RENKU_DOMAIN is set, it overrides the host from remote.
        # Default is localhost.
        host = "localhost"
        if self.client:
            host = self.client.remote.get("host") or host
        return os.environ.get("RENKU_DOMAIN") or host

    def _set_id(self):
        self._id = generate_dataset_id(client=self.client, identifier=self.identifier)

    def __attrs_post_init__(self):
        """Post-Init hook."""
        super().__attrs_post_init__()

        self._set_id()
        self.url = self._id
        self._label = self.identifier

        if self.derived_from:
            host = self._get_host()
            derived_from_id = self.derived_from._id
            derived_from_url = self.derived_from.url.get("@id")
            u = urlparse(derived_from_url)
            derived_from_url = u._replace(netloc=host).geturl()
            self.derived_from = Url(id=derived_from_id, url_id=derived_from_url)

        # if `date_published` is set, we are probably dealing with
        # an imported dataset so `date_created` is not needed
        if self.date_published:
            self.date_created = None

        if not self.path and self.client:
            absolute_path = LinkReference(client=self.client, name=f"datasets/{self.name}").reference.parent
            self.path = str(absolute_path.relative_to(self.client.path))

        self._update_files_metadata()

        try:
            if self.client:
                self.commit = self.client.find_previous_commit(self.path, revision=self.commit or "HEAD")
        except KeyError:
            # if with_dataset is used, the dataset is not committed yet
            pass

        if not self.name:
            self.name = generate_default_name(self.title, self.version)

    def _update_files_metadata(self):
        if not self.files or not self.client:
            return

        paths = [f.path for f in self.files]
        attrs = self.client.find_attr(*paths)

        for file_ in self.files:
            path = Path(file_.path)
            file_exists = path.exists() or (path.is_symlink() and os.path.lexists(path))

            if not file_exists:
                continue

            if attrs.get(str(path), {}).get("filter") == "lfs":
                file_.is_lfs = True
            else:
                file_.is_lfs = False

            if file_.client is None:
                client, _, _ = self.client.resolve_in_submodules(
                    self.client.find_previous_commit(file_.path, revision="HEAD"), file_.path,
                )

                file_.client = client

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Return an instance from a YAML file."""
        data = jsonld.read_yaml(path)

        self = cls.from_jsonld(data=data, client=client, commit=commit)
        self._metadata_path = path

        return self

    @classmethod
    def from_jsonld(cls, data, client=None, commit=None, schema_class=None):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, (dict, list)):
            raise ValueError(data)

        schema_class = schema_class or DatasetSchema
        return schema_class(client=client, commit=commit, flattened=True).load(data)

    def to_yaml(self, path=None):
        """Write an instance to the referenced YAML file."""
        if self._modified and not self.immutable:
            self.mutate()

        self._metadata_path = path or self._metadata_path
        data = DatasetSchema(flattened=True).dump(self)
        jsonld.write_yaml(path=self._metadata_path, data=data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return DatasetSchema(flattened=True).dump(self)


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

    url = Uri(schema.url, missing=None)
    _id = fields.Id(init_name="id", missing=None)


class DatasetTagSchema(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    name = fields.String(schema.name)
    description = fields.String(schema.description, missing=None)
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

    added = DateTimeList(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
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
    keywords = fields.List(schema.keywords, fields.String(), allow_none=True, missing=None)
    license = Uri(schema.license, allow_none=True, missing=None)
    title = fields.String(schema.name)
    url = fields.String(schema.url, missing=None)
    version = fields.String(schema.version, missing=None)
    date_created = fields.DateTime(
        schema.dateCreated, missing=None, allow_none=True, format="iso", extra_formats=("%Y-%m-%d",)
    )
    files = Nested(schema.hasPart, DatasetFileSchema, many=True)
    tags = Nested(schema.subjectOf, DatasetTagSchema, many=True)
    same_as = Nested(schema.sameAs, UrlSchema, missing=None)
    name = fields.String(schema.alternateName)
    derived_from = Nested(prov.wasDerivedFrom, UrlSchema, missing=None)

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


def generate_default_name(dataset_title, dataset_version=None):
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


def generate_url_id(client, url_str, url_id):
    """Generate @id field for Url."""
    if url_str:
        parsed_result = urlparse(url_str)
        id_ = ParseResult("", *parsed_result[1:]).geturl()
    elif url_id:
        parsed_result = urlparse(url_id)
        id_ = ParseResult("", *parsed_result[1:]).geturl()
    else:
        id_ = str(uuid.uuid4())

    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/urls", quote(id_, safe="")))


def generate_dataset_tag_id(client, name, commit):
    """Generate @id field for DatasetTag."""
    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    name = "{0}@{1}".format(name, commit)

    return urljoin("https://{host}".format(host=host), pathlib.posixpath.join("/datasettags", quote(name, safe="")))


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


class DatasetCreatorsJson(marshmallow.Schema):
    """Schema for the dataset creators."""

    name = marshmallow.fields.String()
    email = marshmallow.fields.String()
    affiliation = marshmallow.fields.String()


class DatasetDetailsJson(marshmallow.Schema):
    """Serialize a dataset to a response object."""

    name = marshmallow.fields.String(required=True)
    version = marshmallow.fields.String(allow_none=True)
    created_at = marshmallow.fields.String(allow_none=True, attribute="date_created")

    title = marshmallow.fields.String()
    creators = marshmallow.fields.List(marshmallow.fields.Nested(DatasetCreatorsJson))
    description = marshmallow.fields.String()
    keywords = marshmallow.fields.List(marshmallow.fields.String())
    identifier = marshmallow.fields.String()
