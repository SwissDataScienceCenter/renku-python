# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
import pathlib
import uuid
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse

from git import GitCommandError
from marshmallow import EXCLUDE

from renku.core import errors
from renku.core.models.calamus import DateTimeList, JsonLDSchema, Nested, Uri, fields, prov, renku, schema
from renku.core.models.datasets import (
    DatasetFileSchema,
    DatasetTagSchema,
    ImageObjectSchema,
    LanguageSchema,
    Url,
    UrlSchema,
    generate_dataset_file_url,
    generate_dataset_id,
    is_dataset_name_valid,
)
from renku.core.models.entities import Entity, EntitySchema
from renku.core.models.projects import ProjectSchema
from renku.core.models.provenance.agents import PersonSchema
from renku.core.utils import communication
from renku.core.utils.urls import get_host


class DatasetFile:
    """Represent a file in a dataset."""

    def __init__(
        self,
        *,
        client=None,
        based_on=None,
        date_added=None,
        date_deleted=None,
        entity=None,
        id=None,
        is_external=False,
        path=None,
        source=None,
        url=None,
    ):
        if not path and not entity:
            raise errors.ParameterError("Entity or path must be set.")

        self._client = client

        self.based_on = based_on
        self.date_added = date_added or datetime.datetime.now(datetime.timezone.utc)
        self.date_deleted = date_deleted
        self.entity = entity or Entity.from_revision(client=client, path=path)
        self.id = id
        self.is_external = is_external
        self.source = source
        self.url = url

        self._update_metadata()

    @classmethod
    def from_dataset_file(cls, dataset_file, client, revision):
        """Create an instance by converting from renku.core.models.datasets.DatasetFile if available at revision."""
        path = dataset_file.path

        checksum = _get_object_hash(revision=revision, path=path, client=client)
        if not checksum:
            return None

        host = get_host(client)
        id = _generate_entity_id(entity_checksum=checksum, path=path, host=host)
        entity = Entity(id=id, checksum=checksum, path=path)

        return cls(
            client=client,
            based_on=dataset_file.based_on,
            date_added=dataset_file.added,
            entity=entity,
            id=None,
            is_external=dataset_file.external,
            source=dataset_file.source,
            url=None,
        )

    @staticmethod
    def generate_id(client, identifier, path):
        """Generate @id field."""
        host = get_host(client)

        identifier = quote(identifier, safe="")
        path = str(path).strip("/")

        return urljoin(f"https://{host}", pathlib.posixpath.join("dataset-files", identifier, path))

    @property
    def client(self):
        """Return client."""
        return self._client

    @client.setter
    def client(self, client):
        """Set client."""
        self._client = client
        self._update_metadata()

    @property
    def full_path(self):
        """Return full path in the current reference frame."""
        path = self.client.path / self.entity.path if self.client else self.entity.path
        return Path(os.path.abspath(path))

    def is_deleted(self):
        """Return true if dataset is deleted and should not be accessed."""
        return self.date_deleted is not None

    def _update_metadata(self):
        """Update relevant fields after setting a new client."""
        if not self._client:
            return

        identifier = self._extract_identifier() or str(uuid.uuid4())
        self.id = self.generate_id(self._client, identifier, self.entity.path)
        self.url = generate_dataset_file_url(client=self._client, filepath=self.entity.path)

    def _extract_identifier(self):
        if not self.id:
            return

        parsed_url = urlparse(self.id)
        return list(Path(parsed_url.path).parents)[-3].name

    def to_dataset_file(self, client, revision="HEAD"):
        """Return an instance of renku.core.models.datasets.DatasetFile at a revision."""
        from renku.core.models.datasets import DatasetFile

        try:
            return DatasetFile.from_revision(
                client=client,
                revision=revision,
                added=self.date_added,
                based_on=self.based_on,
                external=self.is_external,
                id=None,
                path=self.entity.path,
                source=self.source,
                url=None,
            )
        except KeyError:  # NOTE: cannot find a previous commit for path starting at revision
            return None


class Dataset:
    """Represent a dataset."""

    def __init__(
        self,
        name,
        *,
        client=None,
        creators=None,
        date_created=None,
        date_deleted=None,
        date_published=None,
        derived_from=None,
        description=None,
        files=None,
        id=None,
        identifier=None,
        images=[],
        immutable=False,
        in_language=None,
        keywords=None,
        license=None,
        original_identifier=None,
        project=None,
        same_as=None,
        tags=None,
        title=None,
        url=None,
        version=None,
    ):
        if not is_dataset_name_valid(name):
            raise errors.ParameterError(f"Invalid dataset name: {name}")

        self._client = client
        self.identifier = identifier or str(uuid.uuid4())
        self.id = id or generate_dataset_id(client=client, identifier=self.identifier)
        self.name = name

        self.creators = creators or []
        self.date_created = date_created or datetime.datetime.now(datetime.timezone.utc)
        self.date_deleted = date_deleted
        self.date_published = date_published
        self.derived_from = derived_from
        self.description = description
        self.files = files or []
        self.images = images
        self.immutable = immutable
        self.in_language = in_language
        self.keywords = keywords or []
        self.license = license
        self.original_identifier = original_identifier
        self.project = project
        self.same_as = same_as
        self.tags = tags or []
        self.title = title
        self.url = url
        self.version = version

        self._modified = False
        self._mutated = False
        self._metadata_path = False

        # if `date_published` is set, we are probably dealing with an imported dataset so `date_created` is not needed
        if self.date_published:
            self.date_created = None

        self._update_metadata()

    @classmethod
    def from_dataset(cls, dataset, client, revision):
        """Create an instance by converting from renku.core.models.datasets.Dataset."""
        files = cls._convert_from_dataset_files(dataset.files, client, revision)

        return cls(
            name=dataset.name,
            client=client,
            creators=dataset.creators,
            date_created=dataset.date_created,
            date_deleted=None,
            date_published=dataset.date_published,
            derived_from=dataset.derived_from,
            description=dataset.description,
            files=files,
            id=None,
            identifier=dataset.identifier,
            images=dataset.images,
            in_language=dataset.in_language,
            keywords=dataset.keywords,
            license=dataset.license,
            original_identifier=dataset.original_identifier,
            same_as=dataset.same_as,
            tags=dataset.tags,
            title=dataset.title,
            url=dataset.url,
            version=dataset.version,
        )

    @staticmethod
    def _convert_from_dataset_files(files, client, revision):
        """Create instances from renku.core.models.datasets.DatasetFile."""
        dataset_files = []
        files = {f.path: f for f in files}  # NOTE: To make sure there are no duplicate paths
        for path in files:
            file = files[path]
            dataset_file = DatasetFile.from_dataset_file(file, client=client, revision=revision)
            if not dataset_file:
                continue

            dataset_files.append(dataset_file)

        return dataset_files

    @property
    def client(self):
        """Return client."""
        return self._client

    @client.setter
    def client(self, client):
        """Set client."""
        self._client = client
        self._update_metadata()

    def is_deleted(self):
        """Return true if dataset is deleted and should not be accessed."""
        return self.date_deleted is not None

    def find_file(self, path, return_index=False):
        """Find a file in files container using its relative path."""
        path = str(path)
        for index, file_ in enumerate(self.files):
            if file_.entity.path == path:
                if return_index:
                    return index
                return file_

    def _set_identifier(self, new_identifier):
        """Set identifier and update all related fields."""
        self.identifier = new_identifier
        self.id = generate_dataset_id(client=self._client, identifier=self.identifier)
        self.url = self.id

    def _update_metadata(self):
        """Update relevant fields after setting a new client."""
        if not self._client:
            return

        self._set_identifier(self.identifier)

        self.project = self._client.project

        if self.derived_from:
            host = get_host(self._client)
            derived_from_id = self.derived_from._id
            derived_from_url = self.derived_from.url.get("@id")
            u = urlparse(derived_from_url)
            derived_from_url = u._replace(netloc=host).geturl()
            self.derived_from = Url(id=derived_from_id, url_id=derived_from_url)

        for file_ in self.files:
            file_.client = self._client

    def update_from(self, dataset, client, revision, date):
        """Update metadata from a new version of the dataset."""
        assert (
            self.identifier == dataset.identifier
        ), f"Dataset is being updated with a different identifier `{dataset.identifier}`"

        self._update_files(dataset, client, revision, date)

        self.creators = dataset.creators
        self.date_created = dataset.date_created
        self.date_deleted = None
        self.date_published = dataset.date_published
        self.derived_from = dataset.derived_from
        self.description = dataset.description
        self.images = dataset.images
        self.in_language = dataset.in_language
        self.keywords = dataset.keywords
        self.license = dataset.license
        self.same_as = dataset.same_as
        self.tags = dataset.tags
        self.title = dataset.title
        self.version = dataset.version

    def _update_files(self, dataset, client, revision, date):
        current_files = {f.entity.path: f for f in self.files if not f.is_deleted()}
        updated_files = {f.path: f for f in dataset.files}

        current_paths = set(current_files.keys())
        updated_paths = set(updated_files.keys())

        deleted_paths = current_paths - updated_paths
        for path in deleted_paths:
            file_ = current_files[path]
            file_.date_deleted = date

        new_paths = updated_paths - current_paths
        if not new_paths:
            return

        new_files = [v for k, v in updated_files.items() if k in new_paths]
        dataset_files = self._convert_from_dataset_files(new_files, client, revision)
        self.files.extend(dataset_files)

    def to_dataset(self, client):
        """Return an instance of renku.core.models.datasets.Dataset."""
        from renku.core.models.datasets import Dataset

        files = self._convert_to_dataset_files(client)

        return Dataset(
            name=self.name,
            client=client,
            creators=self.creators,
            date_created=self.date_created,
            date_published=self.date_published,
            derived_from=self.derived_from,
            description=self.description,
            files=files,
            id=self.id,
            identifier=self.identifier,
            in_language=self.in_language,
            keywords=self.keywords,
            license=self.license,
            same_as=self.same_as,
            tags=self.tags,
            title=self.title,
            url=self.url,
            version=self.version,
        )

    def _convert_to_dataset_files(self, client):
        """Create instances of renku.core.models.datasets.DatasetFile."""
        dataset_files = []
        for file in self.files:
            dataset_file = file.to_dataset_file(client)
            if not dataset_file:
                continue

            dataset_files.append(dataset_file)

        return dataset_files


def _generate_entity_id(entity_checksum, path, host):
    quoted_path = quote(path)
    path = pathlib.posixpath.join("blob", entity_checksum, quoted_path)

    return urljoin(f"https://{host}", path)


def _get_object_hash(revision, path, client):
    try:
        return client.repo.git.rev_parse(f"{revision}:{str(path)}")
    except GitCommandError:
        return None


class DatasetProvenance:
    """A set of datasets."""

    def __init__(self, datasets=None):
        """Initialize."""
        self._datasets = datasets or []
        self._path = None

    def add(self, dataset):
        """Add a Dataset."""
        self._datasets.append(dataset)

    def get(self, identifier):
        """Return a dataset by its original identifier."""
        datasets = (d for d in self._datasets if d.identifier == identifier)
        dataset = next(datasets, None)
        assert next(datasets, None) is None, f"Found more than one dataset with identifier `{identifier}`."
        return dataset

    def get_by_name(self, name):
        """Return a generator that yields datasets by name."""
        return (d for d in self._datasets if d.name == name)

    def get_latest_by_name(self, name):
        """Return the latest version of a dataset."""
        datasets = {d.id: d for d in self.get_by_name(name)}

        for dataset in list(datasets.values()):
            if dataset.derived_from:
                datasets.pop(dataset.derived_from.url_id, None)

        assert len(datasets) <= 1, f"There are more than one latest versions with name `{name}`"

        if not datasets:
            return None

        _, dataset = datasets.popitem()
        return dataset

    @property
    def datasets(self):
        """Return list of datasets."""
        return self._datasets

    def update_dataset(self, dataset, client, revision=None, date=None):
        """Add/update a dataset according to its new content."""
        revision = revision or "HEAD"
        date = date or datetime.datetime.now(datetime.timezone.utc)

        current_dataset = self.get(dataset.identifier)

        if not current_dataset:
            current_dataset = Dataset.from_dataset(dataset, client, revision)
            self.add(current_dataset)
            return

        if current_dataset.is_deleted():
            communication.warn(f"Deleted dataset is being updated `{dataset.identifier}` at revision `{revision}`")
            current_dataset.date_deleted = None

        current_dataset.update_from(dataset, client, revision, date)

    def remove_dataset(self, dataset, client, revision=None, date=None):
        """Remove a dataset."""
        revision = revision or "HEAD"
        date = date or datetime.datetime.now(datetime.timezone.utc)
        current_dataset = self.get(dataset.identifier)
        if not current_dataset:
            current_dataset = Dataset.from_dataset(dataset, client, revision)
            self.add(current_dataset)
        assert not current_dataset.is_deleted(), f"Dataset `{current_dataset.name}` was deleted before."
        current_dataset.date_deleted = date

    @classmethod
    def from_json(cls, path):
        """Return an instance from a file."""
        if Path(path).exists():
            with open(path) as file_:
                data = json.load(file_)
                self = cls.from_jsonld(data=data) if data else DatasetProvenance()
        else:
            self = DatasetProvenance()

        self._path = path

        return self

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        elif not isinstance(data, list):
            raise ValueError(data)

        return DatasetProvenanceSchema(flattened=True).load(data)

    def to_json(self, path=None):
        """Write to file."""
        path = path or self._path
        data = self.to_jsonld()
        with open(path, "w", encoding="utf-8") as file_:
            json.dump(data, file_, ensure_ascii=False, sort_keys=True, indent=2)

    def to_jsonld(self):
        """Create JSON-LD."""
        return DatasetProvenanceSchema(flattened=True).dump(self)


class NewDatasetFileSchema(JsonLDSchema):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.DigitalDocument]
        model = DatasetFile
        unknown = EXCLUDE

    based_on = Nested(schema.isBasedOn, DatasetFileSchema, missing=None, propagate_client=False)
    date_added = DateTimeList(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
    date_deleted = fields.DateTime(prov.invalidatedAtTime, missing=None, allow_none=True, format="iso")
    entity = Nested(prov.entity, EntitySchema)
    id = fields.Id()
    is_external = fields.Boolean(renku.external, missing=False)
    source = fields.String(renku.source, missing=None)
    url = fields.String(schema.url, missing=None)


class NewDatasetSchema(JsonLDSchema):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.Dataset]
        model = Dataset
        unknown = EXCLUDE

    creators = Nested(schema.creator, PersonSchema, many=True)
    date_created = fields.DateTime(
        schema.dateCreated, missing=None, allow_none=True, format="iso", extra_formats=("%Y-%m-%d",),
    )
    date_deleted = fields.DateTime(prov.invalidatedAtTime, missing=None, allow_none=True, format="iso")
    date_published = fields.DateTime(
        schema.datePublished,
        missing=None,
        allow_none=True,
        format="%Y-%m-%d",
        extra_formats=("iso", "%Y-%m-%dT%H:%M:%S"),
    )
    derived_from = Nested(prov.wasDerivedFrom, UrlSchema, missing=None)
    description = fields.String(schema.description, missing=None)
    files = Nested(schema.hasPart, NewDatasetFileSchema, many=True)
    id = fields.Id(missing=None)
    identifier = fields.String(schema.identifier)
    images = fields.Nested(schema.image, ImageObjectSchema, missing=None, allow_none=True)
    in_language = Nested(schema.inLanguage, LanguageSchema, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None, allow_none=True)
    license = Uri(schema.license, missing=None, allow_none=True)
    name = fields.String(schema.alternateName)
    original_identifier = fields.String(renku.originalIdentifier)
    project = Nested(schema.isPartOf, ProjectSchema, missing=None)
    same_as = Nested(schema.sameAs, UrlSchema, missing=None)
    tags = Nested(schema.subjectOf, DatasetTagSchema, many=True)
    title = fields.String(schema.name)
    url = fields.String(schema.url)
    version = fields.String(schema.version, missing=None)


class DatasetProvenanceSchema(JsonLDSchema):
    """DatasetProvenance schema."""

    class Meta:
        """Meta class."""

        rdf_type = renku.DatasetProvenance
        model = DatasetProvenance
        unknown = EXCLUDE

    _datasets = Nested(schema.hasPart, NewDatasetSchema, init_name="datasets", many=True, missing=None)
