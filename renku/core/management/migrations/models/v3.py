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
"""Migration models V3."""

import os

from marshmallow import EXCLUDE, post_load, pre_load

from renku.core.models import jsonld
from renku.core.models.calamus import (
    DateTimeList,
    JsonLDSchema,
    StringList,
    Uri,
    fields,
    prov,
    rdfs,
    renku,
    schema,
    wfprov,
)
from renku.core.models.datasets import generate_dataset_tag_id, generate_url_id
from renku.core.models.git import get_user_info
from renku.core.models.projects import generate_project_id
from renku.core.models.provenance.agents import generate_person_id


class Base:
    """Base class for migration models."""

    def __init__(self, **kwargs):
        """Initialize an instance."""
        self.client = None

        kwargs.setdefault("_id", None)

        for k, v in kwargs.items():
            setattr(self, k, v)


class Person(Base):
    """Person migration model."""

    affiliation = None
    email = None
    name = None

    @staticmethod
    def _fix_person_id(person, client=None):
        """Fixes the id of a Person if it is not set."""
        if not person._id or "mailto:None" in person._id or person._id.startswith("_:"):
            if not client and person.client:
                client = person.client
            person._id = generate_person_id(client=client, email=person.email, full_identity=person.full_identity)

        return person

    @classmethod
    def from_git(cls, git, client=None):
        """Create an instance from a Git repo."""
        name, email = get_user_info(git)
        instance = cls(name=name, email=email)

        instance = Person._fix_person_id(instance, client)

        return instance

    def __init__(self, **kwargs):
        """Initialize an instance."""
        kwargs.setdefault("_id", None)
        super().__init__(**kwargs)

    @property
    def full_identity(self):
        """Return name, email, and affiliation."""
        email = f" <{self.email}>" if self.email else ""
        affiliation = f" [{self.affiliation}]" if self.affiliation else ""
        return f"{self.name}{email}{affiliation}"


class Project(Base):
    """Project migration model."""

    agent_version = None

    @classmethod
    def from_yaml(cls, path, client):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = ProjectSchemaV3().load(data)

        if not self.creator:
            self.creator = Person.from_git(client.repo)

        if not self.name:
            self.name = client.remote.get("name")

        if not self._id or "NULL/NULL" in self._id:
            self._id = generate_project_id(client=client, name=self.name, creator=self.creator)

        return self

    def to_yaml(self, path):
        """Write content to a YAML file."""
        from renku import __version__

        self.agent_version = __version__

        data = ProjectSchemaV3().dump(self)
        jsonld.write_yaml(path=path, data=data)


class DatasetFile(Base):
    """DatasetFile migration model."""


class DatasetTag(Base):
    """DatasetTag migration model."""

    commit = None
    name = None

    def __init__(self, **kwargs):
        """Initialize an instance."""
        super().__init__(**kwargs)

        if not self._id or self._id.startswith("_:"):
            self._id = generate_dataset_tag_id(client=self.client, name=self.name, commit=self.commit)


class Language(Base):
    """Language migration model."""


class Url(Base):
    """Url migration model."""

    url = None
    url_id = None
    url_str = None

    def __init__(self, **kwargs):
        """Initialize an instance."""
        super().__init__(**kwargs)

        if isinstance(self.url, dict):
            self.url_id = self.url["@id"]
        elif isinstance(self.url, str):
            self.url_str = self.url

        if not self._id or self._id.startswith("_:"):
            self._id = generate_url_id(client=self.client, url_str=self.url_str, url_id=self.url_id)


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = DatasetSchemaV3(client=client, commit=commit).load(data)
        self._metadata_path = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        from renku.core.management import LocalClient

        data = DatasetSchemaV3().dump(self)
        path = path or self._metadata_path or os.path.join(self.path, LocalClient.METADATA)
        jsonld.write_yaml(path=path, data=data)


class PersonSchemaV3(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    _id = fields.Id()
    name = StringList(schema.name)
    email = fields.String(schema.email, missing=None)
    label = StringList(rdfs.label, missing=None)
    affiliation = StringList(schema.affiliation, missing=None)
    alternate_name = StringList(schema.alternateName, missing=None)

    @post_load
    def make_instance(self, data, **kwargs):
        """Transform loaded dict into corresponding object."""
        instance = JsonLDSchema.make_instance(self, data, **kwargs)

        instance = Person._fix_person_id(instance)
        return instance


class ProjectSchemaV3(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Location, schema.Project]
        model = Project
        unknown = EXCLUDE

    _id = fields.Id(missing=None)
    agent_version = fields.String(schema.agent, missing="pre-0.11.0")
    name = fields.String(schema.name, missing=None)
    created = DateTimeList(schema.dateCreated, missing=None)
    version = StringList(schema.schemaVersion, missing="1")
    creator = fields.Nested(schema.creator, PersonSchemaV3, missing=None)


class CreatorMixinSchemaV3(JsonLDSchema):
    """CreatorMixin schema."""

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True)


class CommitMixinSchemaV3(JsonLDSchema):
    """CommitMixin schema."""

    _id = fields.Id(missing=None)
    _label = fields.String(rdfs.label, missing=None)
    _project = fields.Nested(schema.isPartOf, ProjectSchemaV3, missing=None)
    path = fields.String(prov.atLocation, missing=None)


class EntitySchemaV3(CommitMixinSchemaV3):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, wfprov.Artifact]


class DatasetFileSchemaV3(EntitySchemaV3):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.DigitalDocument
        model = DatasetFile
        unknown = EXCLUDE

    added = fields.DateTime(schema.dateCreated)
    based_on = fields.Nested(schema.isBasedOn, "DatasetFileSchemaV3", missing=None)
    name = fields.String(schema.name, missing=None)
    url = fields.String(schema.url, missing=None)
    external = fields.Boolean(renku.external, missing=False)


class LanguageSchemaV3(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class DatasetTagSchemaV3(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    _id = fields.Id()
    commit = fields.String(schema.location)
    created = fields.DateTime(schema.startDate, missing=None)
    dataset = fields.String(schema.about)
    description = fields.String(schema.description)
    name = fields.String(schema.name)


class UrlSchemaV3(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    _id = fields.Id(missing=None)
    url = Uri(schema.url, missing=None)


class DatasetSchemaV3(CreatorMixinSchemaV3, EntitySchemaV3):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True, missing=None)
    date_created = fields.DateTime(schema.dateCreated, missing=None)
    date_published = fields.DateTime(schema.datePublished, missing=None)
    description = fields.String(schema.description, missing=None)
    files = fields.Nested(schema.hasPart, DatasetFileSchemaV3, many=True)
    identifier = fields.String(schema.identifier)
    in_language = fields.Nested(schema.inLanguage, LanguageSchemaV3, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    license = Uri(schema.license, missing=None, allow_none=True)
    name = fields.String(schema.alternateName, missing=None)
    same_as = fields.Nested(schema.sameAs, UrlSchemaV3, missing=None)
    tags = fields.Nested(schema.subjectOf, DatasetTagSchemaV3, many=True, missing=None)
    title = fields.String(schema.name)
    url = fields.String(schema.url, missing=None)
    version = fields.String(schema.version, missing=None)

    @pre_load
    def fix_files_context(self, data, **kwargs):
        """Fix DatasetFile context for _label and external fields."""
        from renku.core.utils.migrate import migrate_types

        data = migrate_types(data)

        if "@context" not in data:
            return data

        context = data["@context"]
        if not isinstance(context, dict) or "files" not in context:
            return data

        context.setdefault("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        context.setdefault("_label", "rdfs:label")

        files = data["@context"]["files"]
        if not isinstance(files, dict) or "@context" not in files:
            return data

        context = files["@context"]
        context.setdefault("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        context.setdefault("_label", "rdfs:label")
        context.setdefault("external", "renku:external")
        context.setdefault("renku", "https://swissdatasciencecenter.github.io/renku-ontology#")

        return data


def get_client_datasets(client):
    """Return Dataset migration models for a client."""
    paths = client.renku_datasets_path.rglob(client.METADATA)
    datasets = []
    for path in paths:
        dataset = Dataset.from_yaml(path=path, client=client)
        dataset.path = getattr(dataset, "path", None) or os.path.relpath(path.parent, client.path)
        datasets.append(dataset)

    return datasets
