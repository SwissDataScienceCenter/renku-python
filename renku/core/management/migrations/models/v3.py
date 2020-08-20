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
"""Migration models V3."""

import os

from marshmallow import EXCLUDE, post_load, pre_load

from renku.core.models import jsonld
from renku.core.models.calamus import JsonLDSchema, fields, prov, rdfs, renku, schema, wfprov
from renku.core.models.git import get_user_info
from renku.core.models.projects import generate_project_id
from renku.core.models.provenance.agents import generate_person_id


class Base:
    """Base class for migration models."""

    def __init__(self, **kwargs):
        """Initialize an instance."""
        for k, v in kwargs.items():
            setattr(self, k, v)


class Person(Base):
    """Person migration model."""

    client = None

    @staticmethod
    def _fix_person_id(person, client=None):
        """Fixes the id of a Person if it is not set."""
        if not person._id or "mailto:None" in person._id:
            if not client and person.client:
                client = person.client
            person._id = generate_person_id(email=person.email, client=client)

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


class Project(Base):
    """Project migration model."""

    @classmethod
    def from_yaml(cls, path, client):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = ProjectSchemaV3().load(data)

        if not self.creator:
            self.creator = Person.from_git(client.repo)

        if not self._id or "NULL/NULL" in self._id:
            self._id = generate_project_id(client=client, name=self.name, creator=self.creator)

        return self

    def to_yaml(self, path):
        """Write content to a YAML file."""
        data = ProjectSchemaV3().dump(self)
        jsonld.write_yaml(path=path, data=data)


class DatasetFile(Base):
    """DatasetFile migration model."""


class DatasetTag(Base):
    """DatasetTag migration model."""


class Language(Base):
    """Language migration model."""


class Url(Base):
    """Url migration model."""


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = DatasetSchemaV3(client=client, commit=commit).load(data)
        self.__reference__ = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        from renku.core.management import LocalClient

        data = DatasetSchemaV3().dump(self)
        path = path or self.__reference__ or os.path.join(self.path, LocalClient.METADATA)
        jsonld.write_yaml(path=path, data=data)


class PersonSchemaV3(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    _id = fields.Id()
    name = fields.String(schema.name)
    email = fields.String(schema.email, missing=None)
    label = fields.String(rdfs.label)
    affiliation = fields.String(schema.affiliation, missing=None)
    alternate_name = fields.String(schema.alternateName, missing=None)

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
    name = fields.String(schema.name, missing=None)
    created = fields.DateTime(schema.dateCreated, missing=None)
    updated = fields.DateTime(schema.dateUpdated, missing=None)
    version = fields.String(schema.schemaVersion, missing=1)
    creator = fields.Nested(schema.creator, PersonSchemaV3, missing=None)


class CreatorMixinSchemaV3(JsonLDSchema):
    """CreatorMixin schema."""

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True)


class CommitMixinSchemaV3(JsonLDSchema):
    """CommitMixin schema."""

    _id = fields.Id(missing=None)
    _label = fields.String(rdfs.label, missing=None)
    _project = fields.Nested(schema.isPartOf, ProjectSchemaV3, missing=None)
    path = fields.String(prov.atLocation)


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


class LanguageSchemaV5(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class DatasetTagSchemaV5(JsonLDSchema):
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


class UrlSchemaV5(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    _id = fields.Id(missing=None)
    url = fields.Uri(schema.url, missing=None)


class DatasetSchemaV3(CreatorMixinSchemaV3, EntitySchemaV3):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True)
    date_created = fields.DateTime(schema.dateCreated, missing=None)
    date_published = fields.DateTime(schema.datePublished, missing=None)
    description = fields.String(schema.description, missing=None)
    files = fields.Nested(schema.hasPart, DatasetFileSchemaV3, many=True)
    identifier = fields.String(schema.identifier)
    in_language = fields.Nested(schema.inLanguage, LanguageSchemaV5, missing=None)
    keywords = fields.List(schema.keywords, fields.String())
    license = fields.Uri(schema.license, missing=None, allow_none=True)
    name = fields.String(schema.alternateName, missing=None)
    same_as = fields.Nested(schema.sameAs, UrlSchemaV5, missing=None)
    tags = fields.Nested(schema.subjectOf, DatasetTagSchemaV5, many=True)
    title = fields.String(schema.name)
    url = fields.String(schema.url)
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
    return [Dataset.from_yaml(path=path, client=client) for path in paths]
