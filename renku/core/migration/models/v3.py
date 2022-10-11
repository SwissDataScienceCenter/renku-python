# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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

from renku.command.schema.calamus import DateTimeList, JsonLDSchema, StringList, Uri, fields, prov, rdfs, renku, schema
from renku.core.migration.models.v9 import Person as OldPerson
from renku.core.migration.models.v9 import generate_project_id, wfprov
from renku.core.migration.utils import OLD_METADATA_PATH, generate_dataset_tag_id, generate_url_id, get_datasets_path
from renku.core.util import yaml
from renku.core.util.urls import get_host
from renku.domain_model.project_context import project_context


class Base:
    """Base class for migration models."""

    def __init__(self, **kwargs):
        """Initialize an instance."""
        kwargs.setdefault("_id", None)

        for k, v in kwargs.items():
            setattr(self, k, v)


class Person(Base):
    """Person migration model."""

    affiliation = None
    email = None
    name = None

    @classmethod
    def from_repository(cls, repository):
        """Create an instance from a repository."""
        user = repository.get_user()
        instance = cls(name=user.name, email=user.email)
        instance.fix_id()
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

    def fix_id(self):
        """Fixes the id of a Person if it is not set."""
        if not self._id or "mailto:None" in self._id or self._id.startswith("_:"):
            hostname = get_host()
            self._id = OldPerson.generate_id(email=self.email, full_identity=self.full_identity, hostname=hostname)


class Project(Base):
    """Project migration model."""

    agent_version = None

    @classmethod
    def from_yaml(cls, path):
        """Read content from YAML file."""
        data = yaml.read_yaml(path)
        self = ProjectSchemaV3().load(data)

        if not self.creator:
            self.creator = Person.from_repository(repository=project_context.repository)

        if not self.name:
            self.name = project_context.remote.name

        if not self._id or "NULL/NULL" in self._id:
            self._id = generate_project_id(name=self.name, creator=self.creator)

        return self

    def to_yaml(self, path):
        """Write content to a YAML file."""
        from renku import __version__

        self.agent_version = __version__

        data = ProjectSchemaV3().dump(self)
        yaml.write_yaml(path=path, data=data)


class Collection(Base):
    """Collection migration model."""

    def __init__(self, **kwargs):
        kwargs.setdefault("members", [])
        super().__init__(**kwargs)


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
            self._id = generate_dataset_tag_id(name=self.name, commit=self.commit)


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
            self._id = generate_url_id(url_str=self.url_str, url_id=self.url_id)


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, commit=None):
        """Read content from YAML file."""
        data = yaml.read_yaml(path)
        self = DatasetSchemaV3(commit=commit).load(data)
        self._metadata_path = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        data = DatasetSchemaV3().dump(self)
        path = path or self._metadata_path or os.path.join(self.path, OLD_METADATA_PATH)
        yaml.write_yaml(path=path, data=data)


class PersonSchemaV3(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    _id = fields.Id()
    name = StringList(schema.name)
    email = fields.String(schema.email, load_default=None)
    label = StringList(rdfs.label, load_default=None)
    affiliation = StringList(schema.affiliation, load_default=None)
    alternate_name = StringList(schema.alternateName, load_default=None)

    @post_load
    def make_instance(self, data, **kwargs):
        """Transform loaded dict into corresponding object."""
        instance = JsonLDSchema.make_instance(self, data, **kwargs)
        instance.fix_id()
        return instance


class ProjectSchemaV3(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Location, schema.Project]
        model = Project
        unknown = EXCLUDE

    _id = fields.Id(load_default=None)
    agent_version = fields.String(schema.agent, load_default="pre-0.11.0")
    name = fields.String(schema.name, load_default=None)
    created = DateTimeList(schema.dateCreated, load_default=None)
    version = StringList(schema.schemaVersion, load_default="1")
    creator = fields.Nested(schema.creator, PersonSchemaV3, load_default=None)


class CreatorMixinSchemaV3(JsonLDSchema):
    """CreatorMixin schema."""

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True)


class CommitMixinSchemaV3(JsonLDSchema):
    """CommitMixin schema."""

    _id = fields.Id(load_default=None)
    _label = fields.String(rdfs.label, load_default=None)
    _project = fields.Nested(schema.isPartOf, ProjectSchemaV3, load_default=None)
    path = fields.String(prov.atLocation, load_default=None)


class EntitySchemaV3(CommitMixinSchemaV3):
    """Entity Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, wfprov.Artifact]


class CollectionSchemaV3(EntitySchemaV3):
    """Collection Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Collection]
        model = Collection
        unknown = EXCLUDE

    members = fields.Nested(prov.hadMember, ["DatasetFileSchemaV3", "CollectionSchemaV3"], many=True)


class DatasetFileSchemaV3(EntitySchemaV3):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.DigitalDocument
        model = DatasetFile
        unknown = EXCLUDE

    added = fields.DateTime(schema.dateCreated)
    based_on = fields.Nested(schema.isBasedOn, "DatasetFileSchemaV3", load_default=None)
    name = fields.String(schema.name, load_default=None)
    url = fields.String(schema.url, load_default=None)
    external = fields.Boolean(renku.external, load_default=False)


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
    created = fields.DateTime(schema.startDate, load_default=None)
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

    _id = fields.Id(load_default=None)
    url = Uri(schema.url, load_default=None)


class DatasetSchemaV3(CreatorMixinSchemaV3, EntitySchemaV3):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    creators = fields.Nested(schema.creator, PersonSchemaV3, many=True, load_default=None)
    date_created = fields.DateTime(schema.dateCreated, load_default=None)
    date_published = fields.DateTime(schema.datePublished, load_default=None)
    description = fields.String(schema.description, load_default=None)
    files = fields.Nested(schema.hasPart, [DatasetFileSchemaV3, CollectionSchemaV3], many=True)
    identifier = fields.String(schema.identifier)
    in_language = fields.Nested(schema.inLanguage, LanguageSchemaV3, load_default=None)
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)
    license = Uri(schema.license, load_default=None, allow_none=True)
    name = fields.String(schema.alternateName, load_default=None)
    same_as = fields.Nested(schema.sameAs, UrlSchemaV3, load_default=None)
    tags = fields.Nested(schema.subjectOf, DatasetTagSchemaV3, many=True, load_default=None)
    title = fields.String(schema.name)
    url = fields.String(schema.url, load_default=None)
    version = fields.String(schema.version, load_default=None)

    @pre_load
    def fix_files_context(self, data, **kwargs):
        """Fix DatasetFile context for _label and external fields."""
        from renku.core.migration.utils import migrate_types

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


def get_project_datasets():
    """Return Dataset migration models for a project."""
    paths = get_datasets_path().rglob(OLD_METADATA_PATH)
    datasets = []
    for path in paths:
        dataset = Dataset.from_yaml(path=path)
        dataset.path = getattr(dataset, "path", None) or os.path.relpath(path.parent, project_context.path)
        datasets.append(dataset)

    return datasets
