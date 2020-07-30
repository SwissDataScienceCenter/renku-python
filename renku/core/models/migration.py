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
"""Model used for migrations."""

import os

from renku.core.models.git import get_user_info
from renku.core.models.jsonld import write_yaml
from renku.core.models.projects import generate_project_id
from renku.core.utils.migrate import read_jsonld_yaml


def _to_dict(value):
    if isinstance(value, Base):
        return value.metadata
    if isinstance(value, dict):
        return value
    raise ValueError


def _DatasetFile_to_dict(value):
    from renku.core.models.datasets import DatasetFile

    if isinstance(value, DatasetFile):
        return value.as_jsonld()
    if isinstance(value, dict):
        return value
    raise ValueError


class Base:
    """Base class for migration models."""

    def __init__(self, metadata):
        """Initialize an instance."""
        self.metadata = metadata
        self.metadata['@type'] = self.type

    def to_yaml(self, path):
        """Write content to a YAML file."""
        write_yaml(path=path, data=self.metadata)

    def __getattr__(self, name):
        """Return metadata attributes."""
        attr = self.meta_attributes.get(name)
        if not attr:
            return super().__getattribute__(name)

        return self.metadata.get(attr[0])

    def __setattr__(self, name, value):
        """Set metadata or normal attributes."""
        attr = self.meta_attributes.get(name)
        if not attr:
            return super().__setattr__(name, value)
        name, converter = attr
        if converter:
            value = converter(value)
        self.metadata[name] = value


class Person(Base):
    """Person migration model."""

    type = [
        'http://schema.org/Person',
        'http://www.w3.org/ns/prov#Person',
    ]
    meta_attributes = {
        'name': ('http://schema.org/name', None),
        'email': ('http://schema.org/email', None),
    }

    @classmethod
    def from_git(cls, git):
        """Create an instance from a Git repo."""
        name, email = get_user_info(git)
        self = cls({})
        self.name = name
        self.email = email

        return self


class Project(Base):
    """Project migration model."""

    type = [
        'http://schema.org/Project',
        'http://www.w3.org/ns/prov#Location',
    ]
    meta_attributes = {
        '_id': ('@id', None),
        'creator': ('http://schema.org/creator', _to_dict),
        'name': ('http://schema.org/name', None),
    }

    @classmethod
    def from_yaml(cls, path, client):
        """Read content from YAML file."""
        metadata = read_jsonld_yaml(path)
        return cls(metadata, client)

    def __init__(self, metadata, client):
        """Initialize an instance."""
        super().__init__(metadata)

        if not self.creator:
            self.creator = Person.from_git(client.repo)

        creator = Person(self.creator)

        if not self._id or 'NULL/NULL' in self._id:
            self._id = generate_project_id(
                client=client, name=self.name, creator=creator
            )


class DatasetFile(Base):
    """DatasetFile migration model."""

    type = [
        'http://purl.org/wf4ever/wfprov#Artifact',
        'http://schema.org/DigitalDocument',
        'http://www.w3.org/ns/prov#Entity',
    ]
    meta_attributes = {
        '_id': ('@id', None),
        '_label': ('http://www.w3.org/2000/01/rdf-schema#label', None),
        '_project': ('http://schema.org/isPartOf', _to_dict),
        'based_on': ('http://schema.org/isBasedOn', _DatasetFile_to_dict),
        'name': ('http://schema.org/name', None),
        'path': ('http://www.w3.org/ns/prov#atLocation', str),
        'url': ('http://schema.org/url', None),
    }


class Dataset(Base):
    """Dataset migration model."""

    type = [
        'http://purl.org/wf4ever/wfprov#Artifact',
        'http://schema.org/Dataset',
        'http://www.w3.org/ns/prov#Entity',
    ]
    meta_attributes = {
        '_id': ('@id', None),
        '_label': ('http://www.w3.org/2000/01/rdf-schema#label', None),
        '_project': ('http://schema.org/isPartOf', _to_dict),
        'creators': ('http://schema.org/creator', list),
        'identifier': ('http://schema.org/identifier', None),
        'name': ('http://schema.org/alternateName', None),
        'path': ('http://www.w3.org/ns/prov#atLocation', str),
        'title': ('http://schema.org/name', None),
        'uid': ('http://schema.org/identifier', None),
        'url': ('http://schema.org/url', None),
    }

    @classmethod
    def from_yaml(cls, path):
        """Read content from YAML file."""
        metadata = read_jsonld_yaml(path)
        cls._fix_context_and_types(metadata)
        return cls(metadata)

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        from renku.core.management import LocalClient
        path = path or os.path.join(self.path, LocalClient.METADATA)
        super().to_yaml(path=path)

    @property
    def files(self):
        """Dataset files."""
        files = self.metadata.get('http://schema.org/hasPart', [])
        return [DatasetFile(file_) for file_ in files]

    @staticmethod
    def _fix_context_and_types(metadata):
        """Fix DatasetFile context for _label and external fields."""
        from renku.core.utils.migrate import migrate_types

        if '@context' in metadata:
            context = metadata['@context']
            if isinstance(context, dict) and 'files' in context:
                context.setdefault(
                    'rdfs', 'http://www.w3.org/2000/01/rdf-schema#'
                )

                files = metadata['@context']['files']
                if isinstance(files, dict) and '@context' not in files:
                    context = files['@context']
                    context.setdefault(
                        'rdfs', 'http://www.w3.org/2000/01/rdf-schema#'
                    )
                    context.setdefault('_label', 'rdfs:label')
                    context.setdefault('external', 'renku:external')
                    context.setdefault(
                        'renku',
                        'https://swissdatasciencecenter.github.io/renku-ontology#'
                    )

        migrate_types(metadata)
