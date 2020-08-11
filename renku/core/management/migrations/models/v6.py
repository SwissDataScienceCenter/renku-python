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
"""Migration models V6."""

import os

from marshmallow import EXCLUDE

from renku.core.models import jsonld
from renku.core.models.calamus import JsonLDSchema, fields, schema

from .v3 import Base, DatasetSchemaV3


class DatasetTag(Base):
    """DatasetTag migration model."""


class Language(Base):
    """Language migration model."""


class Url(Base):
    """Url migration model."""


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, client):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = DatasetSchemaV6(client=client).load(data)
        self.__reference__ = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        from renku.core.management import LocalClient

        data = DatasetSchemaV6().dump(self)
        path = path or self.__reference__ or os.path.join(self.path, LocalClient.METADATA)
        jsonld.write_yaml(path=path, data=data)


class LanguageSchemaV6(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class DatasetTagSchemaV6(JsonLDSchema):
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


class UrlSchemaV6(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    _id = fields.Id(missing=None)
    url = fields.Uri(schema.url, missing=None)


class DatasetSchemaV6(DatasetSchemaV3):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Dataset
        model = Dataset
        unknown = EXCLUDE

    in_language = fields.Nested(schema.inLanguage, LanguageSchemaV6, missing=None)
    keywords = fields.List(schema.keywords, fields.String())
    same_as = fields.Nested(schema.sameAs, UrlSchemaV6, missing=None)
    tags = fields.Nested(schema.subjectOf, DatasetTagSchemaV6, many=True)


def get_client_datasets(client):
    """Return Dataset migration models for a client."""
    paths = client.renku_datasets_path.rglob(client.METADATA)
    return [Dataset.from_yaml(path, client=client) for path in paths]
