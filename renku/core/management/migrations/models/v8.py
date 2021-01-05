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
"""Migration models V8."""

import os
from pathlib import Path

from marshmallow import EXCLUDE, pre_dump

from renku.core.models import jsonld
from renku.core.models.calamus import Uri, fields, schema
from renku.core.models.entities import generate_file_id

from .v3 import CreatorMixinSchemaV3, DatasetTagSchemaV3, EntitySchemaV3, LanguageSchemaV3, PersonSchemaV3, UrlSchemaV3
from .v7 import Base, DatasetFileSchemaV7


class DatasetFile(Base):
    """DatasetFile migration model."""

    def __init__(self, **kwargs):
        """Initialize an instance."""
        super().__init__(**kwargs)

        if hasattr(self, "path") and (not self._id or self._id.startswith("_:")):
            hexsha = "UNCOMMITTED"
            if self.client and Path(self.path).exists():
                hexsha = self.client.find_previous_commit().hexsha

            self._id = generate_file_id(client=self.client, hexsha=hexsha, path=self.path)


class Dataset(Base):
    """Dataset migration model."""

    @classmethod
    def from_yaml(cls, path, client=None, commit=None):
        """Read content from YAML file."""
        data = jsonld.read_yaml(path)
        self = DatasetSchemaV8(client=client, commit=commit, flattened=True).load(data)
        self._metadata_path = path
        return self

    def to_yaml(self, path=None):
        """Write content to a YAML file."""
        from renku.core.management import LocalClient

        for file_ in self.files:
            file_._project = self._project

        data = DatasetSchemaV8(flattened=True).dump(self)
        path = path or self._metadata_path or os.path.join(self.path, LocalClient.METADATA)
        jsonld.write_yaml(path=path, data=data)


class DatasetFileSchemaV8(DatasetFileSchemaV7):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.DigitalDocument
        model = DatasetFile
        unknown = EXCLUDE


class DatasetSchemaV8(CreatorMixinSchemaV3, EntitySchemaV3):
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
    files = fields.Nested(schema.hasPart, DatasetFileSchemaV8, many=True)
    identifier = fields.String(schema.identifier)
    in_language = fields.Nested(schema.inLanguage, LanguageSchemaV3, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    license = Uri(schema.license, allow_none=True, missing=None)
    name = fields.String(schema.alternateName, missing=None)
    same_as = fields.Nested(schema.sameAs, UrlSchemaV3, missing=None)
    tags = fields.Nested(schema.subjectOf, DatasetTagSchemaV3, many=True, missing=None)
    title = fields.String(schema.name)
    url = fields.String(schema.url, missing=None)
    version = fields.String(schema.version, missing=None)

    @pre_dump
    def fix_license(self, data, **kwargs):
        """Fix license to be a string."""
        if isinstance(data.license, dict):
            data.license = data.license.get("http://schema.org/url", "")

        return data


def get_client_datasets(client):
    """Return Dataset migration models for a client."""
    paths = client.renku_datasets_path.rglob(client.METADATA)
    return [Dataset.from_yaml(path=path, client=client) for path in paths]
