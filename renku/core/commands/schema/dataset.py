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
"""Datasets JSON-LD schemas."""

from marshmallow import EXCLUDE

from renku.core.commands.schema.agent import PersonSchema
from renku.core.commands.schema.annotation import AnnotationSchema
from renku.core.commands.schema.calamus import DateTimeList, JsonLDSchema, Nested, Uri, fields, oa, prov, renku, schema
from renku.core.commands.schema.entity import CollectionSchema, EntitySchema
from renku.core.models.dataset import Dataset, DatasetFile, DatasetTag, ImageObject, Language, RemoteEntity, Url


def dump_dataset_as_jsonld(dataset: Dataset) -> dict:
    """Return JSON-LD representation of a dataset."""
    return DatasetSchema(flattened=True).dump(dataset)


class UrlSchema(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    id = fields.Id(missing=None)
    url = Uri(schema.url, missing=None)


class DatasetTagSchema(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    dataset_id = Nested(schema.about, UrlSchema, missing=None)
    date_created = fields.DateTime(schema.startDate, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    name = fields.String(schema.name)


class LanguageSchema(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class ImageObjectSchema(JsonLDSchema):
    """ImageObject schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.ImageObject
        model = ImageObject
        unknown = EXCLUDE

    content_url = fields.String(schema.contentUrl)
    id = fields.Id(missing=None)
    position = fields.Integer(schema.position)


class RemoteEntitySchema(JsonLDSchema):
    """RemoteEntity schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity]
        model = RemoteEntity
        unknown = EXCLUDE

    checksum = fields.String(renku.checksum)
    id = fields.Id()
    path = fields.String(prov.atLocation)
    url = fields.String(schema.url)


class DatasetFileSchema(JsonLDSchema):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.DigitalDocument]
        model = DatasetFile
        unknown = EXCLUDE

    based_on = Nested(schema.isBasedOn, RemoteEntitySchema, missing=None)
    date_added = DateTimeList(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
    date_removed = fields.DateTime(prov.invalidatedAtTime, missing=None, format="iso")
    entity = Nested(prov.entity, [EntitySchema, CollectionSchema])
    id = fields.Id()
    is_external = fields.Boolean(renku.external, missing=False)
    source = fields.String(renku.source, missing=None)


class DatasetSchema(JsonLDSchema):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.Dataset]
        model = Dataset
        unknown = EXCLUDE

    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    creators = Nested(schema.creator, PersonSchema, many=True)
    date_created = fields.DateTime(schema.dateCreated, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    date_removed = fields.DateTime(prov.invalidatedAtTime, missing=None, format="iso")
    date_published = fields.DateTime(
        schema.datePublished, missing=None, format="%Y-%m-%d", extra_formats=("iso", "%Y-%m-%dT%H:%M:%S")
    )
    derived_from = Nested(prov.wasDerivedFrom, UrlSchema, missing=None)
    description = fields.String(schema.description, missing=None)
    dataset_files = Nested(schema.hasPart, DatasetFileSchema, many=True)
    id = fields.Id(missing=None)
    identifier = fields.String(schema.identifier)
    images = fields.Nested(schema.image, ImageObjectSchema, missing=None, many=True)
    in_language = Nested(schema.inLanguage, LanguageSchema, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    license = Uri(schema.license, missing=None)
    name = fields.String(renku.slug)
    initial_identifier = fields.String(renku.originalIdentifier)
    project_id = fields.IRI(renku.hasDataset, reverse=True)
    same_as = Nested(schema.sameAs, UrlSchema, missing=None)
    title = fields.String(schema.name)
    version = fields.String(schema.version, missing=None)
