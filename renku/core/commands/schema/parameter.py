# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Parameters JSON-LD schemas."""

from marshmallow import EXCLUDE

from renku.core.commands.schema.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.provenance.parameter import ParameterValue
from renku.core.models.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    CommandParameterBase,
    MappedIOStream,
    ParameterLink,
    ParameterMapping,
)


class ParameterValueSchema(JsonLDSchema):
    """ParameterValue schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.ParameterValue, schema.PropertyValue]
        model = ParameterValue
        unknown = EXCLUDE

    id = fields.Id()

    parameter = fields.IRI(schema.valueReference, attribute="parameter_id")
    value = fields.Raw(schema.value)


class MappedIOStreamSchema(JsonLDSchema):
    """MappedIOStream schema."""

    class Meta:
        """Meta class."""

        rdf_type = renku.IOStream
        model = MappedIOStream
        unknown = EXCLUDE

    id = fields.Id()
    stream_type = fields.String(renku.streamType)


class CommandParameterBaseSchema(JsonLDSchema):
    """CommandParameterBase schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandParameterBase, schema.Property]
        model = CommandParameterBase
        unknown = EXCLUDE

    default_value = fields.Raw(schema.defaultValue, missing=None)
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    name = fields.String(schema.name, missing=None)
    position = fields.Integer(renku.position, missing=None)
    prefix = fields.String(renku.prefix, missing=None)
    derived_from = fields.String(prov.wasDerivedFrom, missing=None)


class CommandParameterSchema(CommandParameterBaseSchema):
    """CommandParameter schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandParameter]
        model = CommandParameter
        unknown = EXCLUDE


class CommandInputSchema(CommandParameterBaseSchema):
    """CommandInput schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandInput]
        model = CommandInput
        unknown = EXCLUDE

    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, missing=None)
    encoding_format = fields.List(schema.encodingFormat, fields.String(), missing=None)


class CommandOutputSchema(CommandParameterBaseSchema):
    """CommandOutput schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandOutput]
        model = CommandOutput
        unknown = EXCLUDE

    create_folder = fields.Boolean(renku.createFolder, missing=False)
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, missing=None)
    encoding_format = fields.List(schema.encodingFormat, fields.String(), missing=None)


class ParameterMappingSchema(CommandParameterBaseSchema):
    """ParameterMapping schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.ParameterMapping]
        model = ParameterMapping
        unknown = EXCLUDE

    mapped_parameters = Nested(
        renku.mapsTo,
        ["ParameterMappingSchema", CommandInputSchema, CommandOutputSchema, CommandParameterSchema],
        many=True,
    )


class ParameterLinkSchema(JsonLDSchema):
    """ParameterLink schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.ParameterLink, schema.Property]
        model = ParameterLink
        unknown = EXCLUDE

    id = fields.Id()
    source = fields.Nested(renku.linkSource, [CommandOutputSchema])
    sinks = fields.Nested(renku.linkSink, [CommandInputSchema, CommandParameterSchema], many=True)
