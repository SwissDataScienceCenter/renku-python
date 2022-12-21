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
"""Project JSON-LD schema."""

from marshmallow import EXCLUDE

from renku.command.schema.agent import PersonSchema
from renku.command.schema.annotation import AnnotationSchema
from renku.command.schema.calamus import DateTimeList, JsonLDSchema, Nested, StringList, fields, oa, prov, renku, schema
from renku.domain_model.project import Project


class ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = Project
        unknown = EXCLUDE

    agent_version = StringList(schema.agent, load_default="pre-0.11.0")
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    creator = Nested(schema.creator, PersonSchema, load_default=None)
    date_created = DateTimeList(schema.dateCreated, load_default=None, format="iso", extra_formats=("%Y-%m-%d",))
    description = fields.String(schema.description, load_default=None)
    id = fields.Id(load_default=None)
    immutable_template_files = fields.List(
        renku.immutableTemplateFiles,
        fields.String(),
        load_default=list(),
        attribute="template_metadata.immutable_template_files",
    )
    name = fields.String(schema.name, load_default=None)
    template_id = fields.String(renku.templateId, load_default=None, attribute="template_metadata.template_id")
    template_metadata = fields.String(renku.templateMetadata, load_default=None, attribute="template_metadata.metadata")
    template_ref = fields.String(renku.templateReference, load_default=None, attribute="template_metadata.template_ref")
    template_source = fields.String(
        renku.templateSource, load_default=None, attribute="template_metadata.template_source"
    )
    template_version = fields.String(
        renku.templateVersion, load_default=None, attribute="template_metadata.template_version"
    )
    version = StringList(schema.schemaVersion, load_default="1")
    keywords = fields.List(schema.keywords, fields.String(), load_default=None)
