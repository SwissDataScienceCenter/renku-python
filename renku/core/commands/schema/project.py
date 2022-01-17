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
"""Project JSON-LD schema."""

from marshmallow import EXCLUDE

from renku.core.commands.schema.agent import PersonSchema
from renku.core.commands.schema.annotation import AnnotationSchema
from renku.core.commands.schema.calamus import (
    DateTimeList,
    JsonLDSchema,
    Nested,
    StringList,
    fields,
    oa,
    prov,
    renku,
    schema,
)
from renku.core.models.project import Project


class ProjectSchema(JsonLDSchema):
    """Project Schema."""

    class Meta:
        """Meta class."""

        rdf_type = [schema.Project, prov.Location]
        model = Project
        unknown = EXCLUDE

    agent_version = StringList(schema.agent, missing="pre-0.11.0")
    annotations = Nested(oa.hasTarget, AnnotationSchema, reverse=True, many=True)
    automated_update = fields.Boolean(renku.automatedTemplateUpdate, missing=False)
    creator = Nested(schema.creator, PersonSchema, missing=None)
    date_created = DateTimeList(schema.dateCreated, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    description = fields.String(schema.description, missing=None)
    id = fields.Id(missing=None)
    immutable_template_files = fields.List(renku.immutableTemplateFiles, fields.String(), missing=[])
    name = fields.String(schema.name, missing=None)
    template_id = fields.String(renku.templateId, missing=None)
    template_metadata = fields.String(renku.templateMetadata, missing=None)
    template_ref = fields.String(renku.templateReference, missing=None)
    template_source = fields.String(renku.templateSource, missing=None)
    template_version = fields.String(renku.templateVersion, missing=None)
    version = StringList(schema.schemaVersion, missing="1")
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
