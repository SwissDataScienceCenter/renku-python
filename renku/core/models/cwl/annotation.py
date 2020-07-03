# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Represent an annotation for a workflow."""

from marshmallow import EXCLUDE

from renku.core.models import jsonld
from renku.core.models.calamus import JsonLDSchema, dcterms, fields, oa


@jsonld.s(
    type='oa:Annotation',
    context={
        'oa': 'http://www.w3.org/ns/oa#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'dcterms': 'http://purl.org/dc/terms/',
    },
    cmp=False,
)
class Annotation:
    """Represents a custom annotation for a research object."""

    _id = jsonld.ib(context='@id', kw_only=True)

    body = jsonld.ib(default=None, context='oa:hasBody', kw_only=True)

    source = jsonld.ib(default=None, context='dcterms:creator', kw_only=True)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return AnnotationSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return AnnotationSchema().dump(self)


class AnnotationSchema(JsonLDSchema):
    """Annotation schema."""

    class Meta:
        """Meta class."""

        rdf_type = oa.Annotation
        model = Annotation
        unknown = EXCLUDE

    _id = fields.Id(init_name='id')
    body = fields.Raw(oa.hasBody)
    source = fields.Raw(dcterms.creator)
