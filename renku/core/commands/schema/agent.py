# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Agents JSON-LD schemas."""

from calamus.schema import JsonLDSchema
from marshmallow import EXCLUDE

from renku.core.commands.schema.calamus import StringList, fields, prov, schema
from renku.core.models.provenance.agent import Person, SoftwareAgent


class PersonSchema(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    affiliation = StringList(schema.affiliation, missing=None)
    alternate_name = StringList(schema.alternateName, missing=None)
    email = fields.String(schema.email, missing=None)
    id = fields.Id()
    name = StringList(schema.name, missing=None)


class SoftwareAgentSchema(JsonLDSchema):
    """SoftwareAgent schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.SoftwareAgent]
        model = SoftwareAgent
        unknown = EXCLUDE

    id = fields.Id()
    name = StringList(schema.name, missing=None)
