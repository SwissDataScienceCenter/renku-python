# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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
"""Test Calamus model classes."""

import pytest

from renku.core.models.calamus import JsonLDSchema, Uri, fields


@pytest.mark.parametrize("value", [{"field": "http://datascience.ch"}, "http://datascience.ch"])
def test_uri_field_serialization(value):
    """Test serialization of Uri fields."""

    class Entity:
        def __init__(self, field):
            self.field = field

    schema = fields.Namespace("http://schema.org/")

    class EntitySchema(JsonLDSchema):
        field = Uri(schema.field, allow_none=True)

        class Meta:
            rdf_type = schema.Entity
            model = Entity

    entity = Entity(field=value)

    data = EntitySchema().dump(entity)

    if "@id" in data:
        del data["@id"]

    assert data == {"@type": ["http://schema.org/Entity"], "http://schema.org/field": value}


@pytest.mark.parametrize("value", [{"url": "http://datascience.ch"}, "http://datascience.ch", None])
def test_uri_field_deserialization(value):
    """Test deserialization of Uri fields."""

    class Entity:
        def __init__(self, field):
            self.field = field

    schema = fields.Namespace("http://schema.org/")

    class EntitySchema(JsonLDSchema):
        field = Uri(schema.field, allow_none=True)

        class Meta:
            rdf_type = schema.Entity
            model = Entity

    data = {"@type": ["http://schema.org/Entity"], "http://schema.org/field": value}

    entity = EntitySchema().load(data)

    assert entity.field == value
