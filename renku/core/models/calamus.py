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
"""Classes for integration with Calamus."""

import marshmallow
from calamus import fields
from calamus.schema import JsonLDSchema as CalamusJsonLDSchema
from calamus.utils import normalize_value

prov = fields.Namespace("http://www.w3.org/ns/prov#")
rdfs = fields.Namespace("http://www.w3.org/2000/01/rdf-schema#")
renku = fields.Namespace("https://swissdatasciencecenter.github.io/renku-ontology#")
schema = fields.Namespace("http://schema.org/")
wfprov = fields.Namespace("http://purl.org/wf4ever/wfprov#")


class JsonLDSchema(CalamusJsonLDSchema):
    """Base schema class for Renku."""

    def __init__(self, *args, commit=None, client=None, **kwargs):
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self._commit = commit
        self._client = client

    def _deserialize(self, *args, **kwargs):
        data = super()._deserialize(*args, **kwargs)
        self._add_field_to_data(data, "client", self._client)
        self._add_field_to_data(data, "commit", self._commit)

        return data

    def _add_field_to_data(self, data, name, value):
        if value:
            if name in data:
                raise ValueError(f"Field {name} is already in data {data}")
            data[name] = value


class Uri(fields._JsonLDField, marshmallow.fields.String, marshmallow.fields.Dict):
    """A Dict/String field."""

    def __init__(self, *args, **kwargs):
        """Create an instance."""
        super().__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, str):
            value = super(marshmallow.fields.String, self)._serialize(value, attr, obj, **kwargs)
            if self.parent.opts.add_value_types:
                value = {"@value": value, "@type": "http://www.w3.org/2001/XMLSchema#string"}
        elif isinstance(value, dict):
            value = super(marshmallow.fields.Dict, self)._serialize(value, attr, obj, **kwargs)

        return value

    def _deserialize(self, value, attr, data, **kwargs):
        value = normalize_value(value)
        if not value:
            return None
        elif isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return super(marshmallow.fields.Dict, self)._deserialize(value, attr, data, **kwargs)
        else:
            raise ValueError("Invalid type for field {}: {}".format(self.name, type(value)))


fields.Uri = Uri
