# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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

import copy
import inspect

import marshmallow
from calamus import fields
from calamus.schema import JsonLDSchema as CalamusJsonLDSchema
from calamus.utils import normalize_type, normalize_value
from marshmallow.base import SchemaABC

from renku.core import errors
from renku.domain_model.project_context import project_context

prov = fields.Namespace("http://www.w3.org/ns/prov#")
rdfs = fields.Namespace("http://www.w3.org/2000/01/rdf-schema#")
renku = fields.Namespace("https://swissdatasciencecenter.github.io/renku-ontology#")
schema = fields.Namespace("http://schema.org/")
oa = fields.Namespace("http://www.w3.org/ns/oa#")
dcterms = fields.Namespace("http://purl.org/dc/terms/")


class JsonLDSchema(CalamusJsonLDSchema):
    """Base schema class for Renku."""

    def __init__(self, *args, commit=None, **kwargs):
        """Create an instance."""
        self._commit = commit
        super().__init__(*args, **kwargs)

    def _deserialize(self, *args, **kwargs):
        data = super()._deserialize(*args, **kwargs)
        const_args = inspect.signature(self.opts.model)
        parameters = const_args.parameters.values()

        if any(p.name == "commit" for p in parameters):
            if self._commit:
                self._add_field_to_data(data, "commit", self._commit)
            elif (
                project_context.has_context()
                and "_label" in data
                and data["_label"]
                and "@UNCOMMITTED" not in data["_label"]
                and "@" in data["_label"]
            ):
                try:
                    self._add_field_to_data(
                        data,
                        "commit",
                        project_context.repository.get_commit(data["_label"].rsplit("@", maxsplit=1)[-1]),
                    )
                except errors.GitCommitNotFoundError:
                    # NOTE: This means the commit does not exist in the local repository. Could be an external file?
                    pass

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
            value = super(fields._JsonLDField, self)._serialize(value, attr, obj, **kwargs)
            if self.parent.opts.add_value_types:
                value = {"@value": value, "@type": "http://www.w3.org/2001/XMLSchema#string"}
        elif isinstance(value, dict):
            value = super(marshmallow.fields.String, self)._serialize(value, attr, obj, **kwargs)

        return value

    def _deserialize(self, value, attr, data, **kwargs):
        value = normalize_value(value)
        if not value:
            return None
        elif isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return super(marshmallow.fields.String, self)._deserialize(value, attr, data, **kwargs)
        else:
            raise ValueError("Invalid type for field {}: {}".format(self.name, type(value)))


class StringList(fields.String):
    """A String field that might be a list when deserializing."""

    def __init__(self, *args, return_max_value=True, **kwargs):
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.return_max_value = return_max_value

    def _deserialize(self, value, attr, data, **kwargs):
        value = normalize_value(value)

        if isinstance(value, (list, tuple, set)):
            value = sorted(value, reverse=self.return_max_value)
            value = value[0] if len(value) > 0 else None

        return super()._deserialize(value, attr, data, **kwargs)


class DateTimeList(fields.DateTime):
    """A DateTime field that might be a list when deserializing."""

    def __init__(self, *args, **kwargs):
        """Create an instance."""
        super().__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        value = normalize_value(value)

        if isinstance(value, (list, tuple, set)):
            value = sorted(value)
            value = value[0] if len(value) > 0 else None

        return super()._deserialize(value, attr, data, **kwargs)


class Nested(fields.Nested):
    """Nested field that passes along commit info."""

    def __init__(self, *args, **kwargs):
        """Init method."""
        super().__init__(*args, **kwargs)

    @property
    def schema(self):
        """The nested ``calamus.Schema`` object.

        This method was copied from marshmallow and modified to support
        multiple different nested schemes.
        """
        if not self._schema:
            # Inherit context from parent.
            context = getattr(self.parent, "context", {})
            self._schema = {"from": {}, "to": {}}
            for nest in self.nested:
                if isinstance(nest, SchemaABC):
                    rdf_type = str(normalize_type(nest.opts.rdf_type))
                    model = nest.opts.model
                    if not rdf_type or not model:
                        raise ValueError("Both rdf_type and model need to be set on the " "schema for nested to work")
                    _schema = copy.copy(nest)
                    _schema.context.update(context)
                    # Respect only and exclude passed from parent and
                    # re-initialize fields
                    set_class = _schema.set_class
                    if self.only is not None:
                        if self._schema.only is not None:
                            original = _schema.only
                        else:  # only=None -> all fields
                            original = _schema.fields.keys()
                        _schema.only = set_class(self.only).intersection(original)
                    if self.exclude:
                        original = _schema.exclude
                        _schema.exclude = set_class(self.exclude).union(original)
                    _schema._init_fields()
                    _schema._visited = self.root._visited
                    self._schema["from"][rdf_type] = _schema
                    self._schema["to"][model] = _schema
                else:
                    if isinstance(nest, type) and issubclass(nest, SchemaABC):
                        schema_class = nest
                    elif not isinstance(nest, (str, bytes)):
                        raise ValueError("Nested fields must be passed a " "Schema, not {}.".format(nest.__class__))
                    elif nest == "self":
                        ret = self
                        while not isinstance(ret, SchemaABC):
                            ret = ret.parent
                        schema_class = ret.__class__
                    else:
                        schema_class = marshmallow.class_registry.get_class(nest)

                    rdf_type = str(normalize_type(schema_class.opts.rdf_type))
                    model = schema_class.opts.model
                    if not rdf_type or not model:
                        raise ValueError("Both rdf_type and model need to be set on the " "schema for nested to work")

                    kwargs = {}

                    self._schema["from"][rdf_type] = schema_class(
                        many=False,
                        only=self.only,
                        exclude=self.exclude,
                        context=context,
                        load_only=self._nested_normalized_option("load_only"),
                        dump_only=self._nested_normalized_option("dump_only"),
                        lazy=self.root.lazy,
                        flattened=self.root.flattened,
                        _visited=self.root._visited,
                        **kwargs,
                    )
                    self._schema["to"][model] = self._schema["from"][rdf_type]
        return self._schema
