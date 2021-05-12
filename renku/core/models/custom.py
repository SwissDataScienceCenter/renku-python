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
"""Custom intermediate format classes."""

import inspect
import typing
import uuid

import marshmallow
from marshmallow import INCLUDE, fields

VALID_FORMATS = ["custom", "json", "jsonld", "json-ld", "zodb", "zope"]

object_registry = {}


def is_collection(obj):
    """A faster replacement for marshmallow.utils.is_collection."""
    return isinstance(obj, (list, tuple))


def assert_valid_format(format):
    """Assert if serialization format is valid."""
    assert format in VALID_FORMATS


class JsonSchemaOpts(marshmallow.SchemaOpts):
    """Options class for `JsonSchema`."""

    def __init__(self, meta, *args, **kwargs):
        super().__init__(meta, *args, **kwargs)

        # NOTE: Include all fields because metadata might be old; Exclude non-existing fields after OTF migration.
        self.unknown = INCLUDE


class JsonSchema(marshmallow.Schema):
    """Base schema class for JSON schema."""

    OPTIONS_CLASS = JsonSchemaOpts

    __model__ = None
    __version__ = 1

    def __init__(self, *args, commit=None, client=None, **kwargs):
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self._commit = commit
        self._client = client

    def load(
        self,
        data: typing.Union[typing.Mapping[str, typing.Any], typing.Iterable[typing.Mapping[str, typing.Any]],],
        flattened=False,
        **kwargs,
    ):
        """Signature copied from marshmallow.Schema.load."""
        if flattened:
            data = _unflatten(data, self.__model__.__name__)
        else:
            # NOTE: Check if data has proper type
            def check_type(datum):
                model = datum.get("@type")
                expected = self.__model__.__name__
                if not model or not model == expected:
                    raise ValueError(f"Deserializing '{expected}' but received '{model}'")

            if not is_collection(data):
                check_type(data)
            else:
                for d in data:
                    check_type(d)

        return super().load(data, **kwargs)

    def dump(self, obj: typing.Any, *, many: typing.Optional[bool] = None, flattened=False):
        """Signature copied from marshmallow.Schema.dump."""
        expected = self.__model__.__name__
        objects = obj if many else [obj]
        for o in objects:
            if o.__class__.__name__ != expected:
                raise ValueError(f"Serializing '{expected}' but received '{o.__class__.__name__}'")

        data = super().dump(obj, many=many)
        return _flatten(data) if flattened else data

    @marshmallow.post_dump
    def add_metadata(self, data, **kwargs):
        """Add type of schema to data."""
        for metadata in ["@type", "@version"]:
            if metadata in data:
                raise ValueError(f"Data already has a '{metadata}' field: {data}")

        data["@type"] = self.__model__.__name__  # TODO: use fully qualified class name
        data["@version"] = self.__version__

        if "@id" not in data or not data["@id"]:
            data["@id"] = f"_:{uuid.uuid4().hex}"

        return data

    @marshmallow.post_load
    def make_instance(self, data, **kwargs):
        """Transform loaded dict into corresponding object."""
        # FIXME _id, id_
        id = data.pop("@id", None) or data.pop("_id", None) or data.pop("id_", None)
        if not id:
            raise ValueError(f"Data has no '@id' field: {data}")

        instance = object_registry.get(id)
        if instance:
            # TODO: Check if data is the same as instance's data
            return instance

        # NOTE: Set proper name for id field in the data so that objects can find it
        data["id"] = id

        # TODO: Cache model's signature
        const_args = inspect.signature(self.__model__)
        keys = set(data.keys())
        args = []
        kwargs = {}
        has_kwargs = False
        for _, parameter in const_args.parameters.items():
            if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
                # NOTE: To avoid potential errors we require positional-only arguments to always be present in data.
                if parameter.name not in keys:
                    raise ValueError("Positional field '{}' not found in data {}".format(parameter.name, data))
                args.append(data[parameter.name])
                keys.remove(parameter.name)
            elif parameter.kind in [inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY]:
                if parameter.name not in keys:
                    if parameter.default is inspect.Parameter.empty:
                        raise ValueError(
                            f"Field '{parameter.name}' for '{self.__model__.__name__}' not found in data {data}"
                        )
                else:
                    kwargs[parameter.name] = data[parameter.name]
                    keys.remove(parameter.name)
            elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
                has_kwargs = True
        missing_data = {k: v for k, v in data.items() if k in keys}
        if has_kwargs:
            instance = self.__model__(*args, **kwargs, **missing_data)
        else:
            instance = self.__model__(*args, **kwargs)

        unset_data = {}
        for key, value in missing_data.items():
            if hasattr(instance, key):
                if not getattr(instance, key):
                    setattr(instance, key, value)
            elif key not in ["@type", "@version"]:
                unset_data[key] = value

        if unset_data:
            raise ValueError(
                "The following fields were not found on class {}:\n\t{}".format(
                    self.__model__, "\n\t".join(unset_data.keys())
                )
            )

        if id:
            object_registry[id] = instance

        return instance


def _flatten(data):
    mappings = {}
    _flatten_helper(data, mappings)
    return list(mappings.values()) if len(mappings) > 1 else data


def _flatten_helper(data, mappings):
    if not data:
        return
    elif isinstance(data, dict):
        if "@id" not in data:
            raise ValueError(f"Dict data doesn't have id: {data}")
        if len(data) == 1:  # An already-flattened reference
            return

        keys = {}
        for key, value in data.items():
            if not is_collection(value) and not isinstance(value, dict):
                continue

            id = _flatten_helper(value, mappings)
            if is_collection(id):
                keys[key] = id
            elif id:
                keys[key] = {"@id": id}

        data.update(keys)
        id = data["@id"]
        # TODO: check if id exists in mapping and do something about duplicates
        mappings[id] = data

        return id
    elif is_collection(data):
        ids = []
        # TODO: Check either all elements have id or none has
        for value in data:
            if is_collection(value):
                raise ValueError(f"Collection within collection: {data}")
            if not isinstance(value, dict):
                # FIXME This value is lost
                continue
            id = _flatten_helper(value, mappings)
            if id:
                ids.append({"@id": id})

        return ids


def _unflatten(data, schema_type):
    if not data:
        return data

    top_nodes = [d for d in data if d["@type"] == schema_type]
    if len(top_nodes) == 0:
        raise ValueError(f"Cannot find '{schema_type}' in data")
    elif len(top_nodes) == 1:
        top_nodes = top_nodes[0]

    mappings = {d["@id"]: d for d in data}
    nested = set()

    return _unflatten_helper(top_nodes, mappings, nested)


def _unflatten_helper(data, mappings, nested):
    # TODO Use `nested` to avoid infinite looping if there is a cycle in data
    if not data:
        return data
    elif isinstance(data, dict):
        if "@id" not in data:
            raise ValueError(f"Dict data doesn't have id: {data}")

        if len(data) == 1:  # A reference
            id = data["@id"]
            data = mappings.get(id)
            if data is None:
                raise ValueError(f"Cannot find datum with id: {id}")

        for key, value in data.items():
            if not is_collection(value) and not isinstance(value, dict):
                continue

            value = _unflatten_helper(value, mappings, nested)
            data[key] = value

        nested.add(data["@id"])

        return data
    elif is_collection(data):
        values = []
        for value in data:
            if is_collection(value):
                raise ValueError(f"Collection within collection: {data}")
            if not isinstance(value, dict):
                continue
            data = _unflatten_helper(value, mappings, nested)
            values.append(data)

        return values
    else:
        return data


models = {}


def deserialize(data, schema_type, flattened=False):
    """Deserialize data to python objects."""
    global models
    if not models:
        models = _get_models()

    if flattened:
        data = _unflatten(data, schema_type)

    return _deserialize_helper(data)


def _deserialize_helper(data):
    if not data:
        return
    elif isinstance(data, dict):
        for key, value in data.items():
            if not is_collection(value) and not isinstance(value, dict):
                continue

            instance = _deserialize_helper(value)
            data[key] = instance

        return _instantiate(data)
    elif is_collection(data):
        instances = []
        # TODO: Check either all elements have id or none has
        for value in data:
            if is_collection(value):
                raise ValueError(f"Collection within collection: {data}")
            elif not isinstance(value, dict):  # A primitive type
                instances.append(value)
            else:  # A dict
                instances.append(_deserialize_helper(value))

        return instances


def _instantiate(data):
    id = data.pop("@id", None) or data.pop("_id", None) or data.pop("id_", None) or data.pop("id", None)
    if not id:
        raise ValueError(f"Data has no '@id' field: {data}")

    # NOTE: Set proper name for id field in the data so that objects can find it
    # NOTE: data can be used in multiple places after unfalttening; do not destroy it
    data["id"] = id

    instance = object_registry.get(id)
    if instance:
        # TODO: Check if data is the same as instance's data
        return instance

    data.pop("@version", 1)
    model_type = data.pop("@type", None)

    if not model_type:
        return data

    instance = models.get(model_type)(**data)

    if id:
        object_registry[id] = instance

    return instance


def _get_models():
    from renku.core.models.cwl.annotation import Annotation
    from renku.core.models.entities import Collection, CommitMixin, Entity
    from renku.core.models.provenance.activity import Activity, ActivityCollection
    from renku.core.models.provenance.agents import Person, SoftwareAgent
    from renku.core.models.provenance.provenance_graph import ProvenanceGraph
    from renku.core.models.provenance.qualified import Association, Generation, Usage
    from renku.core.models.workflow.dependency_graph import DependencyGraph
    from renku.core.models.workflow.parameters import (
        CommandArgument,
        CommandInputTemplate,
        CommandOutputTemplate,
        CommandParameter,
        MappedIOStream,
    )
    from renku.core.models.workflow.plan import Plan

    return {
        m.__name__: m
        for m in [
            Activity,
            ActivityCollection,
            Annotation,
            Association,
            Collection,
            CommandArgument,
            CommandInputTemplate,
            CommandOutputTemplate,
            CommandParameter,
            CommitMixin,
            DependencyGraph,
            Entity,
            Generation,
            MappedIOStream,
            Person,
            Plan,
            ProvenanceGraph,
            SoftwareAgent,
            Usage,
        ]
    }


class Id(fields.String):
    """A node identifier."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, data_key="@id", **kwargs)


class Nested(fields.Nested):
    """A reference to one or more nested classes."""

    def __init__(
        self,
        nested: typing.Union[
            JsonSchema,
            typing.Callable[[], JsonSchema],
            typing.Iterable[JsonSchema],
            typing.Iterable[typing.Callable[[], JsonSchema]],
        ],
        *args,
        **kwargs,
    ):
        super().__init__(nested, *args, **kwargs)

        def check_type(schema):
            if isinstance(schema, type) and issubclass(schema, JsonSchema):
                return schema
            elif callable(schema) and not isinstance(schema, type):  # and issubclass(schema(), JsonSchema):
                return schema
            else:
                raise ValueError(f"Only JsonSchema is allowed in nested fields, not {schema}")

        if is_collection(nested):
            for n in self.nested:
                check_type(n)
        else:
            check_type(nested)

    def _serialize(self, nested_obj, *args, **kwargs):
        self._process_schemas()

        if not self.multiple_schemas:
            return super()._serialize(nested_obj, *args, **kwargs)

        # TODO check if self.many is set or many=True is passed if is_collection
        if is_collection(nested_obj):
            return [self._serialized_single_object(o) for o in nested_obj]
        else:
            return self._serialized_single_object(nested_obj)

    def _serialized_single_object(self, obj):
        object_type = obj.__class__.__name__
        schema = self.schema.get(object_type)

        if not schema:
            raise ValueError(f"Cannot find a nested schema to serialize '{object_type}'")

        return schema.dump(obj, many=False)

    def _deserialize(self, value, *args, **kwargs):
        self._process_schemas()

        if not self.multiple_schemas:
            return super()._deserialize(value, *args, **kwargs)

        # TODO check if self.many is set or many=True is passed if is_collection
        if is_collection(value):
            return [self._deserialized_single_object(v) for v in value]
        else:
            return self._deserialized_single_object(value)

    def _deserialized_single_object(self, value):
        object_type = value.get("@type")
        if not object_type:
            raise ValueError(f"Data has no '@type': '{value}'")

        schema = self.schema.get(object_type)

        if not schema:
            raise ValueError(f"Cannot find a nested schema to deserialize '{object_type}'")

        return schema.load(value, many=False)

    def _process_schemas(self):
        schemas = {}

        if is_collection(self.nested):
            for n in self.nested:
                # NOTE: Force reloading nested schema in marshmallow.Nested
                self.nested = n
                self._schema = None
                schemas[self.schema.__model__.__name__] = self.schema

            self._schema = schemas

    @property
    def multiple_schemas(self):
        """Return if there are multiple nested schemas."""
        return isinstance(self._schema, dict)


fields.Id = Id
