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
"""Represents a workflow template."""

import os
import pathlib
import urllib.parse
import uuid

import attr
from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, fields, rdfs, renku, schema
from renku.core.models.entities import CollectionSchema, EntitySchema
from renku.core.utils.urls import get_slug

RANDOM_ID_LENGTH = 4


@attr.s(eq=False, order=False)
class MappedIOStream(object):
    """Represents an IO stream (stdin, stdout, stderr)."""

    client = attr.ib(default=None, kw_only=True)

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    STREAMS = ["stdin", "stdout", "stderr"]

    stream_type = attr.ib(type=str, kw_only=True)

    def default_id(self):
        """Generate an id for a mapped stream."""
        host = "localhost"
        if self.client:
            host = self.client.remote.get("host") or host
        host = os.environ.get("RENKU_DOMAIN") or host

        return urllib.parse.urljoin(
            "https://{host}".format(host=host), pathlib.posixpath.join("/iostreams", self.stream_type)
        )

    def default_label(self):
        """Set default label."""
        return 'Stream mapping for stream "{}"'.format(self.stream_type)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._id:
            self._id = self.default_id()
        if not self._label:
            self._label = self.default_label()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return MappedIOStreamSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return MappedIOStreamSchema().dump(self)


@attr.s(eq=False, order=False)
class CommandParameter:
    """Represents a parameter for an execution template."""

    _id = attr.ib(default=None, kw_only=True)
    _label = attr.ib(default=None, kw_only=True)

    default_value = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    name: str = attr.ib(default=None, kw_only=True)

    position = attr.ib(default=None, type=int, kw_only=True)

    prefix = attr.ib(default=None, type=str, kw_only=True)

    @property
    def sanitized_id(self):
        """Return ``_id`` sanitized for use in non-jsonld contexts."""
        if "/steps/" in self._id:
            return "/".join(self._id.split("/")[-4:])
        return "/".join(self._id.split("/")[-2:])

    def default_label(self):
        """Set default label."""
        raise NotImplementedError

    def default_name(self):
        """Create a default name."""
        raise NotImplementedError

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._label:
            self._label = self.default_label()
        if not self.name:
            self.name = self.default_name()


@attr.s(eq=False, order=False)
class CommandArgument(CommandParameter):
    """An argument to a command that is neither input nor output."""

    value = attr.ib(default=None, type=str, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/arguments/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Argument "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="param", prefix=self.prefix, position=self.position)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.value]
            return ["{}{}".format(self.prefix, self.default_value)]

        return [self.value]

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.value

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return CommandArgumentSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return CommandArgumentSchema().dump(self)


@attr.s(eq=False, order=False)
class CommandInput(CommandParameter):
    """An input to a command."""

    consumes = attr.ib(kw_only=True)

    mapped_to = attr.ib(default=None, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/inputs/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Input "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="input", prefix=self.prefix, position=self.position)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.default_value]
            return ["{}{}".format(self.prefix, self.default_value)]

        return [self.default_value]

    def to_stream_repr(self):
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        return " < {}".format(self.default_value)

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.consumes.path

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return CommandInputSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return CommandInputSchema().dump(self)


@attr.s(eq=False, order=False)
class CommandOutput(CommandParameter):
    """An output of a command."""

    create_folder = attr.ib(default=False, kw_only=True, type=bool)

    produces = attr.ib(kw_only=True)

    mapped_to = attr.ib(default=None, kw_only=True)

    @staticmethod
    def generate_id(run_id, position=None):
        """Generate an id for an argument."""
        if position:
            id_ = str(position)
        else:
            id_ = uuid.uuid4().hex
        return "{}/outputs/{}".format(run_id, id_)

    def default_label(self):
        """Set default label."""
        return 'Command Output "{}"'.format(self.default_value)

    def default_name(self):
        """Create a default name."""
        return _generate_name(base="output", prefix=self.prefix, position=self.position)

    def to_argv(self):
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.default_value]
            return ["{}{}".format(self.prefix, self.default_value)]

        return [self.default_value]

    def to_stream_repr(self):
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        if self.mapped_to.stream_type == "stdout":
            return " > {}".format(self.default_value)

        return " 2> {}".format(self.default_value)

    def __attrs_post_init__(self):
        """Post-init hook."""
        super().__attrs_post_init__()

        if not self.default_value:
            self.default_value = self.produces.path

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return CommandOutputSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return CommandOutputSchema().dump(self)


@attr.s(eq=False, order=False)
class RunParameter:
    """A run parameter that is set inside the script."""

    _id = attr.ib(default=None, kw_only=True)

    _label = attr.ib(default=None, kw_only=True)

    name = attr.ib(default=None, type=str, kw_only=True)

    value = attr.ib(default=None, type=str, kw_only=True)

    type = attr.ib(default=None, type=str, kw_only=True)

    @staticmethod
    def generate_id(run_id, name):
        """Generate an id."""
        name = urllib.parse.quote(name, safe="")
        return "{}/parameters/{}".format(run_id, name)

    def default_label(self):
        """Set default label."""
        return 'Run Parameter "{}"'.format(self.name)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if not self._label:
            self._label = self.default_label()

        if not self.type:
            self.type = type(self.value).__name__

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return RunParameterSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return RunParameterSchema().dump(self)


class MappedIOStreamSchema(JsonLDSchema):
    """MappedIOStream schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.IOStream]
        model = MappedIOStream
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    stream_type = fields.String(renku.streamType)


class CommandParameterSchema(JsonLDSchema):
    """CommandParameter schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandParameter]  # , schema.PropertyValueSpecification]
        model = CommandParameter
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    default_value = fields.Raw(schema.defaultValue, missing=None)
    description = fields.String(schema.description, missing=None)
    name = fields.String(schema.name, missing=None)
    position = fields.Integer(renku.position, missing=None)
    prefix = fields.String(renku.prefix, missing=None)


class CommandArgumentSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandArgument]
        model = CommandArgument
        unknown = EXCLUDE

    value = fields.String(renku.value)


class CommandInputSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandInput]
        model = CommandInput
        unknown = EXCLUDE

    consumes = Nested(renku.consumes, [EntitySchema, CollectionSchema])
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, missing=None)


class CommandOutputSchema(CommandParameterSchema):
    """CommandArgument schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandOutput]
        model = CommandOutput
        unknown = EXCLUDE

    create_folder = fields.Boolean(renku.createFolder)
    produces = Nested(renku.produces, [EntitySchema, CollectionSchema])
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, missing=None)


class RunParameterSchema(JsonLDSchema):
    """RunParameter schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.RunParameter]
        model = RunParameter
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    _label = fields.String(rdfs.label, init_name="label")
    name = fields.String(schema.name)
    value = fields.String(renku.value)
    type = fields.String(renku.type)


def _generate_name(base, prefix, position):
    name = get_slug(prefix.strip(" -=")) if prefix else base
    position = position or uuid.uuid4().hex[:RANDOM_ID_LENGTH]
    return f"{name}-{position}"
