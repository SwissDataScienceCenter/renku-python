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
"""Classes to represent inputs/outputs/parameters in a Plan."""

import urllib
from abc import abstractmethod
from pathlib import PurePosixPath
from typing import Any, List, Optional
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core.errors import ParameterError
from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.utils.urls import get_slug

RANDOM_ID_LENGTH = 4
DIRECTORY_MIME_TYPE = "inode/directory"


def _validate_mime_type(encoding_format: List[str]):
    """Validates MIME-types."""
    if encoding_format and DIRECTORY_MIME_TYPE in encoding_format and len(encoding_format) > 1:
        raise ParameterError(f"Directory MIME-type '{DIRECTORY_MIME_TYPE}'.")


class MappedIOStream:
    """Represents an IO stream (stdin, stdout, stderr)."""

    STREAMS = ["stdin", "stdout", "stderr"]

    def __init__(self, *, id: str = None, stream_type: str):
        assert stream_type in MappedIOStream.STREAMS

        self.id: str = id or MappedIOStream.generate_id(stream_type)
        self.stream_type = stream_type

    @staticmethod
    def generate_id(stream_type: str) -> str:
        """Generate an id for parameters."""
        return f"/iostreams/{stream_type}"


class CommandParameterBase:
    """Represents a parameter for a Plan."""

    def __init__(
        self,
        *,
        default_value: Any,
        description: str,
        id: str,
        name: str,
        position: Optional[int] = None,
        prefix: Optional[str] = None,
        derived_from: str = None,
        postfix: str = None,
    ):
        self.default_value: Any = default_value
        self.description: str = description
        self.id: str = id
        self.name: str = name
        self.position: Optional[int] = position
        self.prefix: str = prefix
        self._v_actual_value_set = False
        self.derived_from: str = derived_from
        self.postfix: str = postfix

        if not self.name:
            self.name = self._get_default_name()

    @staticmethod
    def _generate_id(plan_id: str, parameter_type: str, position: Optional[int], postfix: str = None) -> str:
        """Generate an id for parameters."""
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/1
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/stdin
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/dda5fcbf-0098-4917-be46-dc12f5f7b675
        position = str(position) if position is not None else uuid4().hex
        postfix = urllib.parse.quote(postfix) if postfix else position
        return f"{plan_id}/{parameter_type}/{postfix}"

    @property
    def role(self) -> str:
        """Return a unique role for this parameter within its Plan."""
        assert self.id, "Id is not set"
        return PurePosixPath(self.id).name

    def to_argv(self) -> List[Any]:
        """String representation (sames as cmd argument)."""
        value = self.actual_value

        if isinstance(value, str) and " " in value:
            value = f'"{value}"'

        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], value]
            return [f"{self.prefix}{value}"]

        return [value]

    @property
    def actual_value(self):
        """Get the actual value to be used for execution."""
        if getattr(self, "_v_actual_value_set", False):
            return self._v_actual_value

        return self.default_value

    @actual_value.setter
    def actual_value(self, value):
        """Set the actual value to be used for execution."""
        self._v_actual_value = value
        self._v_actual_value_set = True

    @property
    def actual_value_set(self):
        """Whether the actual_value on this parameter has been set at least once."""
        return getattr(self, "_v_actual_value_set", False)

    def _generate_name(self, base) -> str:
        name = get_slug(self.prefix.strip(" -="), invalid_chars=["."]) if self.prefix else base
        position = self.position or uuid4().hex[:RANDOM_ID_LENGTH]
        return f"{name}-{position}"

    def _get_default_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def derive(self, plan_id: str) -> "CommandParameterBase":
        """Create a new command parameter from self."""
        raise NotImplementedError


class CommandParameter(CommandParameterBase):
    """An argument to a command that is neither input nor output."""

    def __init__(
        self,
        *,
        default_value: Any = None,
        description: str = None,
        id: str,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
        derived_from: str = None,
        postfix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            name=name,
            position=position,
            prefix=prefix,
            derived_from=derived_from,
            postfix=postfix,
        )

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandParameter."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="parameters", position=position, postfix=postfix
        )

    def _get_default_name(self) -> str:
        return self._generate_name(base="parameter")

    def derive(self, plan_id: str) -> "CommandParameter":
        """Create a new ``CommandParameter`` that is derived from self."""
        return CommandParameter(
            default_value=self.default_value,
            description=self.description,
            id=CommandParameter.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix),
            name=self.name,
            position=self.position,
            prefix=self.prefix,
            derived_from=self.id,
            postfix=self.postfix,
        )


class CommandInput(CommandParameterBase):
    """An input to a command."""

    def __init__(
        self,
        *,
        default_value: Any = None,
        description: str = None,
        id: str,
        mapped_to: MappedIOStream = None,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
        encoding_format: List[str] = None,
        derived_from: str = None,
        postfix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            name=name,
            position=position,
            prefix=prefix,
            derived_from=derived_from,
            postfix=postfix,
        )
        self.mapped_to: MappedIOStream = mapped_to
        _validate_mime_type(encoding_format)
        self.encoding_format: List[str] = encoding_format

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandInput."""
        return CommandParameterBase._generate_id(plan_id, parameter_type="inputs", position=position, postfix=postfix)

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        return f"< {self.default_value}" if self.mapped_to else ""

    def _get_default_name(self) -> str:
        return self._generate_name(base="input")

    def derive(self, plan_id: str) -> "CommandInput":
        """Create a new ``CommandInput`` that is derived from self."""
        return CommandInput(
            default_value=self.default_value,
            description=self.description,
            id=CommandInput.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix),
            mapped_to=self.mapped_to,
            name=self.name,
            position=self.position,
            prefix=self.prefix,
            encoding_format=self.encoding_format,
            derived_from=self.id,
            postfix=self.postfix,
        )


class CommandOutput(CommandParameterBase):
    """An output from a command."""

    def __init__(
        self,
        *,
        create_folder: bool = False,
        default_value: Any = None,
        description: str = None,
        id: str,
        mapped_to: MappedIOStream = None,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
        encoding_format: List[str] = None,
        derived_from: str = None,
        postfix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            name=name,
            position=position,
            prefix=prefix,
            derived_from=derived_from,
            postfix=postfix,
        )
        self.create_folder: bool = create_folder
        self.mapped_to: MappedIOStream = mapped_to
        _validate_mime_type(encoding_format)
        self.encoding_format: List[str] = encoding_format

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandOutput."""
        return CommandParameterBase._generate_id(plan_id, parameter_type="outputs", position=position, postfix=postfix)

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        return f"> {self.default_value}" if self.mapped_to.stream_type == "stdout" else f" 2> {self.default_value}"

    def _get_default_name(self) -> str:
        return self._generate_name(base="output")

    def derive(self, plan_id: str) -> "CommandOutput":
        """Create a new ``CommandOutput`` that is derived from self."""
        return CommandOutput(
            default_value=self.default_value,
            description=self.description,
            id=CommandOutput.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix),
            mapped_to=self.mapped_to,
            name=self.name,
            position=self.position,
            prefix=self.prefix,
            encoding_format=self.encoding_format,
            derived_from=self.id,
            postfix=self.postfix,
        )


class ParameterMapping(CommandParameterBase):
    """A mapping of child parameter(s) to a parent CompositePlan."""

    def __init__(
        self,
        *,
        default_value: Any = None,
        description: str = None,
        id: str,
        name: str = None,
        mapped_parameters: List[CommandParameterBase] = None,
        **kwargs,
    ):
        super().__init__(default_value=default_value, description=description, id=id, name=name, **kwargs)

        self.mapped_parameters: List[CommandParameterBase] = mapped_parameters

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandOutput."""
        return CommandParameterBase._generate_id(plan_id, parameter_type="mappings", position=position, postfix=postfix)

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        return ""

    def _get_default_name(self) -> str:
        return self._generate_name(base="mapping")

    @CommandParameterBase.actual_value.setter
    def actual_value(self, value):
        """Set the actual value to be used for execution."""
        self._v_actual_value = value
        self._v_actual_value_set = True

        for mapped_to in self.mapped_parameters:
            if not mapped_to.actual_value_set:
                mapped_to.actual_value = value

    @property
    def leaf_parameters(self):
        """Return leaf (non-Mapping) parameters contained by this Mapping."""
        for mapped_to in self.mapped_parameters:
            if isinstance(mapped_to, ParameterMapping):
                yield mapped_to.leaf_parameters
            else:
                yield mapped_to


class ParameterLink:
    """A link between a source and one or more sink parameters."""

    def __init__(
        self,
        source: CommandParameterBase,
        sinks: List[CommandParameterBase],
        id: str,
    ):
        self.source = source
        self.sinks = sinks
        self.id: str = id

    def apply(self):
        """Apply source value to sinks."""
        for s in self.sinks:
            s.actual_value = self.source.actual_value

    @staticmethod
    def generate_id(plan_id: str) -> str:
        """Generate an id for parameters."""
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/links/dda5fcbf-0098-4917-be46-dc12f5f7b675
        return f"{plan_id}/links/{uuid4()}"


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
