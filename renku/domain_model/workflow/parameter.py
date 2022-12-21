# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

import copy
import shlex
import urllib
from abc import abstractmethod
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, List, Optional
from uuid import uuid4

from renku.core import errors
from renku.core.errors import ParameterError
from renku.core.util.urls import get_slug

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

    @classmethod
    def from_str(cls, stream_type: str) -> "MappedIOStream":
        """Create an instance from the given stream type string."""
        if stream_type not in MappedIOStream.STREAMS:
            streams = ", ".join(MappedIOStream.STREAMS)
            raise errors.ParameterError(f"'{stream_type}' must be one of {streams}")

        return cls(stream_type=stream_type)

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
        description: Optional[str],
        id: str,
        name: Optional[str],
        position: Optional[int] = None,
        prefix: Optional[str] = None,
        derived_from: Optional[str] = None,
        postfix: Optional[str] = None,
    ):
        self.default_value: Any = default_value
        self.description: Optional[str] = description
        self.id: str = id
        self.position: Optional[int] = position
        self.prefix: Optional[str] = prefix
        self._v_actual_value = None
        self._v_actual_value_set: bool = False
        self.derived_from: Optional[str] = derived_from
        # NOTE: ``postfix`` is used only to generate a nicer ``id`` for a parameter. Its value isn't used anywhere else.
        self.postfix: Optional[str] = postfix

        if name is not None:
            self.name: str = name
        else:
            self.name = self._get_default_name()

    @staticmethod
    def _generate_id(
        plan_id: str,
        parameter_type: str,
        position: Optional[int],
        postfix: Optional[str] = None,
        name: Optional[str] = None,
    ) -> str:
        """Generate an id for parameters."""
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/1
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/stdin
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/data.csv
        # /plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/dda5fc0f00984917be46dc12f5f7b675
        id = uuid4().hex

        if postfix:
            id = urllib.parse.quote(postfix)
        elif position is not None:
            id = str(position)
        elif name:
            id = name

        return f"{plan_id}/{parameter_type}/{id}"

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self.actual_value}'>"

    @property
    def role(self) -> str:
        """Return a unique role for this parameter within its Plan."""
        assert self.id, "Id is not set"
        return PurePosixPath(self.id).name

    @staticmethod
    def _get_equality_attributes() -> List[str]:
        """Return a list of attributes values that determine if instances are equal."""
        return ["name", "description", "default_value", "prefix", "position"]

    def is_equal_to(self, other) -> bool:
        """Return if attributes that cause a change in the parameter, are the same."""
        return all(getattr(self, a) == getattr(other, a) for a in self._get_equality_attributes())

    def to_argv(self, quote_string: bool = True) -> List[Any]:
        """String representation (sames as cmd argument)."""
        value = self.actual_value

        if isinstance(value, str) and quote_string and " " in value:
            value = shlex.quote(value)

        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], str(value)]
            return [f"{self.prefix}{value}"]

        return [str(value)]

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
        return generate_parameter_name(parameter=self, kind=base)

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
    def generate_id(
        plan_id: str, position: Optional[int] = None, postfix: Optional[str] = None, name: Optional[str] = None
    ) -> str:
        """Generate an id for CommandParameter."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="parameters", position=position, postfix=postfix, name=name
        )

    def __repr__(self):
        return (
            f"<Parameter '{self.name}': {self.actual_value} (default: {self.default_value}, prefix: {self.prefix}, "
            f"position: {self.position})>"
        )

    def _get_default_name(self) -> str:
        return self._generate_name(base="parameter")

    def derive(self, plan_id: str) -> "CommandParameter":
        """Create a new ``CommandParameter`` that is derived from self."""
        parameter = copy.copy(self)
        parameter.id = CommandParameter.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix)
        return parameter


class CommandInput(CommandParameterBase):
    """An input to a command."""

    def __init__(
        self,
        *,
        default_value: Optional[Any] = None,
        description: Optional[str] = None,
        id: str,
        mapped_to: Optional[MappedIOStream] = None,
        name: Optional[str] = None,
        position: Optional[int] = None,
        prefix: Optional[str] = None,
        encoding_format: Optional[List[str]] = None,
        derived_from: Optional[str] = None,
        postfix: Optional[str] = None,
    ):
        assert isinstance(default_value, (Path, str)), f"Invalid value type for CommandOutput: {type(default_value)}"

        super().__init__(
            default_value=str(default_value),
            description=description,
            id=id,
            name=name,
            position=position,
            prefix=prefix,
            derived_from=derived_from,
            postfix=postfix,
        )
        self.mapped_to: Optional[MappedIOStream] = mapped_to

        if encoding_format is not None:
            _validate_mime_type(encoding_format)
        self.encoding_format: Optional[List[str]] = encoding_format

    @staticmethod
    def generate_id(
        plan_id: str, position: Optional[int] = None, postfix: Optional[str] = None, name: Optional[str] = None
    ) -> str:
        """Generate an id for CommandInput."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="inputs", position=position, postfix=postfix, name=name
        )

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        return f"< {shlex.quote(self.actual_value)}" if self.mapped_to else ""

    def _get_default_name(self) -> str:
        return self._generate_name(base="input")

    def is_equal_to(self, other) -> bool:
        """Return if attributes that cause a change in the parameter, are the same."""
        if self.mapped_to:
            if not other.mapped_to or self.mapped_to.stream_type != other.mapped_to.stream_type:
                return False
        elif other.mapped_to:
            return False

        return super().is_equal_to(other)

    @staticmethod
    def _get_equality_attributes() -> List[str]:
        """Return a list of attributes values that determine if instances are equal."""
        return CommandParameterBase._get_equality_attributes() + ["encoding_format"]

    def derive(self, plan_id: str) -> "CommandInput":
        """Create a new ``CommandInput`` that is derived from self."""
        parameter = copy.copy(self)
        parameter.id = CommandInput.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix)
        return parameter


class HiddenInput(CommandInput):
    """An input to a command that is added by Renku and should be hidden from users."""


class CommandOutput(CommandParameterBase):
    """An output from a command."""

    def __init__(
        self,
        *,
        create_folder: bool = False,
        default_value: Any = None,
        description: Optional[str] = None,
        id: str,
        mapped_to: Optional[MappedIOStream] = None,
        name: Optional[str] = None,
        position: Optional[int] = None,
        prefix: Optional[str] = None,
        encoding_format: Optional[List[str]] = None,
        derived_from: Optional[str] = None,
        postfix: Optional[str] = None,
    ):
        assert isinstance(default_value, (Path, str)), f"Invalid value type for CommandOutput: {type(default_value)}"

        super().__init__(
            default_value=str(default_value),
            description=description,
            id=id,
            name=name,
            position=position,
            prefix=prefix,
            derived_from=derived_from,
            postfix=postfix,
        )
        self.create_folder: bool = create_folder
        self.mapped_to: Optional[MappedIOStream] = mapped_to

        if encoding_format is not None:
            _validate_mime_type(encoding_format)
        self.encoding_format: Optional[List[str]] = encoding_format

    @staticmethod
    def generate_id(
        plan_id: str, position: Optional[int] = None, postfix: str = None, name: Optional[str] = None
    ) -> str:
        """Generate an id for CommandOutput."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="outputs", position=position, postfix=postfix, name=name
        )

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        value = shlex.quote(self.actual_value)
        return f"> {value}" if self.mapped_to.stream_type == "stdout" else f" 2> {value}"

    def _get_default_name(self) -> str:
        return self._generate_name(base="output")

    def is_equal_to(self, other) -> bool:
        """Return if attributes that cause a change in the parameter, are the same."""
        if self.mapped_to:
            if not other.mapped_to or self.mapped_to.stream_type != other.mapped_to.stream_type:
                return False
        elif other.mapped_to:
            return False

        return super().is_equal_to(other)

    @staticmethod
    def _get_equality_attributes() -> List[str]:
        """Return a list of attributes values that determine if instances are equal."""
        return CommandParameterBase._get_equality_attributes() + ["encoding_format", "create_folder"]

    def derive(self, plan_id: str) -> "CommandOutput":
        """Create a new ``CommandOutput`` that is derived from self."""
        parameter = copy.copy(self)
        parameter.id = CommandOutput.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix)
        return parameter


class ParameterMapping(CommandParameterBase):
    """A mapping of child parameter(s) to a parent CompositePlan."""

    def __init__(
        self,
        *,
        default_value: Any,
        description: Optional[str] = None,
        id: str,
        name: Optional[str] = None,
        mapped_parameters: List[CommandParameterBase],
        **kwargs,
    ):
        super().__init__(default_value=default_value, description=description, id=id, name=name, **kwargs)

        self.mapped_parameters: List[CommandParameterBase] = mapped_parameters

    @staticmethod
    def generate_id(
        plan_id: str, position: Optional[int] = None, postfix: Optional[str] = None, name: Optional[str] = None
    ) -> str:
        """Generate an id for ParameterMapping."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="mappings", position=position, postfix=postfix, name=name
        )

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        return ""

    def _get_default_name(self) -> str:
        return self._generate_name(base="mapping")

    @CommandParameterBase.actual_value.setter  # type: ignore
    def actual_value(self, value):
        """Set the actual value to be used for execution."""
        self._v_actual_value = value
        self._v_actual_value_set = True

        for mapped_to in self.mapped_parameters:
            if not mapped_to.actual_value_set:
                mapped_to.actual_value = value

    @property
    def leaf_parameters(self) -> Iterator[CommandParameterBase]:
        """Return leaf (non-Mapping) parameters contained by this Mapping."""
        for mapped_to in self.mapped_parameters:
            if isinstance(mapped_to, ParameterMapping):
                yield from mapped_to.leaf_parameters
            else:
                yield mapped_to

    def derive(self, plan_id: str) -> "ParameterMapping":
        """Create a new ``CommandParameter`` that is derived from self."""
        parameter = copy.copy(self)
        parameter.id = ParameterMapping.generate_id(plan_id=plan_id, position=self.position, postfix=self.postfix)
        return parameter


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


def generate_parameter_name(parameter, kind) -> str:
    """Generate a name for an input, output, or parameter."""
    name = get_slug(parameter.prefix.strip(" -="), invalid_chars=["."]) if parameter.prefix else kind
    position = parameter.position if parameter.position is not None else uuid4().hex[:RANDOM_ID_LENGTH]
    return f"{name}-{position}"
