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
from pathlib import PurePosixPath
from typing import Any, List, Optional
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, fields, rdfs, renku, schema
from renku.core.models.workflow.parameters import MappedIOStream, MappedIOStreamSchema
from renku.core.utils.urls import get_slug

RANDOM_ID_LENGTH = 4


class CommandParameterBase:
    """Represents a parameter for a Plan."""

    def __init__(
        self,
        *,
        default_value: Any,
        description: str,
        id: str,
        label: str,
        name: str,
        position: Optional[int],
        prefix: str,
    ):
        self.default_value: Any = default_value
        self.description: str = description
        self.id: str = id
        self.label: str = label
        self.name: str = name
        self.position: Optional[int] = position
        self.prefix: str = prefix

        if not self.name:
            self.name = self._get_default_name()
        if not self.label:
            self.label = self._get_default_label()

    @staticmethod
    def _generate_id(plan_id: str, parameter_type: str, position: Optional[int], postfix: str = None) -> str:
        """Generate an id for parameters."""
        # https://localhost/plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/1
        # https://localhost/plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/stdin
        # https://localhost/plans/723fd784-9347-4081-84de-a6dbb067545b/inputs/dda5fcbf-0098-4917-be46-dc12f5f7b675
        position = str(position) if position is not None else str(uuid4())
        postfix = urllib.parse.quote(postfix) if postfix else position
        return f"{plan_id}/{parameter_type}/{postfix}"

    @property
    def role(self) -> str:
        """Return a unique role for this parameter within its Plan."""
        assert self.id, "Id is not set"
        return PurePosixPath(self.id).name

    def to_argv(self) -> List[Any]:
        """String representation (sames as cmd argument)."""
        if self.prefix:
            if self.prefix.endswith(" "):
                return [self.prefix[:-1], self.default_value]
            return [f"{self.prefix}{self.default_value}"]

        return [self.default_value]

    def _generate_name(self, base) -> str:
        name = get_slug(self.prefix.strip(" -=")) if self.prefix else base
        position = self.position or uuid4().hex[:RANDOM_ID_LENGTH]
        return f"{name}-{position}"

    def _get_default_label(self) -> str:
        raise NotImplementedError

    def _get_default_name(self) -> str:
        raise NotImplementedError


class CommandParameter(CommandParameterBase):
    """An argument to a command that is neither input nor output."""

    def __init__(
        self,
        *,
        default_value: Any = None,
        description: str = None,
        id: str,
        label: str = None,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            label=label,
            name=name,
            position=position,
            prefix=prefix,
        )

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandParameter."""
        return CommandParameterBase._generate_id(
            plan_id, parameter_type="parameters", position=position, postfix=postfix
        )

    def _get_default_label(self) -> str:
        return f"Command Parameter '{self.default_value}'"

    def _get_default_name(self) -> str:
        return self._generate_name(base="parameter")


class CommandInput(CommandParameterBase):
    """An input to a command."""

    def __init__(
        self,
        *,
        default_value: Any = None,
        description: str = None,
        id: str,
        label: str = None,
        mapped_to: MappedIOStream = None,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            label=label,
            name=name,
            position=position,
            prefix=prefix,
        )
        self.mapped_to: MappedIOStream = mapped_to

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandInput."""
        return CommandParameterBase._generate_id(plan_id, parameter_type="inputs", position=position, postfix=postfix)

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        return f" < {self.default_value}" if self.mapped_to else ""

    def _get_default_label(self) -> str:
        return f"Command Input '{self.default_value}'"

    def _get_default_name(self) -> str:
        return self._generate_name(base="input")


class CommandOutput(CommandParameterBase):
    """An output from a command."""

    def __init__(
        self,
        *,
        create_folder: bool = False,
        default_value: Any = None,
        description: str = None,
        id: str,
        label: str = None,
        mapped_to: MappedIOStream = None,
        name: str = None,
        position: Optional[int] = None,
        prefix: str = None,
    ):
        super().__init__(
            default_value=default_value,
            description=description,
            id=id,
            label=label,
            name=name,
            position=position,
            prefix=prefix,
        )
        self.create_folder: bool = create_folder
        self.mapped_to: MappedIOStream = mapped_to

    @staticmethod
    def generate_id(plan_id: str, position: Optional[int] = None, postfix: str = None) -> str:
        """Generate an id for CommandOutput."""
        return CommandParameterBase._generate_id(plan_id, parameter_type="outputs", position=position, postfix=postfix)

    def to_stream_representation(self) -> str:
        """Input stream representation."""
        if not self.mapped_to:
            return ""

        return f" > {self.default_value}" if self.mapped_to.stream_type == "stdout" else f" 2> {self.default_value}"

    def _get_default_label(self) -> str:
        return f"Command Output '{self.default_value}'"

    def _get_default_name(self) -> str:
        return self._generate_name(base="output")


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
    label = fields.String(rdfs.label, missing=None)
    name = fields.String(schema.name, missing=None)
    position = fields.Integer(renku.position, missing=None)
    prefix = fields.String(renku.prefix, missing=None)


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


class CommandOutputSchema(CommandParameterBaseSchema):
    """CommandOutput schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.CommandOutput]
        model = CommandOutput
        unknown = EXCLUDE

    create_folder = fields.Boolean(renku.createFolder, missing=False)
    mapped_to = Nested(renku.mappedTo, MappedIOStreamSchema, missing=None)
