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
"""Classes for tracking parameter values in provenance."""

from pathlib import Path
from typing import Any, Union
from uuid import uuid4

from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.workflow.parameter import (
    CommandInput,
    CommandInputSchema,
    CommandOutput,
    CommandOutputSchema,
    CommandParameter,
    CommandParameterSchema,
)


class ParameterValue:
    """Value for a parameter in provenance."""

    def __init__(self, *, id: str):
        self.id = id

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a default id."""
        return f"{activity_id}/parameter-value/{uuid4()}"


class PathParameterValue(ParameterValue):
    """Value for a path parameter in provenance."""

    def __init__(self, *, id: str, parameter: Union[CommandInput, CommandOutput], path: Union[Path, str]):
        super().__init__(id=id)
        self.parameter: Union[CommandInput, CommandOutput] = parameter
        self.path: Union[Path, str] = str(path)


class VariableParameterValue(ParameterValue):
    """Value for a parameter in provenance."""

    def __init__(self, *, id: str, parameter: CommandParameter, value: Any):
        super().__init__(id=id)
        self.parameter: CommandParameter = parameter
        self.value: Any = value


class ParameterValueSchema(JsonLDSchema):
    """ParameterValue schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.ParameterValue, schema.PropertyValue]
        model = ParameterValue
        unknown = EXCLUDE

    id = fields.Id()


class PathParameterValueSchema(ParameterValueSchema):
    """PathParameterValue schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.PathParameterValue, schema.PropertyValue]
        model = PathParameterValue
        unknown = EXCLUDE

    parameter = Nested(schema.valueReference, [CommandInputSchema, CommandOutputSchema], reverse=True)
    path = fields.String(prov.atLocation)


class VariableParameterValueSchema(ParameterValueSchema):
    """VariableParameterValue schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.VariableParameterValue, schema.PropertyValue]
        model = VariableParameterValue
        unknown = EXCLUDE

    parameter = Nested(schema.valueReference, CommandParameterSchema, reverse=True)
    value = fields.Raw(schema.value)
