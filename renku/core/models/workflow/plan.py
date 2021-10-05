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
"""Represent run templates."""

import copy
import itertools
import re
from abc import ABC
from datetime import datetime
from typing import Any, List, Tuple
from uuid import uuid4

import marshmallow
from werkzeug.utils import secure_filename

from renku.core import errors
from renku.core.metadata.database import Persistent
from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.workflow.parameter import (
    CommandInput,
    CommandInputSchema,
    CommandOutput,
    CommandOutputSchema,
    CommandParameter,
    CommandParameterBase,
    CommandParameterSchema,
)

MAX_GENERATED_NAME_LENGTH = 25


class AbstractPlan(Persistent, ABC):
    """Abstract base class for all plans."""

    def __init__(
        self,
        *,
        description: str = None,
        id: str,
        invalidated_at: datetime = None,
        keywords: List[str] = None,
        name: str = None,
        project_id: str = None,
        derived_from: str = None,
    ):
        self.description: str = description
        self.id: str = id
        self.invalidated_at: datetime = invalidated_at
        self.keywords: List[str] = keywords or []
        self.name: str = name
        self.project_id: str = project_id
        self.derived_from: str = derived_from

        if not self.name:
            self.name = self._get_default_name()
        else:
            AbstractPlan.validate_name(name)

    @staticmethod
    def generate_id(uuid: str = None) -> str:
        """Generate an identifier for Plan."""
        uuid = uuid or uuid4().hex
        return f"/plans/{uuid}"

    def _get_default_name(self) -> str:
        name = "-".join(str(a) for a in self.to_argv())
        if not name:
            return uuid4().hex[:MAX_GENERATED_NAME_LENGTH]

        name = secure_filename(name)
        rand_length = 5
        return f"{name[:MAX_GENERATED_NAME_LENGTH - rand_length -1]}-{uuid4().hex[:rand_length]}"

    @staticmethod
    def validate_name(name: str):
        """Check a name for invalid characters."""
        if not re.match("[a-zA-Z0-9-_]+", name):
            raise errors.ParameterError(
                f"Name {name} contains illegal characters. Only characters, numbers, _ and - are allowed."
            )

    def assign_new_id(self) -> str:
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the parameters' list.
        """
        current_uuid = self._extract_uuid()
        new_uuid = uuid4().hex
        self.id = self.id.replace(current_uuid, new_uuid)

        return new_uuid

    def _extract_uuid(self) -> str:
        path_start = self.id.find("/plans/")
        return self.id[path_start + len("/plans/") :]

    def resolve_mapping_path(self, mapping_path: str) -> Tuple[CommandParameterBase, "Plan"]:
        """Resolve a mapping path to its reference parameter."""
        raise NotImplementedError()

    def resolve_direct_reference(self, reference: str) -> CommandParameterBase:
        """Resolve a direct parameter reference."""
        raise NotImplementedError()

    def find_parameter(self, parameter: CommandParameterBase) -> bool:
        """Find if a parameter exists on this plan."""
        raise NotImplementedError()

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> "Plan":
        """Return the workflow a parameter belongs to."""
        raise NotImplementedError()


class Plan(AbstractPlan):
    """Represent a `renku run` execution template."""

    def __init__(
        self,
        *,
        parameters: List[CommandParameter] = None,
        command: str = None,
        description: str = None,
        id: str,
        inputs: List[CommandInput] = None,
        invalidated_at: datetime = None,
        keywords: List[str] = None,
        name: str = None,
        derived_from: str = None,
        project_id: str = None,
        outputs: List[CommandOutput] = None,
        success_codes: List[int] = None,
    ):
        self.command: str = command
        self.inputs: List[CommandInput] = inputs or []
        self.outputs: List[CommandOutput] = outputs or []
        self.parameters: List[CommandParameter] = parameters or []
        self.success_codes: List[int] = success_codes or []
        super().__init__(
            id=id,
            description=description,
            invalidated_at=invalidated_at,
            keywords=keywords,
            name=name,
            project_id=project_id,
            derived_from=derived_from,
        )

    def is_similar_to(self, other: "Plan") -> bool:
        """Return true if plan has the same inputs/outputs/arguments as another plan."""

        def get_input_patterns(plan: Plan):
            return {e.default_value for e in plan.inputs}

        def get_output_patterns(plan: Plan):
            return {e.default_value for e in plan.outputs}

        def get_parameters(plan: Plan):
            return {(a.position, a.prefix, a.default_value) for a in plan.parameters}

        # TODO: Check order of inputs/outputs/parameters as well after sorting by position
        return (
            self.command == other.command
            and set(self.success_codes) == set(other.success_codes)
            and get_input_patterns(self) == get_input_patterns(other)
            and get_output_patterns(self) == get_output_patterns(other)
            and get_parameters(self) == get_parameters(other)
        )

    def resolve_mapping_path(self, mapping_path: str) -> Tuple[CommandParameterBase, "Plan"]:
        """Resolve a mapping path to its reference parameter."""

        parts = mapping_path.split(".", maxsplit=1)

        if len(parts) > 1:
            raise errors.ParameterNotFoundError(mapping_path, self.name)

        return self.resolve_direct_reference(parts[0]), self

    def resolve_direct_reference(self, reference: str) -> CommandParameterBase:
        """Resolve a direct parameter reference."""
        try:
            if reference.startswith("@input"):
                return self.inputs[int(reference[6:]) - 1]
            elif reference.startswith("@output"):
                return self.outputs[int(reference[7:]) - 1]
            elif reference.startswith("@param"):
                return self.parameters[int(reference[6:]) - 1]
        except (ValueError, IndexError):
            raise errors.ParameterNotFoundError(reference, self.name)

        for parameter in self.inputs + self.outputs + self.parameters:
            if parameter.name == reference:
                return parameter

        raise errors.ParameterNotFoundError(reference, self.name)

    def find_parameter(self, parameter: CommandParameterBase) -> bool:
        """Find if a parameter exists on this plan."""
        return parameter in self.inputs + self.outputs + self.parameters

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> "Plan":
        """Return the workflow a parameter belongs to."""
        if self.find_parameter(parameter):
            return self

    def assign_new_id(self):
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the parameters' list.
        """
        new_uuid = super().assign_new_id()
        current_uuid = self._extract_uuid()
        if hasattr(self, "parameters"):
            self.parameters = copy.deepcopy(self.parameters)

            for a in self.parameters:
                a.id = a.id.replace(current_uuid, new_uuid)

    def derive(self) -> "Plan":
        """Create a new ``Plan`` that is derived from self."""
        derived = Plan(
            parameters=self.parameters.copy(),
            command=self.command,
            description=self.description,
            id=self.id,
            inputs=self.inputs.copy(),
            invalidated_at=self.invalidated_at,
            keywords=self.keywords.copy(),
            name=self.name,
            derived_from=self.id,
            outputs=self.outputs.copy(),
            success_codes=self.success_codes.copy(),
        )
        derived.assign_new_id()
        return derived

    @property
    def keywords_csv(self):
        """Comma-separated list of keywords associated with workflow."""
        return ", ".join(self.keywords)

    def to_argv(self) -> List[Any]:
        """Convert a Plan into argv list."""
        arguments = itertools.chain(self.inputs, self.outputs, self.parameters)
        arguments = filter(lambda a: a.position is not None and not getattr(a, "mapped_to", None), arguments)
        arguments = sorted(arguments, key=lambda a: a.position)

        argv = self.command.split(" ") if self.command else []
        argv.extend(e for a in arguments for e in a.to_argv())

        return argv

    def set_parameters_from_strings(self, params_strings: List[str]) -> None:
        """Set parameters by parsing parameters strings."""
        for param_string in params_strings:
            name, value = param_string.split("=", maxsplit=1)
            for param in self.inputs + self.outputs + self.parameters:
                if param.name == name:
                    param.default_value = value
                    break
            else:
                self.parameters.append(
                    CommandParameter(default_value=value, id=CommandParameter.generate_id(plan_id=self.id), name=name)
                )

    def copy(self):
        """Create a copy of this plan.

        Required where a plan is used several times in a workflow but we need to set different values on them.
        """
        return copy.deepcopy(self)


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork]
        model = Plan
        unknown = marshmallow.EXCLUDE

    command = fields.String(renku.command, missing=None)
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, missing=None)
    invalidated_at = fields.DateTime(prov.invalidatedAtTime, add_value_types=True)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    name = fields.String(schema.name, missing=None)
    derived_from = fields.String(prov.wasDerivedFrom, missing=None)
    project_id = fields.IRI(renku.hasPlan, reverse=True)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, missing=None)
    parameters = Nested(renku.hasArguments, CommandParameterSchema, many=True, missing=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), missing=[0])


class PlanDetailsJson(marshmallow.Schema):
    """Serialize a plan to a response object."""

    name = marshmallow.fields.String(required=True)
    derived_from = marshmallow.fields.String()
    title = marshmallow.fields.String()
    description = marshmallow.fields.String()
    keywords = marshmallow.fields.List(marshmallow.fields.String())
    id = marshmallow.fields.String()
