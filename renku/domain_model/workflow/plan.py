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
"""Represent run templates."""

import copy
import itertools
import re
from abc import ABC
from datetime import datetime
from typing import Any, List, Optional, Set, Tuple, Union, cast
from uuid import uuid4

import marshmallow
from werkzeug.utils import secure_filename

from renku.core import errors
from renku.core.util.datetime8601 import local_now
from renku.domain_model.provenance.agent import Person
from renku.domain_model.provenance.annotation import Annotation
from renku.domain_model.workflow.parameter import (
    CommandInput,
    CommandOutput,
    CommandParameter,
    CommandParameterBase,
    HiddenInput,
)
from renku.infrastructure.database import Persistent

MAX_GENERATED_NAME_LENGTH = 25


class AbstractPlan(Persistent, ABC):
    """Abstract base class for all plans."""

    date_created: datetime
    creators: List[Person] = list()

    def __init__(
        self,
        *,
        description: Optional[str] = None,
        id: str,
        date_created: Optional[datetime] = None,
        date_modified: Optional[datetime] = None,
        date_removed: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
        name: Optional[str] = None,
        project_id: Optional[str] = None,
        derived_from: Optional[str] = None,
        creators: Optional[List[Person]] = None,
    ):
        self.description: Optional[str] = description
        self.id: str = id
        self.date_created: datetime = date_created or local_now()
        self.date_modified: datetime = date_modified or local_now()
        self.date_removed: Optional[datetime] = date_removed
        self.keywords: List[str] = keywords or []

        if creators:
            self.creators = creators

        self.project_id: Optional[str] = project_id
        self.derived_from: Optional[str] = derived_from

        if name is None:
            self.name: str = self._get_default_name()
        else:
            self.validate_name(name)
            self.name = name

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self.name}'>"

    @property
    def deleted(self) -> bool:
        """True if plan is deleted."""
        return self.date_removed is not None

    @staticmethod
    def generate_id(*, uuid: Optional[str] = None, **_) -> str:
        """Generate an identifier for Plan."""
        uuid = uuid or uuid4().hex
        return f"/plans/{uuid}"

    def _get_default_name(self) -> str:
        name = "-".join(str(a).replace(".", "_") for a in self.to_argv())
        if not name:
            return uuid4().hex[:MAX_GENERATED_NAME_LENGTH]

        name = secure_filename(name)
        rand_length = 5
        return f"{name[:MAX_GENERATED_NAME_LENGTH - rand_length -1]}-{uuid4().hex[:rand_length]}"

    @staticmethod
    def validate_name(name: str):
        """Check a name for invalid characters."""
        validate_plan_name(name=name)

    def assign_new_id(self, *, uuid: Optional[str] = None) -> str:
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the parameters' list.
        """
        current_uuid = self._extract_uuid()
        new_uuid = uuid or uuid4().hex
        self.id = self.id.replace(current_uuid, new_uuid)

        # NOTE: We also need to re-assign the _p_oid since identifier has changed
        self.reassign_oid()

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

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> Optional["AbstractPlan"]:
        """Return the workflow a parameter belongs to."""
        raise NotImplementedError()

    def derive(self, creator: Optional[Person] = None) -> "AbstractPlan":
        """Create a new ``AbstractPlan`` that is derived from self."""
        raise NotImplementedError()

    def is_derivation(self) -> bool:
        """Return if an ``AbstractPlan`` has correct derived_from."""
        raise NotImplementedError()

    def delete(self):
        """Mark a plan as deleted.

        NOTE: Don't call this function for deleting plans since it doesn't delete the whole plan derivatives chain. Use
        renku.core.workflow.plan::remove_plan instead.
        """
        self.unfreeze()
        self.date_removed = local_now()
        self.freeze()


class Plan(AbstractPlan):
    """Represent a `renku run` execution template."""

    annotations: List[Annotation] = list()

    hidden_inputs: List[HiddenInput] = list()
    """Includes a list of dependencies that are defined by Renku and should be hidden from users."""

    def __init__(
        self,
        *,
        annotations: Optional[List[Annotation]] = None,
        command: str,
        creators: Optional[List[Person]] = None,
        date_created: Optional[datetime] = None,
        date_modified: Optional[datetime] = None,
        derived_from: Optional[str] = None,
        description: Optional[str] = None,
        hidden_inputs: List[HiddenInput] = None,
        id: str,
        inputs: Optional[List[CommandInput]] = None,
        date_removed: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
        name: Optional[str] = None,
        outputs: Optional[List[CommandOutput]] = None,
        parameters: Optional[List[CommandParameter]] = None,
        project_id: Optional[str] = None,
        success_codes: Optional[List[int]] = None,
    ):
        self.annotations: List[Annotation] = annotations or []
        self.command: str = command
        self.hidden_inputs: List[HiddenInput] = hidden_inputs or []
        self.inputs: List[CommandInput] = inputs or []
        self.outputs: List[CommandOutput] = outputs or []
        self.parameters: List[CommandParameter] = parameters or []
        self.success_codes: List[int] = success_codes or []
        super().__init__(
            id=id,
            description=description,
            date_created=date_created,
            date_modified=date_modified,
            date_removed=date_removed,
            keywords=keywords,
            name=name,
            project_id=project_id,
            derived_from=derived_from,
            creators=creators,
        )

        # NOTE: Validate plan
        duplicates = get_duplicate_arguments_names(plan=self)
        if duplicates:
            duplicates_string = ", ".join(duplicates)
            raise errors.ParameterError(f"Duplicate input, output or parameter names found: {duplicates_string}")

    @property
    def keywords_csv(self) -> str:
        """Comma-separated list of keywords associated with workflow."""
        return ", ".join(self.keywords)

    def is_equal_to(self, other: "Plan") -> bool:
        """Return true if plan hasn't changed from the other plan."""

        def are_equal_with_order(values, other_values):
            return len(values) == len(other_values) and all(s.is_equal_to(o) for s, o in zip(values, other_values))

        def are_equal(values, other_values):
            return len(values) == len(other_values) and set(values) == set(other_values)

        # TODO: Include ``annotations`` if it is added to the workflow definition file
        return (
            self.name == other.name
            and self.description == other.description
            and self.project_id == other.project_id
            and self.command == other.command
            and are_equal(self.success_codes, other.success_codes)
            and are_equal(self.keywords, other.keywords)
            and are_equal(self.creators, other.creators)
            and are_equal_with_order(self.inputs, other.inputs)
            and are_equal_with_order(self.outputs, other.outputs)
            and are_equal_with_order(self.parameters, other.parameters)
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

        for parameter in self.inputs + self.outputs + self.parameters:  # type: ignore
            if parameter.name == reference:
                return parameter

        raise errors.ParameterNotFoundError(reference, self.name)

    def find_parameter(self, parameter: CommandParameterBase) -> bool:
        """Find if a parameter exists on this plan."""
        return any(
            parameter.id == p.id and parameter.actual_value == p.actual_value
            for p in self.inputs + self.outputs + self.parameters  # type: ignore
        )

    def get_parameter_path(self, parameter: CommandParameterBase):
        """Get the path to a parameter inside this plan."""
        if self.find_parameter(parameter):
            return [self]

        return None

    def get_parameter_by_id(self, parameter_id: str) -> Optional[CommandParameterBase]:
        """Get a parameter on this plan by id."""
        return next(
            (p for p in self.inputs + self.outputs + self.parameters if parameter_id == p.id), None  # type: ignore
        )

    def find_parameter_workflow(self, parameter: CommandParameterBase) -> Optional["Plan"]:
        """Return the workflow a parameter belongs to."""
        if self.find_parameter(parameter):
            return self
        return None

    def assign_new_id(self, *, uuid: Optional[str] = None):
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the parameters' list.
        """
        new_uuid = super().assign_new_id(uuid=uuid)
        current_uuid = self._extract_uuid()
        if hasattr(self, "parameters"):
            self.parameters = copy.deepcopy(self.parameters)

            for a in self.parameters:
                a.id = a.id.replace(current_uuid, new_uuid)

    def derive(self, creator: Optional[Person] = None) -> "Plan":
        """Create a new ``Plan`` that is derived from self."""
        derived = copy.copy(self)
        derived.derived_from = self.id
        derived.date_created = local_now()
        derived.parameters = self.parameters.copy()
        derived.inputs = self.inputs.copy()
        derived.keywords = copy.deepcopy(self.keywords)
        derived.outputs = self.outputs.copy()
        derived.success_codes = self.success_codes.copy()
        derived.assign_new_id()

        if creator and hasattr(creator, "email") and not any(c for c in self.creators if c.email == creator.email):
            self.creators.append(creator)

        return derived

    def is_derivation(self) -> bool:
        """Return if an ``Plan`` has correct derived_from."""
        return self.derived_from is not None and self.id != self.derived_from

    def to_argv(self, with_streams: bool = False, quote_string: bool = True) -> List[Any]:
        """Convert a Plan into argv list."""
        arguments = itertools.chain(self.inputs, self.outputs, self.parameters)
        filtered_arguments = filter(lambda a: a.position is not None and not getattr(a, "mapped_to", None), arguments)
        arguments = sorted(filtered_arguments, key=lambda a: a.position)  # type: ignore

        argv = self.command.split(" ") if self.command else []
        argv.extend(e for a in arguments for e in a.to_argv(quote_string=quote_string))

        if with_streams:
            arguments = itertools.chain(self.inputs, self.outputs, self.parameters)
            filtered_arguments = filter(lambda a: a.position is not None and getattr(a, "mapped_to", None), arguments)
            argv.extend(a.to_stream_representation() for a in filtered_arguments)  # type: ignore

        return argv

    def set_parameters_from_strings(self, params_strings: List[str]) -> None:
        """Set parameters by parsing parameters strings."""
        for param_string in params_strings:
            name, value = param_string.split("=", maxsplit=1)
            found = False
            for collection in cast(List[List[CommandParameterBase]], [self.inputs, self.outputs, self.parameters]):
                for i, param in enumerate(collection):
                    if param.name == name:
                        new_param = param.derive(plan_id=self.id)
                        new_param.default_value = value
                        collection[i] = new_param
                        found = True
                        break
                if found:
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

    def get_field_by_id(self, id: str) -> Union[CommandInput, CommandOutput, CommandParameter]:
        """Return an in Input/Output/Parameter by its id."""
        for field in itertools.chain(self.inputs, self.outputs, self.parameters):
            if field.id == id:
                return field  # type: ignore

        raise errors.ParameterError(f"Parameter {id} not found on plan {self.id}.")


class PlanDetailsJson(marshmallow.Schema):
    """Serialize a plan to a response object."""

    name = marshmallow.fields.String(required=True)
    full_command = marshmallow.fields.String(data_key="command")
    derived_from = marshmallow.fields.String()
    description = marshmallow.fields.String()
    keywords = marshmallow.fields.List(marshmallow.fields.String())
    id = marshmallow.fields.String()


def get_duplicate_arguments_names(plan: Plan) -> List[str]:
    """Return a list of duplicate inputs/outputs/parameters names in a plan."""
    all_names = [p.name for p in itertools.chain(plan.inputs, plan.outputs, plan.parameters) if p.name]
    seen: Set[str] = set()

    duplicates: List[str] = []
    for n in all_names:
        if n in seen:
            duplicates.append(n)
        else:
            seen.add(n)

    return duplicates


def validate_plan_name(name: str, extra_valid_characters: str = "_-"):
    """Check a name for invalid characters."""
    if not re.match(f"^[a-zA-Z0-9][a-zA-Z0-9{extra_valid_characters}]+$", name):
        raise errors.ParameterError(
            f"Name '{name}' contains illegal characters. Only English letters, numbers, _ and - are allowed."
        )
