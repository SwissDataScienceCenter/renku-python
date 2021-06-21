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
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Dict, List
from uuid import uuid4

from marshmallow import EXCLUDE
from werkzeug.utils import secure_filename

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.entities import Entity
from renku.core.models.workflow import parameters as old_parameter
from renku.core.models.workflow.parameter import (
    CommandInput,
    CommandInputSchema,
    CommandOutput,
    CommandOutputSchema,
    CommandParameter,
    CommandParameterSchema,
)
from renku.core.models.workflow.run import Run
from renku.core.utils.urls import get_host

MAX_GENERATED_NAME_LENGTH = 25


class Plan:
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
        outputs: List[CommandOutput] = None,
        success_codes: List[int] = None,
    ):
        self.command: str = command
        self.description: str = description
        self.id: str = id
        self.inputs: List[CommandInput] = inputs or []
        self.invalidated_at: datetime = invalidated_at
        self.keywords: List[str] = keywords or []
        self.name: str = name
        self.outputs: List[CommandOutput] = outputs or []
        self.parameters: List[CommandParameter] = parameters or []
        self.success_codes: List[int] = success_codes or []

        if not self.name:
            self.name = self._get_default_name()

    def __repr__(self):
        return self.name

    @classmethod
    def from_run(cls, run: Run, hostname: str):
        """Create a Plan from a Run."""
        assert not run.subprocesses, f"Cannot create a Plan from a Run with subprocesses: {run._id}"

        def extract_run_uuid(run_id: str) -> str:
            # https://localhost/runs/723fd784-9347-4081-84de-a6dbb067545b/
            return run_id.rstrip("/").rsplit("/", maxsplit=1)[-1]

        uuid = extract_run_uuid(run._id)
        plan_id = cls.generate_id(hostname=hostname, uuid=uuid)

        def convert_argument(argument: old_parameter.CommandArgument) -> CommandParameter:
            """Convert an old CommandArgument to a new CommandParameter."""
            assert isinstance(argument, old_parameter.CommandArgument)

            return CommandParameter(
                default_value=argument.value,
                description=argument.description,
                id=CommandParameter.generate_id(plan_id=plan_id, postfix=PurePosixPath(argument._id).name),
                label=None,
                name=argument.name,
                position=argument.position,
                prefix=argument.prefix,
            )

        def convert_input(input: old_parameter.CommandInput) -> CommandInput:
            """Convert an old CommandInput to a new CommandInput."""
            assert isinstance(input, old_parameter.CommandInput)

            return CommandInput(
                default_value=input.consumes.path,
                description=input.description,
                id=CommandInput.generate_id(plan_id=plan_id, postfix=PurePosixPath(input._id).name),
                label=None,
                mapped_to=input.mapped_to,
                name=input.name,
                position=input.position,
                prefix=input.prefix,
            )

        def convert_output(output: old_parameter.CommandOutput) -> CommandOutput:
            """Convert an old CommandOutput to a new CommandOutput."""
            assert isinstance(output, old_parameter.CommandOutput)

            return CommandOutput(
                create_folder=output.create_folder,
                default_value=output.produces.path,
                description=output.description,
                id=CommandOutput.generate_id(plan_id=plan_id, postfix=PurePosixPath(output._id).name),
                label=None,
                mapped_to=output.mapped_to,
                name=output.name,
                position=output.position,
                prefix=output.prefix,
            )

        return cls(
            command=run.command,
            description=run.description,
            id=plan_id,
            inputs=[convert_input(i) for i in run.inputs],
            keywords=run.keywords,
            name=run.name,
            outputs=[convert_output(o) for o in run.outputs],
            parameters=[convert_argument(a) for a in run.arguments],
            success_codes=run.successcodes,
        )

    @staticmethod
    def generate_id(hostname: str, uuid: str) -> str:
        """Generate an identifier for Plan."""
        uuid = uuid or str(uuid4())
        return f"https://{hostname}/plans/{uuid}"

    def _get_default_name(self) -> str:
        name = "-".join(str(a) for a in self.to_argv())
        if not name:
            return uuid4().hex[:MAX_GENERATED_NAME_LENGTH]

        name = secure_filename(name)
        rand_length = 5
        return f"{name[:MAX_GENERATED_NAME_LENGTH - rand_length -1]}-{uuid4().hex[:rand_length]}"

    def assign_new_id(self):
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the parameters' list.
        """
        current_uuid = self._extract_uuid()
        new_uuid = str(uuid4())
        self.id = self.id.replace(current_uuid, new_uuid)
        self.parameters = copy.deepcopy(self.parameters)
        for a in self.parameters:
            a.id = a.id.replace(current_uuid, new_uuid)

    def _extract_uuid(self) -> str:
        path_start = self.id.find("/plans/")
        return self.id[path_start + len("/plans/") :]

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

    def to_argv(self) -> List[Any]:
        """Convert a Plan into argv list."""
        arguments = itertools.chain(self.inputs, self.outputs, self.parameters)
        arguments = filter(lambda a: a.position is not None, arguments)
        arguments = sorted(arguments, key=lambda a: a.position)

        argv = self.command.split(" ") if self.command else []
        argv.extend(e for a in arguments for e in a.to_argv())

        return argv

    def to_run(self, client, entities_cache: Dict[str, Entity]) -> Run:
        """Create a Run."""
        uuid = self._extract_uuid()
        host = get_host(client)
        # TODO: This won't work if plan_id was randomly generated; for PoC it's OK.
        run_id = f"https://{host}/runs/{uuid}"

        def get_entity(path: str) -> Entity:
            entity = entities_cache.get(path)
            if not entity:
                entity = Entity.from_revision(client=client, path=path, revision="HEAD")
                entities_cache[path] = entity
            return entity

        def convert_parameter(argument: CommandParameter) -> old_parameter.CommandArgument:
            return old_parameter.CommandArgument(
                description=argument.description,
                id=argument.id.replace(self.id, run_id),
                label=None,
                name=argument.name,
                position=argument.position,
                prefix=argument.prefix,
                value=argument.default_value,
            )

        def convert_input(input: CommandInput) -> old_parameter.CommandInput:
            return old_parameter.CommandInput(
                consumes=get_entity(input.default_value),
                description=input.description,
                id=input.id.replace(self.id, run_id),
                label=None,
                mapped_to=input.mapped_to,
                name=input.name,
                position=input.position,
                prefix=input.prefix,
            )

        def convert_output(output: CommandOutput) -> old_parameter.CommandOutput:
            return old_parameter.CommandOutput(
                create_folder=output.create_folder,
                description=output.description,
                id=output.id.replace(self.id, run_id),
                label=None,
                mapped_to=output.mapped_to,
                name=output.name,
                position=output.position,
                prefix=output.prefix,
                produces=get_entity(output.default_value),
            )

        return Run(
            arguments=[convert_parameter(p) for p in self.parameters],
            command=self.command,
            id=run_id,
            inputs=[convert_input(i) for i in self.inputs],
            outputs=[convert_output(o) for o in self.outputs],
            successcodes=self.success_codes,
        )


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork]
        model = Plan
        unknown = EXCLUDE

    command = fields.String(renku.command, missing=None)
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, missing=None)
    invalidated_at = fields.DateTime(prov.invalidatedAtTime, add_value_types=True)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    name = fields.String(schema.name, missing=None)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, missing=None)
    parameters = Nested(renku.hasArguments, CommandParameterSchema, many=True, missing=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), missing=[0])
