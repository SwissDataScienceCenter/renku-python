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
import pathlib
import urllib.parse
import uuid
from pathlib import Path
from typing import Sequence

from marshmallow import EXCLUDE
from werkzeug.utils import secure_filename

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.entities import Entity
from renku.core.models.workflow.parameters import (
    CommandArgument,
    CommandArgumentSchema,
    CommandInput,
    CommandInputTemplate,
    CommandInputTemplateSchema,
    CommandOutput,
    CommandOutputTemplate,
    CommandOutputTemplateSchema,
)
from renku.core.models.workflow.run import Run
from renku.core.utils.urls import get_host

MAX_GENERATED_NAME_LENGTH = 25


class Plan:
    """Represent a `renku run` execution template."""

    def __init__(
        self,
        id_,
        arguments=None,
        command=None,
        description=None,
        inputs=None,
        keywords=None,
        name=None,
        outputs=None,
        success_codes=None,
    ):
        """Initialize."""
        self.arguments: Sequence[CommandArgument] = arguments or []
        self.command = command
        self.description = description
        self.id_ = id_
        self.inputs: Sequence[CommandInputTemplate] = inputs or []
        self.keywords = keywords or []
        self.name = name or f"{secure_filename(self.command)}-{uuid.uuid4().hex}"
        self.outputs: Sequence[CommandOutputTemplate] = outputs or []
        self.success_codes = success_codes or []

    def __repr__(self):
        """String representation."""
        return self.name

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        elif not isinstance(data, dict):
            raise ValueError(data)

        return PlanSchema(flattened=True).load(data)

    @classmethod
    def from_run(cls, run: Run, client):
        """Create a Plan from a Run."""
        assert not run.subprocesses, f"Cannot create from a Run with subprocesses: {run._id}"

        uuid_ = _extract_run_uuid(run._id)
        plan_id = cls.generate_id(client=client, uuid_=uuid_)

        inputs = [_convert_command_input(i, plan_id) for i in run.inputs]
        outputs = [_convert_command_output(o, plan_id) for o in run.outputs]

        return cls(
            arguments=run.arguments,
            command=run.command,
            description=run.description,
            id_=plan_id,
            inputs=inputs,
            keywords=run.keywords,
            name=run.name or cls._generate_name(run),
            outputs=outputs,
            success_codes=run.successcodes,
        )

    @staticmethod
    def generate_id(client, uuid_):
        """Generate an identifier for the plan."""
        uuid_ = uuid_ or str(uuid.uuid4())
        host = get_host(client)
        return urllib.parse.urljoin(f"https://{host}", pathlib.posixpath.join("plans", uuid_))

    @staticmethod
    def _generate_name(run):
        if not run:
            return uuid.uuid4().hex[:MAX_GENERATED_NAME_LENGTH]

        name = "-".join(str(a) for a in run.to_argv())
        name = secure_filename(name)
        rand_length = 5
        return f"{name[:MAX_GENERATED_NAME_LENGTH - rand_length -1]}-{uuid.uuid4().hex[:rand_length]}"

    def assign_new_id(self):
        """Assign a new UUID.

        This is required only when there is another plan which is exactly the same except the arguments list.
        """
        path_start = self.id_.find("/plans/")
        old_uuid = self.id_[path_start + len("/plans/") :]
        new_uuid = str(uuid.uuid4())
        self.id_ = self.id_.replace(old_uuid, new_uuid)
        self.arguments = copy.deepcopy(self.arguments)
        for a in self.arguments:
            a._id = a._id.replace(old_uuid, new_uuid)

    def _extract_uuid(self):
        path_start = self.id_.find("/plans/")
        return self.id_[path_start + len("/plans/") :]

    def to_jsonld(self):
        """Create JSON-LD."""
        return PlanSchema(flattened=True).dump(self)

    def is_similar_to(self, other) -> bool:
        """Return true if plan has the same inputs/outputs/arguments as another plan."""

        def get_input_patterns(plan: Plan):
            return {e.default_value for e in plan.inputs}

        def get_output_patterns(plan: Plan):
            return {e.default_value for e in plan.outputs}

        def get_arguments(plan: Plan):
            return {(a.position, a.prefix, a.value) for a in plan.arguments}

        # TODO: Check order of inputs/outputs/parameters as well after sorting by position
        return (
            self.command == other.command
            and set(self.success_codes) == set(other.success_codes)
            and get_input_patterns(self) == get_input_patterns(other)
            and get_output_patterns(self) == get_output_patterns(self)
            and get_arguments(self) == get_arguments(other)
        )

    def to_run(self, client, entities_cache) -> Run:
        """Create a Run."""

        def convert_input(input_: CommandInputTemplate) -> CommandInput:
            entity = entities_cache.get(input_.default_value)
            if not entity:
                entity = Entity.from_revision(client=client, path=input_.default_value, revision="HEAD")
                entities_cache[input_.default_value] = entity

            return CommandInput(
                id=input_._id.replace(self.id_, run_id),
                consumes=entity,
                description=input_.description,
                mapped_to=input_.mapped_to,
                name=input_.name,
                position=input_.position,
                prefix=input_.prefix,
            )

        def convert_output(output: CommandOutputTemplate) -> CommandOutput:
            entity = entities_cache.get(output.default_value)
            if not entity:
                entity = Entity.from_revision(client=client, path=output.default_value, revision="HEAD")
                entities_cache[output.default_value] = entity

            return CommandOutput(
                id=output._id.replace(self.id_, run_id),
                create_folder=output.create_folder,
                description=output.description,
                mapped_to=output.mapped_to,
                name=output.name,
                position=output.position,
                prefix=output.prefix,
                produces=entity,
            )

        uuid_ = self._extract_uuid()
        host = get_host(client)
        # TODO: This won't work if plan_id was randomly generated; for PoC it's OK.
        run_id = urllib.parse.urljoin(f"https://{host}", pathlib.posixpath.join("runs", uuid_))

        inputs = [convert_input(i) for i in self.inputs]
        outputs = [convert_output(o) for o in self.outputs]

        return Run(
            arguments=self.arguments,
            command=self.command,
            id=run_id,
            inputs=inputs,
            outputs=outputs,
            successcodes=self.success_codes,
        )


def _extract_run_uuid(run_id) -> str:
    # https://localhost/runs/723fd784-9347-4081-84de-a6dbb067545b
    parsed_url = urllib.parse.urlparse(run_id)
    return parsed_url.path[len("/runs/") :]


def _convert_command_input(input_: CommandInput, plan_id) -> CommandInputTemplate:
    """Convert a CommandInput to CommandInputTemplate."""
    assert isinstance(input_, CommandInput)

    # TODO: add a '**' if this is a directory
    # TODO: For now this is always a fully qualified path; in future this might be a glob pattern.
    consumes = input_.consumes.path

    return CommandInputTemplate(
        id=CommandInputTemplate.generate_id(plan_id=plan_id, id_=Path(input_._id).name),
        default_value=consumes,
        description=input_.description,
        mapped_to=input_.mapped_to,
        name=input_.name,
        position=input_.position,
        prefix=input_.prefix,
    )


def _convert_command_output(output: CommandOutput, plan_id) -> CommandOutputTemplate:
    """Convert a CommandOutput to CommandOutputTemplate."""
    assert isinstance(output, CommandOutput)

    # TODO: add a '*' if this is a directory
    # TODO: For now this is always a fully qualified path; in future this might be glob pattern.
    produces = output.produces.path

    return CommandOutputTemplate(
        id=CommandOutputTemplate.generate_id(plan_id=plan_id, id_=Path(output._id).name),
        default_value=produces,
        description=output.description,
        mapped_to=output.mapped_to,
        name=output.name,
        position=output.position,
        prefix=output.prefix,
        create_folder=output.create_folder,
    )


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan, schema.Action, schema.CreativeWork]
        model = Plan
        unknown = EXCLUDE

    arguments = Nested(renku.hasArguments, CommandArgumentSchema, many=True, missing=None)
    command = fields.String(renku.command, missing=None)
    description = fields.String(schema.description, missing=None)
    id_ = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputTemplateSchema, many=True, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    name = fields.String(schema.name, missing=None)
    outputs = Nested(renku.hasOutputs, CommandOutputTemplateSchema, many=True, missing=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), missing=[0])
