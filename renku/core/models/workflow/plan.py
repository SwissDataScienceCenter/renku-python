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

from marshmallow import EXCLUDE
from werkzeug.utils import secure_filename

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku, schema
from renku.core.models.entities import Entity
from renku.core.models.workflow.parameters import (
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


class Plan:
    """Represent a `renku run` execution template."""

    def __init__(self, id_, arguments=None, command=None, inputs=None, name=None, outputs=None, success_codes=None):
        """Initialize."""
        self.arguments = arguments or []
        self.command = command
        self.id_ = id_
        self.inputs = inputs or []
        self.name = name or "{}-{}".format(secure_filename(self.command), uuid.uuid4().hex)
        self.outputs = outputs or []
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
    def from_run(cls, run: Run, name, client):
        """Create a Plan from a Run."""
        assert not run.subprocesses, f"Cannot create from a Run with subprocesses: {run._id}"

        uuid_ = _extract_run_uuid(run._id)
        plan_id = cls.generate_id(client=client, uuid_=uuid_)

        inputs = [_convert_command_input(i, plan_id) for i in run.inputs]
        outputs = [_convert_command_output(o, plan_id) for o in run.outputs]

        return cls(
            arguments=run.arguments,
            command=run.command,
            id_=plan_id,
            inputs=inputs,
            name=name,
            outputs=outputs,
            success_codes=run.successcodes,
        )

    @staticmethod
    def generate_id(client, uuid_):
        """Generate an identifier for the plan."""
        uuid_ = uuid_ or str(uuid.uuid4())
        host = get_host(client)
        return urllib.parse.urljoin(f"https://{host}", pathlib.posixpath.join("plans", uuid_))

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

    def is_similar_to(self, other):
        """Return true if plan has the same inputs/outputs/arguments as another plan."""

        def get_input_patterns(plan):
            return {e.consumes for e in plan.inputs}

        def get_output_patterns(plan):
            return {e.produces for e in plan.outputs}

        def get_arguments(plan):
            return {(a.position, a.prefix, a.value) for a in plan.arguments}

        # TODO: Check order of inputs/outputs/parameters as well after sorting by position
        return (
            self.command == other.command
            and set(self.success_codes) == set(other.success_codes)
            and get_input_patterns(self) == get_input_patterns(other)
            and get_output_patterns(self) == get_output_patterns(self)
            and get_arguments(self) == get_arguments(other)
        )

    def to_run(self, client, entities_cache):
        """Create a Run."""

        def convert_input(input_: CommandInputTemplate) -> CommandInput:
            entity = entities_cache.get(input_.consumes)
            if not entity:
                entity = Entity.from_revision(client=client, path=input_.consumes, revision="HEAD")
                entities_cache[input_.consumes] = entity

            return CommandInput(
                id=input_._id.replace(self.id_, run_id),
                consumes=entity,
                mapped_to=input_.mapped_to,
                position=input_.position,
                prefix=input_.prefix,
            )

        def convert_output(output: CommandOutputTemplate) -> CommandOutput:
            entity = entities_cache.get(output.produces)
            if not entity:
                entity = Entity.from_revision(client=client, path=output.produces, revision="HEAD")
                entities_cache[output.produces] = entity

            return CommandOutput(
                id=output._id.replace(self.id_, run_id),
                produces=entity,
                mapped_to=output.mapped_to,
                position=output.position,
                prefix=output.prefix,
                create_folder=output.create_folder,
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
        consumes=consumes,
        mapped_to=input_.mapped_to,
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
        produces=produces,
        mapped_to=output.mapped_to,
        position=output.position,
        prefix=output.prefix,
        create_folder=output.create_folder,
    )


class PlanSchema(JsonLDSchema):
    """Plan schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Plan]
        model = Plan
        unknown = EXCLUDE

    arguments = Nested(renku.hasArguments, CommandArgumentSchema, many=True, missing=None)
    command = fields.String(renku.command, missing=None)
    id_ = fields.Id()
    inputs = Nested(renku.hasInputs, CommandInputTemplateSchema, many=True, missing=None)
    name = fields.String(schema.name, missing=None)
    outputs = Nested(renku.hasOutputs, CommandOutputTemplateSchema, many=True, missing=None)
    success_codes = fields.List(renku.successCodes, fields.Integer(), missing=[0])
