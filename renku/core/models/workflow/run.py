# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
from bisect import bisect
from copy import copy
from functools import total_ordering
from pathlib import Path

import attr
from marshmallow import EXCLUDE

from renku.core.models.calamus import JsonLDSchema, Nested, fields, prov, renku
from renku.core.models.cwl.types import PATH_OBJECTS
from renku.core.models.entities import Collection, CommitMixin, CommitMixinSchema, Entity
from renku.core.models.workflow.parameters import (
    CommandArgument,
    CommandArgumentSchema,
    CommandInput,
    CommandInputSchema,
    CommandOutput,
    CommandOutputSchema,
    MappedIOStream,
    RunParameter,
    RunParameterSchema,
)


def _entity_from_path(client, path, commit):
    """Gets the entity associated with a path."""
    client, commit, path = client.resolve_in_submodules(
        client.find_previous_commit(path, revision=commit.hexsha), path,
    )

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, client.DATASETS)):
        return client.load_dataset_from_path(path, commit=commit)
    else:
        return entity_cls(commit=commit, client=client, path=str(path),)


def _convert_cmd_binding(binding, client, commit):
    """Convert a cwl argument to ``CommandArgument``."""

    base_id = Run.generate_id(client)

    id_ = CommandArgument.generate_id(base_id, binding.position)

    return CommandArgument(id=id_, position=binding.position, value=binding.valueFrom)


def _convert_cmd_input(input, client, commit, run_id):
    """Convert a cwl input to ``CommandInput``."""
    val = input.default

    if isinstance(val, list):
        val = input.inputBinding.itemSeparator.join(val)

    if input.type in PATH_OBJECTS and input.default:
        if input.inputBinding:
            prefix = input.inputBinding.prefix
            if prefix and input.inputBinding.separate:
                prefix += " "
            return CommandInput(
                id=CommandInput.generate_id(run_id, input.inputBinding.position),
                position=input.inputBinding.position,
                prefix=prefix,
                consumes=_entity_from_path(client, input.default.path, commit),
            )
        else:
            return CommandInput(
                id=CommandInput.generate_id(run_id, "stdin" if input.id == "input_stdin" else None),
                consumes=_entity_from_path(client, input.default.path, commit),
                mapped_to=MappedIOStream(client=client, stream_type="stdin") if input.id == "input_stdin" else None,
            )
    else:
        prefix = input.inputBinding.prefix
        if prefix and input.inputBinding.separate:
            prefix += " "
        return CommandArgument(
            id=CommandArgument.generate_id(run_id, input.inputBinding.position),
            position=input.inputBinding.position,
            value=val,
            prefix=prefix,
        )


def _convert_cmd_output(output, factory, client, commit, run_id):
    """Convert a cwl output to ``CommandOutput``."""
    path = None
    mapped = None
    input_prefix = "$(inputs."
    position = None
    prefix = None
    input_to_remove = None
    create_folder = False

    if output.outputBinding:
        if output.outputBinding.glob.startswith(input_prefix):
            input_id = output.outputBinding.glob[len(input_prefix) : -1]
            inp = next(i for i in factory.inputs if i.id == input_id)
            path = inp.default
            position = inp.inputBinding.position
            prefix = inp.inputBinding.prefix
            if prefix and inp.inputBinding.separate:
                prefix += " "
            input_to_remove = inp
        else:
            path = output.outputBinding.glob

    if output.type in MappedIOStream.STREAMS:
        path = getattr(factory, output.type)
        mapped = MappedIOStream(client=client, stream_type=output.type)

    if ((client.path / path).is_dir() and path in factory.existing_directories) or (
        not (client.path / path).is_dir() and str(Path(path).parent) in factory.existing_directories
    ):
        create_folder = True

    return (
        CommandOutput(
            id=CommandOutput.generate_id(run_id, position),
            produces=_entity_from_path(client, path, commit),
            mapped_to=mapped,
            position=position,
            prefix=prefix,
            create_folder=create_folder,
        ),
        input_to_remove,
    )


def _convert_run_parameter(parameter, run_id):
    """Convert a cwl run parameters to ``RunParameter``."""
    id_ = RunParameter.generate_id(run_id=run_id, name=parameter.name)
    return RunParameter(id=id_, name=parameter.name, value=parameter.value)


@total_ordering
@attr.s(cmp=False,)
class Run(CommitMixin):
    """Represents a `renku run` execution template."""

    command = attr.ib(default=None, type=str, kw_only=True,)

    successcodes = attr.ib(kw_only=True, type=list, factory=list)

    subprocesses = attr.ib(kw_only=True, factory=list)

    arguments = attr.ib(kw_only=True, factory=list)

    inputs = attr.ib(kw_only=True, factory=list)

    outputs = attr.ib(kw_only=True, factory=list)

    run_parameters = attr.ib(kw_only=True, factory=list)

    _activity = attr.ib(kw_only=True, default=None)

    @staticmethod
    def generate_id(client, identifier=None):
        """Generate an id for an argument."""
        host = "localhost"
        if client:
            host = client.remote.get("host") or host
        host = os.environ.get("RENKU_DOMAIN") or host

        if not identifier:
            identifier = str(uuid.uuid4())

        return urllib.parse.urljoin(
            "https://{host}".format(host=host),
            pathlib.posixpath.join("/runs", urllib.parse.quote(identifier, safe="")),
        )

    @classmethod
    def from_factory(cls, factory, client, commit, path):
        """Creates a ``Run`` from a ``CommandLineToolFactory``."""
        inputs = []
        arguments = []
        run_id = cls.generate_id(client)
        outputs = [_convert_cmd_output(o, factory, client, commit, run_id) for o in factory.outputs]

        if outputs:
            outputs, inputs_to_remove = zip(*outputs)
            outputs = list(outputs)

            for i in inputs_to_remove:
                # remove inputs that are actually outputs
                # note: a single input can represent multiple outputs
                # in case of repetition in the cli
                if not i:
                    continue
                if i in factory.inputs:
                    factory.inputs.remove(i)

        for i in factory.inputs:
            res = _convert_cmd_input(i, client, commit, run_id)

            if isinstance(res, CommandInput):
                inputs.append(res)
            else:
                arguments.append(res)

        return cls(
            id=run_id,
            client=client,
            commit=commit,
            path=path,
            command=" ".join(factory.baseCommand),
            successcodes=factory.successCodes,
            arguments=[_convert_cmd_binding(a, client, commit) for a in factory.arguments] + arguments,
            inputs=inputs,
            outputs=outputs,
            run_parameters=[_convert_run_parameter(a, run_id) for a in factory.run_parameters],
        )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity() if self._activity else None

    def to_argv(self):
        """Convert run into argv list."""
        argv = []

        if self.command:
            argv.extend(self.command.split(" "))

        arguments = self.inputs + self.outputs + self.arguments

        arguments = filter(lambda x: x.position, arguments)
        arguments = sorted(arguments, key=lambda x: x.position)
        argv.extend(e for a in arguments for e in a.to_argv())

        return argv

    def to_stream_repr(self):
        """Input/output stream representation."""
        stream_repr = []

        for input_ in self.inputs:
            if input_.mapped_to:
                stream_repr.append(input_.to_stream_repr())

        for output in self.outputs:
            if output.mapped_to:
                stream_repr.append(output.to_stream_repr())
        return stream_repr

    def update_id_and_label_from_commit_path(self, client, commit, path, is_subprocess=False):
        """Updates the _id and _label using supplied commit and path."""
        self.client = client
        if not self.commit:
            self.commit = commit
            if not is_subprocess:
                path = Path(os.path.abspath(path)).relative_to(self.client.path)
                self.path = path
                self._id = self.generate_id(client)
                self._label = self.default_label()

        if len(self.subprocesses) > 0:
            for s in self.subprocesses:
                s.process.update_id_and_label_from_commit_path(client, commit, path, is_subprocess=True)

    def add_subprocess(self, subprocess):
        """Adds a subprocess to this run."""
        process_order = 0
        if self.subprocesses:
            processes = [o.process for o in self.subprocesses]
            # Get position to insert based on dependencies
            process_order = bisect(processes, subprocess)
            if process_order < len(processes):
                # adjust ids of inputs inherited from latter subprocesses
                for i in range(len(processes), process_order, -1):
                    sp = self.subprocesses[i - 1]
                    sp._id = sp._id.replace(f"subprocess/{i}", f"subprocess/{i+1}")
                    sp.index += 1

                    for inp in self.inputs:
                        inp._id = inp._id.replace(f"/steps/step_{i}/", f"/steps/step_{i+1}/")
                    for outp in self.outputs:
                        outp._id = outp._id.replace(f"/steps/step_{i}/", f"/steps/step_{i+1}/")

        input_paths = [i.consumes.path for i in self.inputs]
        output_paths = [o.produces.path for o in self.outputs]

        for input_ in subprocess.inputs:
            if input_.consumes.path not in input_paths and input_.consumes.path not in output_paths:
                new_input = copy(input_)

                new_input._id = f"{self._id}/steps/step_{process_order + 1}/" f"{new_input.sanitized_id}"
                new_input.mapped_to = None

                matching_output = next((o for o in self.outputs if o.produces.path == new_input.consumes.path), None)

                if not matching_output:
                    self.inputs.append(new_input)
                    input_paths.append(new_input.consumes.path)

        for output in subprocess.outputs:
            if output.produces.path not in output_paths:
                new_output = copy(output)

                new_output._id = f"{self._id}/steps/step_{process_order + 1}/" f"{new_output.sanitized_id}"
                new_output.mapped_to = None
                self.outputs.append(new_output)
                output_paths.append(new_output.produces.path)

                matching_input = next((i for i in self.inputs if i.consumes.path == new_output.produces.path), None)
                if matching_input:
                    self.inputs.remove(matching_input)
                    input_paths.remove(matching_input.consumes.path)
        ordered_process = OrderedSubprocess(
            id=OrderedSubprocess.generate_id(self._id, process_order + 1), index=process_order + 1, process=subprocess
        )
        self.subprocesses.insert(process_order, ordered_process)

    def __lt__(self, other):
        """Compares two subprocesses order based on their dependencies."""
        a_inputs = set()
        b_outputs = set()

        for i in other.inputs:
            entity = i.consumes
            for subentity in entity.entities:
                a_inputs.add(subentity.path)

        for i in self.outputs:
            entity = i.produces
            for subentity in entity.entities:
                b_outputs.add(subentity.path)

        return a_inputs & b_outputs

    def __attrs_post_init__(self):
        """Calculate properties."""
        if self.client and not self._id:
            self._id = Run.generate_id(self.client)
        super().__attrs_post_init__()

        commit_not_set = not self.commit or self.commit.hexsha in self._label
        if commit_not_set and self.client and self.path and Path(self.path).exists():
            self.commit = self.client.find_previous_commit(self.path)

        # List order is not guaranteed when loading from JSON-LD
        self.subprocesses.sort()

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return RunSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return RunSchema().dump(self)


@total_ordering
@attr.s(cmp=False,)
class OrderedSubprocess:
    """A subprocess with ordering."""

    _id = attr.ib(kw_only=True)

    index = attr.ib(kw_only=True, type=int)

    process = attr.ib(kw_only=True)

    @staticmethod
    def generate_id(parent_id, index):
        """Generate an id for an ``OrderedSubprocess``."""
        return f"{parent_id}/subprocess/{index}"

    def __lt__(self, other):
        """Compares two ordered subprocesses."""
        return self.index < other.index


class RunSchema(CommitMixinSchema):
    """Run schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.Run, prov.Plan, prov.Entity]
        model = Run
        unknown = EXCLUDE

    command = fields.String(renku.command, missing=None)
    successcodes = fields.List(renku.successCodes, fields.Integer(), missing=[0])
    subprocesses = Nested(renku.hasSubprocess, nested="OrderedSubprocessSchema", missing=None, many=True)
    arguments = Nested(renku.hasArguments, CommandArgumentSchema, many=True, missing=None)
    inputs = Nested(renku.hasInputs, CommandInputSchema, many=True, missing=None)
    outputs = Nested(renku.hasOutputs, CommandOutputSchema, many=True, missing=None)
    run_parameters = Nested(renku.hasRunParameters, RunParameterSchema, many=True, missing=None)


class OrderedSubprocessSchema(JsonLDSchema):
    """OrderedSubprocess schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.OrderedSubprocess]
        model = OrderedSubprocess
        unknown = EXCLUDE

    _id = fields.Id(init_name="id")
    index = fields.Integer(renku.index)
    process = Nested(renku.process, RunSchema)
