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

from renku.core.models.calamus import Nested, fields, prov, renku
from renku.core.models.cwl.types import PATH_OBJECTS
from renku.core.models.entities import Collection, CommitMixin, \
    CommitMixinSchema, Entity
from renku.core.models.workflow.parameters import CommandArgument, \
    CommandArgumentSchema, CommandInput, CommandInputSchema, CommandOutput, \
    CommandOutputSchema, MappedIOStream


def _entity_from_path(client, path, commit):
    """Gets the entity associated with a path."""
    client, commit, path = client.resolve_in_submodules(
        client.find_previous_commit(path, revision=commit.hexsha),
        path,
    )

    entity_cls = Entity
    if (client.path / path).is_dir():
        entity_cls = Collection

    if str(path).startswith(os.path.join(client.renku_home, client.DATASETS)):
        return client.load_dataset_from_path(path, commit=commit)
    else:
        return entity_cls(
            commit=commit,
            client=client,
            path=str(path),
        )


def _convert_cmd_binding(binding, client, commit):
    """Convert a cwl argument to ``CommandArgument``."""

    host = 'localhost'
    if client:
        host = client.remote.get('host') or host
    host = os.environ.get('RENKU_DOMAIN') or host

    base_id = urllib.parse.urljoin(
        'https://{host}'.format(host=host),
        pathlib.posixpath.join(
            '/runs/commit', urllib.parse.quote(commit.hexsha, safe='')
        )
    )

    id_ = '{}/{}'.format(
        base_id,
        pathlib.posixpath.join(
            'arguments', urllib.parse.quote(str(binding.position), safe='')
        )
    )

    return CommandArgument(
        id=id_, position=binding.position, value=binding.valueFrom
    )


def _convert_cmd_input(input, client, commit):
    """Convert a cwl input to ``CommandInput``."""
    val = input.default

    if isinstance(val, list):
        val = input.inputBinding.itemSeparator.join(val)

    host = 'localhost'
    if client:
        host = client.remote.get('host') or host
    host = os.environ.get('RENKU_DOMAIN') or host

    base_id = '{}/{}'.format(
        'https://{host}'.format(host=host),
        pathlib.posixpath.join(
            '/runs/commit', urllib.parse.quote(commit.hexsha, safe='')
        )
    )

    if input.type in PATH_OBJECTS and input.default:
        if input.inputBinding:
            prefix = input.inputBinding.prefix
            if prefix and input.inputBinding.separate:
                prefix += ' '
            id_ = '{}/{}'.format(
                base_id,
                pathlib.posixpath.join(
                    'inputs',
                    urllib.parse.quote(
                        str(input.inputBinding.position), safe=''
                    )
                )
            )
            return CommandInput(
                id=id_,
                position=input.inputBinding.position,
                prefix=prefix,
                consumes=_entity_from_path(client, input.default.path, commit)
            )
        else:
            id_ = '{}/{}'.format(
                base_id, 'inputs/stdin' if input.id == 'input_stdin' else
                'inputs/{}'.format(uuid.uuid4().hex)
            )

            mapped_id = '{}/{}'.format(base_id, 'mappedstreams/stdin')

            return CommandInput(
                id=id_,
                consumes=_entity_from_path(client, input.default.path, commit),
                mapped_to=MappedIOStream(id=mapped_id, stream_type='stdin')
                if input.id == 'input_stdin' else None
            )
    else:
        prefix = input.inputBinding.prefix
        if prefix and input.inputBinding.separate:
            prefix += ' '
        id_ = '{}/{}'.format(
            base_id,
            pathlib.posixpath.join(
                'arguments',
                urllib.parse.quote(str(input.inputBinding.position), safe='')
            )
        )
        return CommandArgument(
            id=id_,
            position=input.inputBinding.position,
            value=val,
            prefix=prefix
        )


def _convert_cmd_output(output, factory, client, commit):
    """Convert a cwl output to ``CommandOutput``."""
    path = None
    mapped = None
    input_prefix = '$(inputs.'
    position = None
    prefix = None
    input_to_remove = None
    create_folder = False

    host = 'localhost'
    if client:
        host = client.remote.get('host') or host
    host = os.environ.get('RENKU_DOMAIN') or host

    base_id = urllib.parse.urljoin(
        'https://{host}'.format(host=host),
        pathlib.posixpath.join(
            '/runs/commit', urllib.parse.quote(commit.hexsha, safe='')
        )
    )

    id_ = uuid.uuid4().hex

    if output.outputBinding:
        if output.outputBinding.glob.startswith(input_prefix):
            input_id = output.outputBinding.glob[len(input_prefix):-1]
            inp = next(i for i in factory.inputs if i.id == input_id)
            path = inp.default
            position = inp.inputBinding.position
            id_ = str(position)
            prefix = inp.inputBinding.prefix
            if prefix and inp.inputBinding.separate:
                prefix += ' '
            input_to_remove = inp
        else:
            path = output.outputBinding.glob

    if output.type in MappedIOStream.STREAMS:
        path = getattr(factory, output.type)
        id_ = output.type
        mapped_id = '{}/{}'.format(
            base_id,
            pathlib.posixpath.join(
                'mappedstreams', urllib.parse.quote(id_, safe='')
            )
        )
        mapped = MappedIOStream(id=mapped_id, stream_type=output.type)

    if (((client.path / path).is_dir() and
         path in factory.existing_directories) or (
             not (client.path / path).is_dir() and
             str(Path(path).parent) in factory.existing_directories
         )):
        create_folder = True

    id_ = '{}/{}'.format(
        base_id,
        pathlib.posixpath.join('outputs', urllib.parse.quote(id_, safe=''))
    )

    return CommandOutput(
        id=id_,
        produces=_entity_from_path(client, path, commit),
        mapped_to=mapped,
        position=position,
        prefix=prefix,
        create_folder=create_folder
    ), input_to_remove


@total_ordering
@attr.s(
    cmp=False,
)
class Run(CommitMixin):
    """Represents a `renku run` execution template."""

    command = attr.ib(
        default=None,
        type=str,
        kw_only=True,
    )

    process_order = attr.ib(
        default=None,
        type=int,
        kw_only=True,
    )

    successcodes = attr.ib(kw_only=True, type=list, factory=list)

    subprocesses = attr.ib(kw_only=True, factory=list)

    arguments = attr.ib(kw_only=True, factory=list)

    inputs = attr.ib(kw_only=True, factory=list)

    outputs = attr.ib(kw_only=True, factory=list)

    @classmethod
    def from_factory(cls, factory, client, commit, path):
        """Creates a ``Run`` from a ``CommandLineToolFactory``."""
        inputs = []
        arguments = []
        outputs = [
            _convert_cmd_output(o, factory, client, commit)
            for o in factory.outputs
        ]  # TODO: handle stream!

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
            res = _convert_cmd_input(i, client, commit)

            if isinstance(res, CommandInput):
                inputs.append(res)
            else:
                arguments.append(res)

        return cls(
            client=client,
            commit=commit,
            path=path,
            command=' '.join(factory.baseCommand),
            successcodes=factory.successCodes,
            arguments=[
                _convert_cmd_binding(a, client, commit)
                for a in factory.arguments
            ] + arguments,
            inputs=inputs,
            outputs=outputs
        )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity()

    def default_id(self):
        """Define default value for id field."""
        host = 'localhost'
        if self.client:
            host = self.client.remote.get('host') or host
        host = os.environ.get('RENKU_DOMAIN') or host

        if self.commit:
            id_ = self.commit.hexsha
        else:
            id_ = str(uuid.uuid4())

        return urllib.parse.urljoin(
            'https://{host}'.format(host=host),
            pathlib.posixpath.join(
                '/runs/commit', urllib.parse.quote(id_, safe='')
            )
        )

    def to_argv(self):
        """Convert run into argv list."""
        argv = []

        if self.command:
            argv.extend(self.command.split(' '))

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

    def update_id_and_label_from_commit_path(self, client, commit, path):
        """Updates the _id and _label using supplied commit and path."""
        self.client = client

        if not self.commit:
            self.commit = commit

            path = Path(os.path.abspath(path)).relative_to(self.client.path)
            self.path = path
            self._id = self.default_id()
            self._label = self.default_label()

        if len(self.subprocesses) > 0:
            for s in self.subprocesses:
                s.update_id_and_label_from_commit_path(client, commit, path)

    def set_process_order(self, process_order):
        """Updates the process_order on a subprocess and its parameters."""
        self.process_order = process_order

        parts = self._id.split('/')
        if '/step/' in self._id:
            parts[-1] = str(process_order)
        else:
            parts.extend(['step', str(process_order)])
        self._id = '/'.join(parts)

        for input_ in self.inputs:
            parts = input_._id.split('/')
            if '/step/' in input_._id:
                parts[-3] = str(process_order)
            else:
                parts.insert(-2, 'step')
                parts.insert(-2, str(process_order))
            input_._id = '/'.join(parts)

        for output in self.outputs:
            parts = output._id.split('/')
            if '/step/' in output._id:
                parts[-3] = str(process_order)
            else:
                parts.insert(-2, 'step')
                parts.insert(-2, str(process_order))
            output._id = '/'.join(parts)

        for argument in self.arguments:
            # adjust id to be a subprocess id
            parts = argument._id.split('/')
            if '/step/' in argument._id:
                parts[-3] = str(process_order)
            else:
                parts.insert(-2, 'step')
                parts.insert(-2, str(process_order))
            argument._id = '/'.join(parts)

    def add_subprocess(self, subprocess, process_order=None):
        """Adds a subprocess to this run."""
        if not process_order:
            process_order = 0
            if self.subprocesses:
                # sort subprocesses by dependencies
                process_order = bisect(self.subprocesses, subprocess)
                if process_order < len(self.subprocesses):
                    # inserted before end, recalculate orders or rest
                    for s in self.subprocesses:
                        if s.process_order >= process_order:
                            s.set_process_order(s.process_order + 1)

        if any(s.process_order == process_order for s in self.subprocesses):
            raise ValueError(
                'process_order {} already exists'.format(process_order)
            )

        input_paths = [i.consumes.path for i in self.inputs]
        output_paths = [o.produces.path for o in self.outputs]

        for input_ in subprocess.inputs:
            if (
                input_.consumes.path not in input_paths and
                input_.consumes.path not in output_paths
            ):
                new_input = copy(input_)
                new_input.mapped_to = None

                matching_output = next((
                    o for o in self.outputs
                    if o.produces.path == new_input.consumes.path
                ), None)

                if not matching_output:
                    self.inputs.append(new_input)
                    input_paths.append(new_input.consumes.path)

        for output in subprocess.outputs:
            if output.produces.path not in output_paths:
                new_output = copy(output)
                new_output.mapped_to = None
                self.outputs.append(new_output)
                output_paths.append(new_output.produces.path)

                matching_input = next((
                    i for i in self.inputs
                    if i.consumes.path == new_output.produces.path
                ), None)
                if matching_input:
                    self.inputs.remove(matching_input)
                    input_paths.remove(matching_input.consumes.path)

        subprocess.set_process_order(process_order)

        self.subprocesses.append(subprocess)

        self.subprocesses = sorted(
            self.subprocesses, key=lambda s: s.process_order
        )

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
        super().__attrs_post_init__()

        if ((not self.commit or self.commit.hexsha in self._id) and
            self.client and Path(self.path).exists()):
            self.commit = self.client.find_previous_commit(self.path)

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


class RunSchema(CommitMixinSchema):
    """Run schema."""

    class Meta:
        """Meta class."""

        rdf_type = [renku.Run, prov.Plan, prov.Entity]
        model = Run
        unknown = EXCLUDE

    command = fields.String(renku.command, missing=None)
    process_order = fields.Integer(renku.processOrder, missing=None)
    successcodes = fields.List(
        renku.successCodes, fields.Integer(), missing=[0]
    )
    subprocesses = Nested(
        renku.hasSubprocess, 'RunSchema', many=True, missing=None
    )
    arguments = Nested(
        renku.hasArguments, CommandArgumentSchema, many=True, missing=None
    )
    inputs = Nested(
        renku.hasInputs, CommandInputSchema, many=True, missing=None
    )
    outputs = Nested(
        renku.hasOutputs, CommandOutputSchema, many=True, missing=None
    )
