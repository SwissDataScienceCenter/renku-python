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
from copy import copy

from renku.core.models import jsonld as jsonld
from renku.core.models.cwl.types import PATH_OBJECTS
from renku.core.models.entities import Collection, CommitMixin, Entity
from renku.core.models.workflow.parameters import CommandArgument, \
    CommandInput, CommandOutput, MappedIOStream


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


def _convert_cmd_binding(binding):
    """Convert a cwl argument to ``CommandArgument``."""
    return CommandArgument(position=binding.position, value=binding.valueFrom)


def _convert_cmd_input(input, client, commit):
    """Convert a cwl input to ``CommandInput``."""
    val = input.default

    if isinstance(val, list):
        val = input.inputBinding.itemSeparator.join(val)

    if input.type in PATH_OBJECTS and input.default:
        if input.inputBinding:
            prefix = input.inputBinding.prefix
            if prefix and input.inputBinding.separate:
                prefix += ' '
            return CommandInput(
                position=input.inputBinding.position,
                prefix=prefix,
                consumes=_entity_from_path(client, input.default.path, commit)
            )
        else:
            return CommandInput(
                consumes=_entity_from_path(client, input.default.path, commit),
                mapped_to=MappedIOStream(stream_type='stdin')
                if input.id == 'input_stdin' else None
            )
    else:
        prefix = input.inputBinding.prefix
        if prefix and input.inputBinding.separate:
            prefix += ' '
        return CommandArgument(
            position=input.inputBinding.position, value=val, prefix=prefix
        )


def _convert_cmd_output(output, factory, client, commit):
    """Convert a cwl output to ``CommandOutput``."""
    path = None
    mapped = None
    input_prefix = '$(inputs.'
    position = None
    prefix = None
    input_to_remove = None
    if output.outputBinding:
        if output.outputBinding.glob.startswith(input_prefix):
            input_id = output.outputBinding.glob[len(input_prefix):-1]
            inp = next(i for i in factory.inputs if i.id == input_id)
            path = inp.default
            position = inp.inputBinding.position
            prefix = inp.inputBinding.prefix
            if prefix and inp.inputBinding.separate:
                prefix += ' '
            input_to_remove = inp
        else:
            path = output.outputBinding.glob

    if output.type in MappedIOStream.STREAMS:
        path = getattr(factory, output.type)
        mapped = MappedIOStream(stream_type=output.type)

    return CommandOutput(
        produces=_entity_from_path(client, path, commit),
        mapped_to=mapped,
        position=position,
        prefix=prefix
    ), input_to_remove


@jsonld.s(
    type=[
        'renku:Run',
        'prov:Entity',
        'prov:Plan',
    ],
    context={
        'renku': 'https://swissdatasciencecenter.github.io/renku-ontology#',
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Run(CommitMixin):
    """Represents a `renku run` execution template."""

    command = jsonld.ib(
        default=None,
        context={
            '@id': 'renku:command',
            '@type': 'http://www.w3.org/2001/XMLSchema#string',
        },
        type=str,
        kw_only=True,
    )

    process_order = jsonld.ib(
        default=None,
        context={
            '@id': 'renku:processOrder',
            '@type': 'http://www.w3.org/2001/XMLSchema#integer',
        },
        type=int,
        kw_only=True,
    )

    successcodes = jsonld.ib(
        default=None,
        context={
            '@id': 'renku:successCodes',
            '@type': 'http://www.w3.org/2001/XMLSchema#integer',
        },
        type=int,
        kw_only=True,
    )

    subprocesses = jsonld.container.list(
        context='renku:hasSubprocess',
        kw_only=True,
        type='renku.core.models.workflow.run.Run'
    )

    arguments = jsonld.container.list(
        context='renku:hasArguments', kw_only=True, type=CommandArgument
    )

    inputs = jsonld.container.list(
        context='renku:hasInputs', kw_only=True, type=CommandInput
    )

    outputs = jsonld.container.list(
        context='renku:hasOutputs', kw_only=True, type=CommandOutput
    )

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
            arguments=[_convert_cmd_binding(a)
                       for a in factory.arguments] + arguments,
            inputs=inputs,
            outputs=outputs
        )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity()

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

    def add_subprocess(self, subprocess, process_order=None):
        """Adds a subprocess to this run."""
        if not process_order:
            process_order = 0
            if self.subprocesses:
                process_order = max(
                    s.process_order for s in self.subprocesses
                ) + 1

        if any(s.process_order == process_order for s in self.subprocesses):
            raise ValueError(
                'process_order {} already exists'.format(process_order)
            )

        subprocess.process_order = process_order

        input_paths = [i.consumes.path for i in self.inputs]
        output_paths = [o.produces.path for o in self.outputs]

        for input_ in subprocess.inputs:
            if (
                input_.consumes.path not in input_paths and
                input_.consumes.path not in output_paths
            ):
                new_input = copy(input_)
                new_input.mapped_to = None
                self.inputs.append(new_input)
                input_paths.append(new_input.consumes.path)

        for output in subprocess.outputs:
            if output.produces.path not in output_paths:
                new_output = copy(output)
                new_output.mapped_to = None
                self.outputs.append(new_output)
                output_paths.append(new_output.produces.path)

        self.subprocesses.append(subprocess)
