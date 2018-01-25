# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Represent a ``CommandLineTool`` from the Common Workflow Language."""

import re
import shlex

import attr

from renga._compat import Path

from .parameter import CommandInputParameter, CommandLineBinding
from .process import Process


@attr.s
class CommandLineTool(Process):
    """Represent a command line tool."""

    # specialize inputs and outputs with Command{Input,Output}Parameter

    baseCommand = attr.ib(
        default='',
        validator=lambda self, attr, cmd: bool(cmd),
    )  # str or list(str) -> shutil.split()
    arguments = attr.ib(
        default=attr.Factory(list),
        converter=lambda cmd: list(cmd) if isinstance(
            cmd, (list, tuple)) else shlex.split(cmd),
    )  # list(string, Expression, CommandLineBinding)

    stdin = attr.ib(default=None)  # null, str, Expression
    stderr = attr.ib(default=None)  # null, str, Expression
    stdout = attr.ib(default=None)  # null, str, Expression

    successCodes = attr.ib(default=attr.Factory(list))  # list(int)
    temporaryFailCodes = attr.ib(default=attr.Factory(list))  # list(int)
    permanentFailCodes = attr.ib(default=attr.Factory(list))  # list(int)

    def run(self, job=None):  # job: Job
        """Execute the command line tool with an optional job description."""
        raise NotImplemented()

    @classmethod
    def from_args(cls, command_line, directory=None):
        """Return an instance guessed from arguments."""
        factory = CommandLineToolFactory(
            command_line=command_line,
            directory=directory or '.',
        )
        return cls(
            baseCommand=factory.baseCommand,
            arguments=factory.arguments,
            inputs=factory.inputs,
            outputs=factory.outputs,
        )


@attr.s
class CommandLineToolFactory(object):
    """Command Line Tool Factory."""

    _RE_SUBCOMMAND = re.compile(r'^[A-Za-z]+(-[A-Za-z]+)?$')

    command_line = attr.ib(
        converter=lambda cmd: list(cmd) if isinstance(
            cmd, (list, tuple)) else shlex.split(cmd),
    )

    directory = attr.ib(
        default='.',
        converter=Path,
    )

    baseCommand = attr.ib(init=False)
    arguments = attr.ib(init=False)
    inputs = attr.ib(init=False)
    outputs = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.baseCommand, detect = self.split_command_and_args()
        self.arguments = []
        self.inputs = []
        self.outputs = []

        for input_ in self.guess_inputs(*detect):
            if isinstance(input_, CommandLineBinding):
                self.arguments.append(input_)
            else:
                self.inputs.append(input_)

    @command_line.validator
    def validate_command_line(self, attribute, value):
        """Check the command line structure."""
        if not value:
            raise ValueError('Command line can not be empty.')

    @directory.validator
    def validate_path(self, attribute, value):
        """Path must exists."""
        if not value.exists():
            raise ValueError('Directory must exist.')

    def file_candidate(self, candidate):
        """Return a path instance if it exists in current directory."""
        candidate = Path(candidate)

        if not candidate.is_absolute():
            candidate = self.directory / candidate

        if candidate.exists():
            return candidate

    def split_command_and_args(self):
        """Return tuple with command and args from command line arguments."""
        cmd = [self.command_line[0]]
        args = list(self.command_line[1:])

        while args and re.match(self._RE_SUBCOMMAND, args[0]) \
                and not self.file_candidate(args[0]):
            cmd.append(args.pop(0))

        return cmd, args

    def guess_type(self, value):
        """Return new value and CWL parameter type."""
        try:
            value = int(value)
            return value, 'integer'
        except ValueError:
            pass

        candidate = self.file_candidate(value)
        if candidate:
            try:
                return str(candidate.relative_to(self.directory)), 'File'
            except ValueError:
                # The candidate points to a file outside the working
                # directory
                # TODO suggest that the file should be imported to the repo
                pass

        return value, 'string'

    def guess_inputs(self, *arguments):
        """Yield tuples with ``Path`` instance and argument position."""
        position = 0
        prefix = None

        for index, argument in enumerate(arguments):

            if prefix:
                if argument.startswith('-'):
                    position += 1
                    yield CommandLineBinding(
                        position=position,
                        prefix=prefix,
                    )
                    prefix = None

            if argument.startswith('--'):
                if '=' in argument:
                    prefix, default = argument.split('=', 1)
                    default, type = self.guess_type(default)
                    # TODO can be output

                    position += 1
                    yield CommandInputParameter(
                        id='input_{0}'.format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position,
                            prefix=prefix + '=',
                            separate=False,
                        )
                    )
                    prefix = None
                else:
                    prefix = argument

            elif argument.startswith('-'):
                if len(argument) > 2:
                    # possibly a flag with value
                    prefix = argument[0:2]
                    default, type = self.guess_type(argument[2:])
                    # TODO can be output

                    position += 1
                    yield CommandInputParameter(
                        id='input_{0}'.format(position),
                        type=type,
                        default=default,
                        inputBinding=dict(
                            position=position,
                            prefix=prefix,
                            separate=not bool(argument[2:]),
                        )
                    )
                    prefix = None
                else:
                    prefix = argument

            else:
                default, type = self.guess_type(argument)
                # TODO can be output

                # TODO there might be an array
                position += 1
                yield CommandInputParameter(
                    id='input_{0}'.format(position),
                    type=type,
                    default=default,
                    inputBinding=dict(
                        position=position,
                        prefix=prefix,
                    )
                )
                prefix = None

        if prefix:
            position += 1
            yield CommandLineBinding(
                position=position,
                prefix=prefix,
            )
