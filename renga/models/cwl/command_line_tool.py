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
    def from_args(cls, args, directory=None):
        """Return an instance guessed from arguments."""
        baseCommand, detect = split_command_and_args(args)
        arguments = []
        inputs = []
        for input_ in guess_inputs(directory, *detect):
            if isinstance(input_, CommandLineBinding):
                arguments.append(input_)
            else:
                inputs.append(input_)

        return cls(
            baseCommand=baseCommand,
            arguments=arguments,
            inputs=inputs,
        )

_RE_SUBCOMMAND = re.compile(r'^[A-Za-z]+(-[A-Za-z]+)?$')


def split_command_and_args(arguments):
    """Return tuple with command and args."""
    cmd = [arguments[0]]
    args = list(arguments[1:])
    # TODO check that it's not an existing file just in case
    while args and re.match(_RE_SUBCOMMAND, args[0]):
        cmd.append(args.pop(0))
    return cmd, args


def guess_type(value, directory=None):
    """Return new value and CWL parameter type."""
    try:
        value = int(value)
        return value, 'integer'
    except ValueError:
        pass

    if directory:
        directory = Path(directory)

        if directory.exists():
            candidate = Path(value)

            if not candidate.is_absolute():
                candidate = directory / candidate

            if candidate.exists():
                try:
                    # TODO return flag too if there `was_flag`
                    return str(candidate.relative_to(directory)), 'File'
                except ValueError:
                    # The candidate points to a file outside the working
                    # directory
                    # TODO suggest that the file should be imported to the repo
                    pass

    return value, 'string'


def guess_inputs(directory, *arguments):
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
                default, type = guess_type(default, directory=directory)

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
                default, type = guess_type(argument[2:], directory=directory)

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
            default, type = guess_type(argument, directory=directory)

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
