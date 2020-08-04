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
"""Represent parameters from the Common Workflow Language."""

import attr


def convert_default(value):
    """Convert a default value."""
    return value


@attr.s
class _IdMixin(object):
    """Define id field."""

    id = attr.ib(default=None)


@attr.s
class Parameter(object):
    """Define an input or output parameter to a process."""

    streamable = attr.ib(default=None, converter=bool)


@attr.s
class InputParameter(_IdMixin, Parameter):
    """An input parameter."""

    type = attr.ib(default="string")
    description = attr.ib(default=None)
    default = attr.ib(default=None, converter=convert_default)
    inputBinding = attr.ib(default=None)


@attr.s
class CommandLineBinding(object):
    """Define the binding behavior when building the command line."""

    position = attr.ib(default=None)  # int
    prefix = attr.ib(default=None)  # int
    separate = attr.ib(default=True, type=bool)
    itemSeparator = attr.ib(default=None)  # str
    valueFrom = attr.ib(default=None)  # str | Expression
    shellQuote = attr.ib(default=True, type=bool)

    def to_argv(self, default=None):
        """Format command line binding as shell argument."""
        if self.valueFrom is not None:
            if self.valueFrom.startswith("$("):
                raise NotImplementedError()
            value = self.valueFrom
        else:
            value = default

        def _convert(value):
            """Convert value to a argument list."""
            if self.prefix:
                if self.separate:
                    return [self.prefix, str(value)]
                else:
                    return [self.prefix + str(value)]
            else:
                return [str(value)]

        if self.prefix is None and not self.separate:
            raise ValueError("Can not separate an empty prefix.")

        if isinstance(value, list):
            if self.itemSeparator and value:
                value = self.itemSeparator.join([str(v) for v in value])
            elif value:
                return [a for v in value for a in _convert(v)]
        elif (value is True or value is None) and self.prefix:
            return [self.prefix]
        elif value is False or value is None or (value is True and not self.prefix):
            return []

        return _convert(value)


@attr.s
class CommandInputParameter(InputParameter):
    """An input parameter for a CommandLineTool."""

    inputBinding = attr.ib(
        default=None,
        converter=lambda data: CommandLineBinding(**data)
        if not isinstance(data, CommandLineBinding) and data is not None
        else data,
    )

    @classmethod
    def from_cwl(cls, data):
        """Create instance from type definition."""
        if not isinstance(data, dict):
            data = {"type": data}
        return cls(**data)

    def to_argv(self, **kwargs):
        """Format command input parameter as shell argument."""
        return self.inputBinding.to_argv(default=self.default, **kwargs) if self.inputBinding else []


@attr.s
class OutputParameter(_IdMixin, Parameter):
    """An output parameter."""

    type = attr.ib(default="string")
    description = attr.ib(default=None)
    format = attr.ib(default=None)
    outputBinding = attr.ib(default=None)


@attr.s
class CommandOutputBinding(object):
    """Define the binding behavior for outputs."""

    glob = attr.ib(default=None)  # null, string, Expression, array[string]
    # loadContents, outputEval


@attr.s
class CommandOutputParameter(OutputParameter):
    """Define an output parameter for a CommandLineTool."""

    outputBinding = attr.ib(
        default=None,
        converter=lambda data: CommandOutputBinding(**data)
        if not isinstance(data, CommandOutputBinding) and data is not None
        else data,
    )


@attr.s
class WorkflowOutputParameter(OutputParameter):
    """Define an output parameter for a Workflow."""

    outputSource = attr.ib(default=None)
