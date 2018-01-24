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
"""Represent parameters from the Common Workflow Language."""

import attr


@attr.s
class CommandLineBinding(object):
    """Define the binding behavior when building the command line."""

    position = attr.ib(default=None)  # int
    prefix = attr.ib(default=None)  # int
    separate = attr.ib(default=True, type=bool)
    itemSeparator = attr.ib(default=None)  # str
    valueFrom = attr.ib(default=None)  # str | Expression
    shellQuote = attr.ib(default=True, type=bool)


@attr.s
class CommandInputParameter(object):
    """An input parameter for a CommandLineTool."""

    id = attr.ib()
    type = attr.ib(default='string')
    description = attr.ib(default=None)
    default = attr.ib(default=None)
    inputBinding = attr.ib(
        default=None,
        converter=lambda data: CommandLineBinding(**data)
        if not isinstance(data, CommandLineBinding) and data is not None
        else data,
    )
    streamable = attr.ib(default=None)
