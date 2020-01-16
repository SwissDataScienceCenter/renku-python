# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Represent a ``Process`` from the Common Workflow Language."""

import os

import attr

from .ascwl import mapped
from .parameter import InputParameter, OutputParameter
from .types import PATH_OBJECTS


@attr.s(init=False)
class Process(object):
    """Represent a process."""

    inputs = mapped(InputParameter)
    outputs = mapped(OutputParameter)
    requirements = attr.ib(default=attr.Factory(list))
    # list ProcessRequirement
    hints = attr.ib(default=attr.Factory(list))  # list Any
    label = attr.ib(default=None)  # str
    doc = attr.ib(default=None)  # str
    cwlVersion = attr.ib(default='v1.0')  # derive from a parent

    def iter_input_files(self, basedir):
        """Yield tuples with input id and path."""
        stdin = getattr(self, 'stdin', None)
        if stdin and stdin[0] != '$':  # pragma: no cover
            raise NotImplementedError(self.stdin)
        for input_ in self.inputs:
            if input_.type in PATH_OBJECTS and input_.default:
                yield (
                    input_.id,
                    os.path.normpath(
                        os.path.join(basedir, str(input_.default.path))
                    )
                )
