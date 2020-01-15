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
"""Process requirements modify the semantics or runtime environment."""

from pathlib import Path

import attr

from ..datastructures import DirectoryTree
from .ascwl import CWLClass
from .types import DIRECTORY_EXPRESSION, PATH_OBJECTS, Dirent


class ProcessRequirement(object):
    """Declare a prerequisite that may or must be fulfilled."""


@attr.s
class InlineJavascriptRequirement(CWLClass):
    """Indicate that runner must support inline Javascript expressions."""


@attr.s
class InitialWorkDirRequirement(ProcessRequirement, CWLClass):
    """Define a list of files and subdirectories that must be created."""

    listing = attr.ib(default=attr.Factory(list))  # File, Directory

    @classmethod
    def from_tool(cls, tool, existing_directories=None, working_dir=''):
        """Create a directory structure based on tool inputs and outputs."""
        directories = DirectoryTree()
        inputs = {input_.id: input_ for input_ in tool.inputs}

        converters = {
            'File': lambda value: Path(value).parent,
            'Directory': lambda value: Path(value),
        }

        # TODO enable for extra tool inputs when there is no inputBinding
        # for input_ in tool.inputs:
        #     # NOTE use with CWL 1.1
        #     # if intput_.type == 'stdin':
        #     #     stream = getattr(tool, input_.type)
        #     #     directories[stream]
        #     if input_.type == 'File':
        #         directories.add(input_.default.path)
        #     # TODO add Directory

        for output in tool.outputs:
            # NOTE output streams should be handled automatically
            # if output.type in {'stdout', 'stderr'}:
            #     stream = getattr(tool, output.type)
            #     directories.add(stream)
            if output.type in PATH_OBJECTS:
                glob = output.outputBinding.glob
                convert = converters[output.type]
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    input_ = inputs.get(input_id)
                    if input_ is not None:
                        directories.add(convert(input_.default))
                        # TODO parametrize directory name directories.add(glob)
                elif glob:
                    directories.add(convert(glob))

        requirement = cls()

        for directory in directories:
            if existing_directories and directory not in existing_directories:
                # Create only existing directories.
                continue

            requirement.listing.append(
                Dirent(
                    entryname=directory,
                    entry=DIRECTORY_EXPRESSION,
                    writable=True,
                )
            )

        for input in tool.inputs:
            if input.type in PATH_OBJECTS:
                entryname = input.default.path.relative_to(working_dir)
                requirement.listing.append(
                    Dirent(
                        entry='$(inputs.{})'.format(input.id),
                        entryname=str(entryname),
                    )
                )

        if requirement.listing:
            return requirement
