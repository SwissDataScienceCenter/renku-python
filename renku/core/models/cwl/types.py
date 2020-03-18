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
"""Represent the Common Workflow Language types."""

import json
import os
from pathlib import Path

import attr

from .ascwl import CWLClass, ascwl


class PathFormatterMixin:
    """Format path property."""

    def __str__(self):
        """Simple conversion to string."""
        reference = self.__reference__
        if reference:
            return str(os.path.normpath(str(reference.parent / self.path)))
        return os.path.relpath(
            os.path.abspath(str(self.path)), os.path.realpath(os.getcwd())
        )


@attr.s
class File(CWLClass, PathFormatterMixin):
    """Represent a file."""

    path = attr.ib(converter=Path)


@attr.s
class Directory(CWLClass, PathFormatterMixin):
    """Represent a directory."""

    # TODO add validation to allow only directories
    path = attr.ib(default=None)
    listing = attr.ib(default=attr.Factory(list))


DIRECTORY_EXPRESSION = '$({0})'.format(
    json.dumps(ascwl(Directory(), filter=lambda _, x: x is not None))
)
PATH_OBJECTS = {'File', 'Directory'}
PATH_TYPES = (File, Directory)


@attr.s
class Dirent(object):
    """Define a file or subdirectory."""

    entryname = attr.ib(default=None)
    entry = attr.ib(default=None)
    writable = attr.ib(default=False)
