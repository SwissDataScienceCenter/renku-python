# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

import os
from pathlib import Path
from typing import Any, List, Optional


class _PathFormatterMixin:
    """Format path property."""

    path: Path

    def __str__(self):
        """Simple conversion to string."""
        return os.path.relpath(os.path.abspath(str(self.path)), os.path.realpath(os.getcwd()))


class File(_PathFormatterMixin):
    """Represent a file."""

    def __init__(self, path: Path, mime_type: Optional[List[str]] = None) -> None:
        self.path = path
        self.mime_type = mime_type or ["application/octet-stream"]


class Directory(_PathFormatterMixin):
    """Represent a directory."""

    def __init__(self, path: Path, listing: Optional[List[Any]] = None) -> None:
        self.path = path  # TODO add validation to allow only directories
        self.listing = listing or []


PATH_OBJECTS = {"File", "Directory"}
PATH_TYPES = (File, Directory)
