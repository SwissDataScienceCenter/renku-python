# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Project properties/configuration."""

import threading
from contextlib import contextmanager
from pathlib import Path

from renku.core.util.git import default_path


class ProjectProperties(threading.local):
    """A configuration class to hold global configuration."""

    external_storage_requested = True
    """External storage (e.g. LFS) requested for Renku command."""

    def __init__(self) -> None:
        path = default_path()

        path = Path(path).resolve()

        self._path_stack = [path]

    @property
    def path(self):
        """Current project path."""
        return self._path_stack[-1]

    def pop_path(self) -> Path:
        """Pop current project path from stack.

        Returns:
            Path: the popped project path.
        """
        if len(self._path_stack) > 1:
            return self._path_stack.pop()
        else:
            raise IndexError("Can't pop last remaining path from stack.")

    def push_path(self, path: Path) -> None:
        """Push a new project path to the stack.

        Arguments:
            path(Path): The path to push.
        """
        self._path_stack.append(path.resolve())

    def replace_path(self, path: Path):
        """Replace the current project path with a new one.

        Arguments:
            path(Path): The path to replace with.
        """
        self._path_stack[-1] = path.resolve()

    @contextmanager
    def with_path(self, path: Path):
        """Temporarily push a new project path to the stack.

        Arguments:
            path(Path): The path to push.
        """
        self.push_path(path)

        try:
            yield self.path
        finally:
            self.pop_path()


project_properties = ProjectProperties()
