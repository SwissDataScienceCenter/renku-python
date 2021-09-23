# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""OS utility functions."""

import os
from pathlib import Path
from typing import List, Union

from renku.core import errors


def get_relative_path_to_cwd(path: Union[Path, str]) -> str:
    """Get a relative path to current working directory."""
    absolute_path = os.path.abspath(path)
    return os.path.relpath(absolute_path, os.getcwd())


def get_relative_paths(base: Union[Path, str], paths: List[Union[Path, str]]) -> List[str]:
    """Return a list of paths relative to a base path."""
    relative_paths = []

    for path in paths:
        try:
            # NOTE: Do not use os.path.realpath or Path.resolve() because they resolve symlinks
            absolute_path = os.path.abspath(os.path.join(base, path))
            relative_path = Path(absolute_path).relative_to(base)
        except ValueError:
            raise errors.ParameterError(f"Path '{path}' is not within base path '{base}'")

        relative_paths.append(str(relative_path))

    return relative_paths


def are_paths_related(a, b):
    """Return True if paths are equal or one is the parent of the other."""
    common_path = os.path.commonpath((a, b))
    absolute_common_path = os.path.abspath(common_path)
    return absolute_common_path == os.path.abspath(a) or absolute_common_path == os.path.abspath(b)
