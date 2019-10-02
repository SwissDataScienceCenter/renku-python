# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Compatibility layer for different Python versions."""

import os
import sys
from pathlib import Path

if sys.version_info < (3, 6):
    original_resolve = Path.resolve

    def resolve(self, strict=False):
        """Support strict parameter."""
        if strict:
            return original_resolve(self)
        return Path(os.path.realpath(os.path.abspath(str(self))))

    Path.resolve = resolve

try:
    FileNotFoundError
except NameError:  # pragma: no cover
    FileNotFoundError = IOError

__all__ = (
    'FileNotFoundError',
    'Path',
)
