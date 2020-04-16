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
"""Compatibility layer for different Python versions."""

import cgi
import contextlib
import html
from pathlib import Path

import pathspec

try:
    contextlib.nullcontext
except AttributeError:

    class nullcontext(object):
        """Context manager that does no additional processing.

        Used as a stand-in for a normal context manager, when a particular
        block of code is only sometimes used with a normal context manager:
        cm = optional_cm if condition else nullcontext()
        with cm:
            # Perform operation, using optional_cm if condition is True
        """

        def __init__(self, enter_result=None):
            self.enter_result = enter_result

        def __enter__(self):
            return self.enter_result

        def __exit__(self, *excinfo):
            pass

    contextlib.nullcontext = nullcontext

try:
    FileNotFoundError
except NameError:  # pragma: no cover
    FileNotFoundError = IOError

cgi.escape = html.escape


class RenkuGitWildMatchPattern(pathspec.patterns.GitWildMatchPattern):
    """Custom GitWildMatchPattern matcher."""

    __slots__ = ('pattern', )

    def __init__(self, pattern, include=None):
        """Initialize RenkuRegexPattern."""
        super().__init__(pattern, include)
        self.pattern = pattern


pathspec.util.register_pattern('renku_gitwildmatch', RenkuGitWildMatchPattern)

__all__ = (
    'FileNotFoundError',
    'Path',
    'contextlib',
    'cgi',
)
