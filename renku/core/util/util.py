# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""General utility functions."""

from typing import Any, NewType, Optional

from packaging.version import Version


def to_string(value: Optional[Any], strip: bool = False) -> str:
    """Return a string representation of value and return an empty string for None."""
    return "" if value is None else str(value).strip() if strip else str(value)


def to_semantic_version(value: str) -> Optional[Version]:
    """Convert value to SemVer."""
    try:
        return Version(value)
    except ValueError:
        return None


NoValueType = NewType("NoValueType", object)
"""Type to represent a value not being set in cases where ``None`` is a valid value."""

NO_VALUE = NoValueType(object())
"""Sentinel to represent a value not being set in cases where ``None`` is a valid value."""
