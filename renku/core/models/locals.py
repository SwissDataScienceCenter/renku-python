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
"""Manage local contexts."""

from contextlib import contextmanager

import attr
from werkzeug.local import LocalStack

_current_reference = LocalStack()

current_reference = _current_reference()


@contextmanager
def with_reference(path):
    """Manage reference stack."""
    _current_reference.push(path)
    yield
    if path != _current_reference.pop():
        raise RuntimeError('current_reference has been modified')


def has_reference():
    """Check if the current reference is bounded."""
    return _current_reference.top is not None


@attr.s(cmp=False)
class ReferenceMixin:
    """Define an automatic ``__reference__`` attribute."""

    __source__ = attr.ib(init=False, kw_only=True, repr=False, default=None)
    __reference__ = attr.ib(init=False, kw_only=True, repr=False)

    @__reference__.default
    def default_reference(self):
        """Create a default reference path."""
        return current_reference._get_current_object() if has_reference(
        ) else None
