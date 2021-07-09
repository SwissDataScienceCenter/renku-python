# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Test Immutable classes."""

import pytest

from renku.core.incubation.immutable import Immutable


class A(Immutable):
    """Test class."""

    __slots__ = ("a_member",)


class B(A):
    """Test class."""

    __slots__ = "b_member"

    def __init__(self, *, b_member=None, **kwargs):
        super().__init__(b_member=b_member, **kwargs)


def test_instantiate(client, runner):
    """Test instantiating Immutable subclasses."""
    b = B(a_member=42, b_member=43)

    assert {"a_member": 42, "b_member": 43} == b.__getstate__()


def test_instantiate_incomplete(client, runner):
    """Test instantiating Immutable subclasses without setting all members."""
    b = B(a_member=42)

    assert {"a_member": 42, "b_member": None} == b.__getstate__()


def test_instantiate_invalid_member(client, runner):
    """Test instantiating Immutable subclasses and passing a non-member."""
    with pytest.raises(AttributeError) as e:
        B(c_member=42)

    assert "object has no attribute 'c_member'" in str(e)


def test_cannot_mutate(client, runner):
    """Test cannot mutate an Immutable subclasses."""
    b = B(a_member=42, b_member=43)

    with pytest.raises(TypeError) as e:
        b.a_member = None

    assert "Cannot modify an immutable class" in str(e)
    assert 42 == b.a_member


def test_get_all_slots(client, runner):
    """Test get all slots from an Immutable subclasses."""
    b = B(a_member=42, b_member=43)
    _ = b.__getstate__()

    assert ("b_member", "a_member", "__weakref__") == B.__all_slots__
