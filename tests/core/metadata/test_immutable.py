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

from renku.core.metadata.immutable import Immutable, Slots


class A(Slots):
    """Test class."""

    __slots__ = ("a_member",)


class B(A):
    """Test class."""

    __slots__ = "b_member"

    def __init__(self, *, b_member=None, **kwargs):
        super().__init__(b_member=b_member, **kwargs)


class C(Immutable):
    """Test class."""

    __slots__ = ("c_member",)


def test_instantiate():
    """Test instantiating Slots subclasses."""
    b = B(a_member=42, b_member=43)

    assert {"a_member": 42, "b_member": 43} == b.__getstate__()


def test_instantiate_incomplete():
    """Test instantiating Slots subclasses without setting all members."""
    b = B(a_member=42)

    assert {"a_member": 42, "b_member": None} == b.__getstate__()


def test_instantiate_invalid_member():
    """Test instantiating Slots subclasses and passing a non-member."""
    with pytest.raises(AttributeError) as e:
        B(c_member=42)

    assert "object has no attribute 'c_member'" in str(e)


def test_get_all_slots():
    """Test get all slots from an Immutable subclasses."""
    b = B(a_member=42, b_member=43)
    _ = b.__getstate__()

    assert {"b_member", "a_member", "__weakref__"} == set(B.__all_slots__)


def test_immutable_object_id():
    """Test Immutable subclasses have an `id` field."""
    c = C(id=42, c_member=43)

    assert {"c_member": 43, "id": 42} == c.__getstate__()


def test_cannot_mutate():
    """Test cannot mutate an Immutable subclasses."""
    c = C(c_member=42)

    with pytest.raises(TypeError) as e:
        c.c_member = None

    assert "Cannot modify an immutable class" in str(e)
    assert 42 == c.c_member


def test_immutable_objects_cache():
    """Test Immutable objects are cached once created."""
    data = {"id": 42, "c_member": 43}

    o1 = C.make_instance(C(**data))
    o2 = C.make_instance(C(**data))

    assert o1 is o2


def test_immutable_objects_cache_without_id():
    """Test Immutable objects cannot be cached if id is not set."""
    data = {"c_member": 43}

    o1 = C.make_instance(C(**data))
    o2 = C.make_instance(C(**data))

    assert o1 is not o2
