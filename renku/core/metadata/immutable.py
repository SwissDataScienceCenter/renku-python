# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Slots and Immutable classes."""

import threading
import weakref


class Slots:
    """An immutable class.

    Subclasses are supposed to use __slots__ to define their members.
    """

    __slots__ = ("__weakref__",)
    __all_slots__ = None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)

    @classmethod
    def make_instance(cls, **kwargs):
        """Instantiate from the given parameters."""
        return cls(**kwargs)

    def __getstate__(self):
        if not self.__class__.__all_slots__:
            self.__class__.__all_slots__ = self._get_all_slots()
        return {name: getattr(self, name, None) for name in self.__class__.__all_slots__ if name != "__weakref__"}

    def _get_all_slots(self):
        all_slots = set()
        for cls in self.__class__.mro():
            if not hasattr(cls, "__slots__"):
                continue
            slots = {cls.__slots__} if isinstance(cls.__slots__, str) else set(cls.__slots__)
            all_slots.update(slots)
        return tuple(all_slots)


class Immutable(Slots):
    """An immutable class that its instances can be cached and reused.

    Immutable subclasses should only contain immutable members. They must call super().__init__(...) to initialize their
    instances. They should not redefine `id` attribute.

    NOTE: Immutable objects must not be modified during the whole provenance and not just only during the object's
    lifetime. These is because we cache these objects and return a single instance if an object with the same `id`
    exists. For example, a DatasetFile object is not immutable across the whole provenance because once it gets removed
    its `date_removed` attribute is set which make the object different from a previous version. As a rule of thumb, an
    object can be immutable if all of its attributes values appear in its id.
    """

    __slots__ = ("id",)

    _local = threading.local()

    @classmethod
    def make_instance(cls, **kwargs):
        """Return a cached instance if available otherwise create an instance from the given parameters."""
        if getattr(cls._local, "cache", None) is None:
            cls._local.cache = weakref.WeakValueDictionary()

        id = kwargs.get("id")
        existing_instance = cls._local.cache.get(id)
        if existing_instance:
            return existing_instance

        instance = cls(**kwargs)
        if id is not None:
            cls._local.cache[id] = instance

        return instance

    def __setattr__(self, name, value):
        if name != "__weakref__":
            raise TypeError(f"Cannot modify an immutable class {self} {self.__class__}")

        object.__setattr__(self, name, value)
