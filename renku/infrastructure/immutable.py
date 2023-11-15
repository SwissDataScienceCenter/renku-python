# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
from typing import Tuple, cast


class Slots:
    """An immutable class.

    Subclasses are supposed to use __slots__ to define their members.
    """

    __slots__ = ("__weakref__",)
    __all_slots__: Tuple[str, ...] = tuple()

    def __new__(cls, *args, **kwargs):
        """Create and return an empty instance of the class."""
        if not cls.__all_slots__:
            cls.__all_slots__ = cast(Tuple[str, ...], cls._get_all_slots())

        return object.__new__(cls)

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)

    @classmethod
    def make_instance(cls, **kwargs):
        """Instantiate from the given parameters."""
        return cls(**kwargs)

    def __getstate__(self):
        return {name: getattr(self, name, None) for name in self.__class__.__all_slots__ if name != "__weakref__"}

    def __setstate__(self, state):
        new_attributes = set(self.__all_slots__) - set(state)
        for name in new_attributes:
            if name != "__weakref__":
                object.__setattr__(self, name, None)

        for name, value in state.items():
            object.__setattr__(self, name, value)

    @classmethod
    def _get_all_slots(cls):
        all_slots = set()
        for klass in cls.mro():
            if not hasattr(klass, "__slots__"):
                continue
            slots = {klass.__slots__} if isinstance(klass.__slots__, str) else set(klass.__slots__)
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
    def make_instance(cls, instance):
        """Return a cached instance if available otherwise create an instance from the given parameters."""
        if getattr(cls._local, "cache", None) is None:
            cls._local.cache = weakref.WeakValueDictionary()

        id = getattr(instance, "id", None)
        existing_instance = cls._local.cache.get(id)
        if existing_instance:
            return existing_instance

        if id is not None:
            cls._local.cache[id] = instance

        return instance

    def __setattr__(self, name, value):
        if name != "__weakref__":
            raise TypeError(f"Cannot modify an immutable class {self} {self.__class__}")

        object.__setattr__(self, name, value)

    @property
    def __name__(self):
        return self.__class__.__name__


class DynamicProxy:
    """A proxy class to allow adding dynamic fields to slots/immutable classes."""

    def __init__(self, subject, update=True):
        self._subject = subject
        self._update = update

    def __getattr__(self, name):
        return getattr(self._subject, name)

    def __getattribute__(self, name: str):
        if name == "__class__":
            # NOTE: Makes isinstance() checks work with proxies
            return object.__getattribute__(self._subject, "__class__")

        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name == "_subject" or not hasattr(self._subject, name):
            super().__setattr__(name, value)
        else:
            if not self._update:
                raise ValueError(f"Cannot set attribute '{name}' on {self._subject}.")
            setattr(self._subject, name, value)
