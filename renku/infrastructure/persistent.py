# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Base Renku persistent class."""

import persistent


class Persistent(persistent.Persistent):
    """Base Persistent class for renku classes.

    Subclasses are assumed to be immutable once persisted to the database. If a class shouldn't be immutable then
    subclass it directly from persistent.Persistent.
    """

    _v_immutable = False

    def reassign_oid(self):
        """Reassign ``oid`` (after assigning a new identifier for example)."""
        from renku.infrastructure.database import Database

        if self._p_jar is not None:
            self._p_jar.remove_from_cache(self)

        self._p_oid = None
        self._p_oid = Database.generate_oid(self)

    @property
    def immutable(self):
        """Return if object is immutable."""
        return self._v_immutable

    def freeze(self):
        """Set immutable property."""
        self._v_immutable = True

    def unfreeze(self):
        """Allows modifying an immutable object.

        Don't make an object mutable unless the intention is to drop the changes or modify the object in-place. Modified
        objects will be updated in-place which results in a binary diff when persisted. Normally, we want to create a
        mutable copy and persist it as a new object.
        """
        self._v_immutable = False

    def __setattr__(self, key, value):
        if self._v_immutable and key != "__weakref__" and not key.startswith("_p_") and not key.startswith("_v_"):
            raise RuntimeError(f"Cannot modify immutable object {self}.{key}")

        super().__setattr__(key, value)

    @property
    def __name__(self):
        return self.__class__.__name__
