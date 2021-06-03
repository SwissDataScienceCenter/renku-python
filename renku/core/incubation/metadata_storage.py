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
"""Custom database for store Persistent objects."""

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Union

import BTrees.OOBTree
import transaction
from ZODB.POSException import POSKeyError
from persistent import Persistent, PickleCache
from persistent.interfaces import IPersistentDataManager
# from ZODB import FileStorage
from ZODB.broken import find_global
from ZODB.Connection import RootConvenience
from ZODB.interfaces import IConnection, IStorage
from ZODB.serialize import ObjectReader, ObjectWriter
from ZODB.utils import z64
from zope.interface import implementer

from renku.core import errors

MARKER = object()


class Storage:
    """Store Persistent object on the disk."""

    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.automatic_id = len([1 for _ in self.path.glob("*")])

    def new_oid(self):
        self.automatic_id += 1
        return bytes(str(self.automatic_id), "utf-8")

    def open(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass

    def release(self, *args, **kwargs):
        pass

    def store(self, oid: bytes, serial: str, data: bytes, version=None, transaction=None):
        """Store data for object with identifier oid."""
        if oid == z64:
            oid = "000000"
        if not isinstance(oid, str):
            oid = str(oid, "utf-8")
        filename = self.path / oid
        filename.write_bytes(data)

    def load(self, oid: bytes, version=''):
        """Load data for object with object id oid."""
        if oid == z64:
            oid = "000000"
        if not isinstance(oid, str):
            print("CONV", oid)
            oid = str(oid, "utf-8")
        filename = self.path / oid
        if not filename.exists():
            raise POSKeyError(oid)

        return filename.read_bytes(), z64


# @implementer(IConnection, IPersistentDataManager)
class MetadataStorage:
    """The Metadata Object Database.

    The database provides a few methods intended for application code -- open, close, undo, and pack -- and a large
    collection of methods for inspecting the database and its connections' caches.
    """

    def __init__(self, storage: Storage):

        self._storage: Storage = storage  # TODO: Is this needed
        self._is_open: bool = False

        # NOTE: This is used to avoid looping in the cache forever when an object is a ghost!?

        self._cache = PickleCache(self)
        self._reader = ObjectReader(self, self._cache, self.classFactory)

        # The pre-cache is used by get to avoid infinite loops when objects immediately load their state when they get
        # their persistent data set.
        self._pre_cache: Dict[bytes, Persistent] = {}

        # List of all objects (not oids) registered as modified by the persistence machinery, or by add(), or whose
        # access caused a ReadConflictError (just to be able to clean them up from the cache on abort with the other
        # modified objects). All objects of this list are either in _cache or in _added.
        self._registered_objects: List[Persistent] = []

        # ids and serials of objects for which readCurrent was called in a transaction.
        # self._readCurrent = {} # {oid ->serial}

        # Dict of oid->obj added explicitly through add(). Used as a preliminary cache until commit time when objects
        # are all moved to the real _cache. The objects are moved to _creating at commit time.
        self._added: Dict[bytes, Persistent] = {}

        # During commit this is turned into a list, which receives objects added as a side-effect of storing a modified
        # object.
        self._added_during_commit = None

        # During commit, all objects go to either _modified or _creating:

        # Dict of oid->flag of new objects (without serial), either
        # added by add() or implicitly added (discovered by the
        # serializer during commit). The flag is True for implicit
        # adding. Used during abort to remove created objects from the
        # _cache, and by persistent_id to check that a new object isn't
        # reachable from multiple databases.
        self._creating = {}  # {oid -> implicitly_added_flag}

        # List of oids of modified objects, which have to be invalidated
        # in the cache on abort and in other connections on finish.
        self._modified: List[bytes] = []

    def open(self):
        """Return a database Connection."""
        if self._is_open:
            raise errors.ConnectionStateError("The connection is already open.")

        self._is_open = True

        return self

    def close(self):
        """Close the connection."""
        if not self._is_open:
            raise errors.ConnectionStateError("The connection is already closed.")

        self._added = {}
        self._pre_cache = {}
        self._storage.release()  # TODO
        self._storage.close()  # TODO is this needed

        self._is_open = False

    def classFactory(self, connection, modulename, globalname):
        # Zope will rebind this method to arbitrary user code at runtime.
        return find_global(modulename, globalname)

    def add(self, object: Persistent, oid: bytes = None):
        """Add a new object 'obj' to the database and assign it an oid."""
        if not self._is_open:
            raise errors.ConnectionStateError("The connection is closed")

        print("ADD", object)

        p_oid = getattr(object, "_p_oid", MARKER)
        if p_oid is MARKER:
            raise TypeError("Only first-class persistent objects may be added to a Connection.", object)

        if object._p_jar is None:
            object._p_jar = self
            if oid is None:
                oid = self._generate_object_id(object)
            object._p_oid = oid
            self._added[oid] = object
            self.register(object)
        elif object._p_jar is not self:
            raise errors.ObjectStateError(f"Object '{object._p_oid}' already has a DataManager: '{object._p_jar}'")

    @staticmethod
    def _generate_object_id(object: Persistent) -> bytes:
        id = object.id
        # TODO: What if object does not have an id
        return bytes(hashlib.sha3_512(id.encode("utf-8")).hexdigest(), "utf-8")

    def get(self, oid):
        """Get the object by oid."""
        if not self._is_open:
            raise errors.ConnectionStateError("The database connection is closed")

        obj = self._cache.get(oid)
        if obj is not None:
            return obj
        obj = self._added.get(oid)
        if obj is not None:
            return obj
        obj = self._pre_cache.get(oid)
        if obj is not None:
            return obj

        p, _ = self._storage.load(oid)
        obj = self._reader.getGhost(p)

        # Avoid infinite loop if obj tries to load its state before
        # it is added to the cache and it's state refers to it.
        # (This will typically be the case for non-ghostifyable objects,
        # like persistent caches.)
        self._pre_cache[oid] = obj
        self._cache.new_ghost(oid, obj)
        self._pre_cache.pop(oid)
        return obj

    def new_oid(self):
        return self._storage.new_oid()

    @property
    def root(self):
        """Return the database root object."""
        try:
            return RootConvenience(self.get(z64))
        except:
            from persistent.mapping import PersistentMapping
            root = PersistentMapping()
            self.add(root, z64)
            return RootConvenience(self.get(z64))

    def commit(self):
        """Commit changes to an object"""

        # Just in case an object is added as a side-effect of storing
        # a modified object.  If, for example, a __getstate__() method
        # calls add(), the newly added objects will show up in
        # _added_during_commit.  This sounds insane, but has actually
        # happened.

        self._added_during_commit = []

        for obj in self._registered_objects:
            oid = obj._p_oid
            assert oid

            if obj._p_jar is not self:
                raise errors.ObjectStateError(f"Object '{obj}' doesn't belong to DataManager.")
            elif oid in self._added:
                assert obj._p_serial == z64
            elif oid in self._creating or not obj._p_changed:
                # Nothing to do.  It's been said that it's legal, e.g., for
                # an object to set _p_changed to false after it's been
                # changed and registered.
                # And new objects that are registered after any referrer are
                # already processed.
                continue

            self._store_objects(ObjectWriter(obj), None)

        for obj in self._added_during_commit:
            self._store_objects(ObjectWriter(obj), None)
        self._added_during_commit = None

    def _store_objects(self, writer, transaction):
        for obj in writer:
            oid = obj._p_oid
            serial = getattr(obj, "_p_serial", z64)

            if serial == z64:
                # obj is a new object

                # Because obj was added, it is now in _creating, so it
                # can be removed from _added.  If oid wasn't in
                # adding, then we are adding it implicitly.

                implicitly_adding = self._added.pop(oid, None) is None
                self._creating[oid] = implicitly_adding
            else:
                self._modified.append(oid)

            p = writer.serialize(obj)  # This calls __getstate__ of obj

            s = self._storage.store(oid, serial, p)

            try:
                self._cache[oid] = obj
            except:
                # Dang, I bet it's wrapped:
                # TODO:  Deprecate, then remove, this.
                if hasattr(obj, "aq_base"):
                    self._cache[oid] = obj.aq_base
                else:
                    raise

            self._cache.update_object_size_estimation(oid, len(p))
            obj._p_estimated_size = len(p)

            # # if we write an object, we don't want to check if it was read
            # # while current.  This is a convenient choke point to do this.
            # self._readCurrent.pop(oid, None)
            if s:
                # savepoint
                obj._p_changed = 0  # transition from changed to up-to-date
                obj._p_serial = s

    # persistent.interfaces.IPersistentDataManager

    def oldstate(self, obj, tid):
        """See persistent.interfaces.IPersistentDataManager::oldstate."""
        raise NotImplementedError

    def setstate(self, object):
        """Load the state for a ghost object."""
        if not self._is_open:
            raise errors.ConnectionStateError("Connection is closed.")

        oid = object._p_oid

        p, serial = self._storage.load(oid)
        self._reader.setGhostState(object, p)

        print(p, serial, oid)

        object._p_serial = serial

    def register(self, object):
        """See persistent.interfaces.IPersistentDataManager::register."""
        self._registered_objects.append(object)

    ####################

    def get_object_by_id(self, type: str, id: str) -> Any:
        """Return an object with specific type and id."""
        root = self.root()
        if type in root:
            objects = root[type]
            if id in objects:
                return objects[id]

    def store_object(self, type: str, id: str, object: Persistent):
        """Store an object."""
        root = self.root()
        if type not in root:
            root[type] = BTrees.OOBTree.BTree()
        objects = root[type]
        objects[id] = object
        # self.add(object)
        print("STORE", id, object, object._p_jar, object._p_oid, object._p_serial, object._p_changed)
        # TODO commit transaction properly
        # self.commit()

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> "MetadataStorage":
        """Create a FileStorage and return a storage.

        :param str path: Path to the parent directory of storage file
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        # path = path / "store.fs"
        storage = Storage(str(path))

        return cls(storage)
