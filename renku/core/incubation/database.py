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

import datetime
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import uuid4

import BTrees.OOBTree
from persistent import GHOST, UPTODATE, Persistent, PickleCache
from persistent.interfaces import IPersistentDataManager
from ZODB.interfaces import IConnection
from ZODB.POSException import POSKeyError
from ZODB.utils import z64
from zope.interface import implementer

MARKER = object()


# NOTE These are used as _p_serial to mark if an object was read from storage or is new
NEW = z64  # NOTE: Do not change this value since this is the default when a Persistent object is created
PERSISTED = b"1" * 8

MIN_COMPRESSED_FILENAME_LENGTH = 64


@implementer(IConnection, IPersistentDataManager)
class Database:
    """The Metadata Object Database."""

    ROOT_TYPE_NAMES = ("Activity", "Entity", "Plan")

    def __init__(self, storage):
        self._storage: Storage = storage
        self._cache = PickleCache(self)
        # The pre-cache is used by get to avoid infinite loops when objects load their state
        self._pre_cache: Dict[bytes, Persistent] = {}
        # Objects added explicitly by add() or when serializing other objects. After commit they are moved to _cache.
        self._objects_to_commit: Dict[bytes, Persistent] = {}
        self._reader: ObjectReader = ObjectReader(database=self)
        self._writer: ObjectWriter = ObjectWriter(database=self)
        self._root: Optional[BTrees.OOBTree.BTree] = None

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> "Database":
        """Create a Storage and Database using the given path."""
        storage = Storage(path)
        return Database(storage=storage)

    @property
    def root(self):
        """Return the database root object."""
        if not self._root:
            try:
                self._root = self.get(b"root")
            except POSKeyError:
                self._root = BTrees.OOBTree.BTree()
                self.add(self._root, b"root")

        # NOTE: Make sure that all root objects have an entry
        self._create_root_types()

        return self._root

    def _create_root_types(self):
        for root_type in Database.ROOT_TYPE_NAMES:
            if root_type in self._root:
                continue

            object = BTrees.OOBTree.BTree()
            object._p_oid = root_type.encode("ascii")
            self._root[root_type] = object

    @staticmethod
    def new_oid():
        """Generate a random oid."""
        return f"{uuid4().hex}{uuid4().hex}".encode("ascii")

    @staticmethod
    def generate_oid(object) -> bytes:
        """Generate oid for a Persistent object based on its id."""
        oid = getattr(object, "_p_oid")
        if oid:
            assert isinstance(oid, bytes)
            return oid

        id: str = getattr(object, "id") or getattr(object, "_id")
        if id:
            return Database.hash_id(id)

        return Database.new_oid()

    @staticmethod
    def hash_id(id: str) -> bytes:
        """Return oid from id."""
        return hashlib.sha3_256(id.encode("utf-8")).hexdigest().encode("ascii")

    def add(self, object: Persistent, oid: bytes = None):
        """Add a new object to the database and assign an oid to it."""
        p_oid = getattr(object, "_p_oid", MARKER)
        if p_oid is MARKER:
            raise TypeError("Only first-class persistent objects may be added to database.", object)

        if oid is not None:
            assert isinstance(oid, bytes), f"Invalid 'oid' type: '{type(oid)}'"
        else:
            if object._p_oid is None:
                assert getattr(object, "id") is not None, f"Object does not have 'id': {object}"
            oid = self.generate_oid(object)

        object._p_jar = self
        object._p_oid = oid
        object._p_serial = NEW
        self.register(object)

    def oldstate(self, object, tid):
        """See persistent.interfaces.IPersistentDataManager::oldstate."""
        raise NotImplementedError

    def setstate(self, object):
        """Load the state for a ghost object."""
        oid = object._p_oid

        data = self._storage.load(filename=self._get_filename_from_oid(oid))
        self._reader.set_ghost_state(object, data)

        object._p_serial = PERSISTED

    def register(self, object: Persistent):
        """See persistent.interfaces.IPersistentDataManager::register."""
        if object._p_oid is None:
            object._p_oid = self.generate_oid(object)
        object._p_jar = self
        self._objects_to_commit[object._p_oid] = object

    def get(self, oid: Union[bytes, str]) -> Persistent:
        """Get the object by oid."""
        if oid in Database.ROOT_TYPE_NAMES:
            return self.root[oid]

        if isinstance(oid, str):
            oid = oid.encode("utf-8")

        object = self.get_cached(oid)
        if object is not None:
            return object

        data = self._storage.load(filename=self._get_filename_from_oid(oid))
        object = self._reader.deserialize(data)
        object._p_changed = 0
        object._p_serial = PERSISTED

        # NOTE: Avoid infinite loop if object tries to load its state before it is added to the cache
        self._pre_cache[oid] = object
        self._cache[oid] = object
        self._pre_cache.pop(oid)

        return object

    def get_cached(self, oid: bytes) -> Optional[Persistent]:
        """Return an object if it is in the cache or will be committed."""
        object = self._cache.get(oid)
        if object is not None:
            return object

        object = self._pre_cache.get(oid)
        if object is not None:
            return object

        object = self._objects_to_commit.get(oid)
        if object is not None:
            return object

    def commit(self):
        """Commit modified and new objects."""
        while self._objects_to_commit:
            oid, object = self._objects_to_commit.popitem()

            if not object._p_changed and object._p_serial != NEW:
                continue

            object_type = type(object).__name__
            if object_type in Database.ROOT_TYPE_NAMES:
                oid = oid.decode("ascii")
                root_object = self.root[object_type]
                root_object[oid] = object

            self._store_object(object)

    def _store_object(self, object: Persistent):
        oid = object._p_oid

        data = self._writer.serialize(object)

        self._storage.store(filename=self._get_filename_from_oid(oid), data=data)
        self._cache[oid] = object

        self._cache.update_object_size_estimation(oid, 1)
        object._p_estimated_size = 1

        object._p_changed = 0  # NOTE: transition from changed to up-to-date
        object._p_serial = PERSISTED

    @staticmethod
    def _get_filename_from_oid(oid: bytes) -> str:
        return oid.decode("ascii").lower()

    def new_ghost(self, oid: bytes, object: Persistent):
        """Create a new ghost object."""
        self._cache.new_ghost(oid, object)

    def replace(self, object: Persistent):
        """Remove an object by creating a new oid for it."""
        try:
            del self._cache[object._p_oid]
        except KeyError:
            pass
        else:
            type_name = type(object).__name__
            if type_name in Database.ROOT_TYPE_NAMES:
                root_object = self.root[type_name]
                del root_object[object._p_oid.decode("ascii")]

        object._p_oid = None
        self.add(object)

    def readCurrent(self, object):
        """We don't use this method but some Persistent logic require its existence."""
        assert object._p_jar is self
        assert object._p_oid is not None


class Storage:
    """Store Persistent objects on the disk."""

    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def store(self, filename: str, data: Union[Dict, List]):
        """Store object."""
        assert isinstance(filename, str)

        compressed = len(filename) >= MIN_COMPRESSED_FILENAME_LENGTH

        if compressed:
            path = self.path / filename[0:2] / filename[2:4] / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            open_func = open  # TODO: Change this to gzip.open for the final version
        else:
            path = self.path / filename
            open_func = open

        with open_func(path, "w") as file:
            json.dump(data, file, ensure_ascii=False, sort_keys=True, indent=2)

    def load(self, filename: str):
        """Load data for object with object id oid."""
        assert isinstance(filename, str)

        compressed = len(filename) >= MIN_COMPRESSED_FILENAME_LENGTH

        if compressed:
            path = self.path / filename[0:2] / filename[2:4] / filename
            open_func = open  # TODO: Change this to gzip.open for the final version
        else:
            path = self.path / filename
            open_func = open

        if not path.exists():
            raise POSKeyError(filename)

        with open_func(path) as file:
            data = json.load(file)

        return data


class ObjectWriter:
    """Serialize objects for storage in storage."""

    def __init__(self, database: Database):
        self._database: Database = database

    @staticmethod
    def _get_type(object) -> str:
        object_type = type(object)
        return f"{object_type.__module__}.{object_type.__qualname__}"

    def serialize(self, object: Persistent):
        """Convert an object to JSON."""
        assert isinstance(object, Persistent), f"Cannot serialize object of type '{type(object)}': {object}"
        assert object._p_oid, f"Object does not have an oid: '{object}'"
        assert object._p_jar is not None, f"Object is not associated with a DataManager: '{object._p_oid}'"

        state = object.__getstate__()
        data = self._serialize_helper(state)

        if not isinstance(data, dict):
            data = {"@value": data}

        data["@type"] = self._get_type(object)
        data["@oid"] = object._p_oid.decode("ascii")

        return data

    def _serialize_helper(self, object):
        # TODO: Add support for weakref. See persistent.wref.WeakRef
        if object is None:
            return None
        elif isinstance(object, list):
            return [self._serialize_helper(value) for value in object]
        elif isinstance(object, tuple):
            return tuple([self._serialize_helper(value) for value in object])
        elif isinstance(object, dict):
            for key, value in object.items():
                object[key] = self._serialize_helper(value)
            return object
        elif isinstance(object, (int, float, str, bool)):
            return object
        elif isinstance(object, datetime.datetime):
            return {"@type": self._get_type(object), "@value": object.isoformat()}
        elif isinstance(object, Persistent):
            if not object._p_oid:
                object._p_oid = Database.generate_oid(object)
            if object._p_state not in [GHOST, UPTODATE] or (object._p_state == UPTODATE and object._p_serial == NEW):
                self._database.add(object)
            return {"@type": self._get_type(object), "@oid": object._p_oid.decode("ascii"), "@reference": True}
        elif hasattr(object, "__getstate__"):
            state = object.__getstate__()
            if isinstance(state, dict) and "_id" in state:  # TODO: Remove this once all Renku classes have 'id' field
                state["id"] = state.pop("_id")
            return self._serialize_helper(state)
        else:
            state = object.__dict__.copy()
            state = self._serialize_helper(state)
            state["@type"] = self._get_type(object)
            if "_id" in state:  # TODO: Remove this once all Renku classes have 'id' field
                state["id"] = state.pop("_id")
            return state


class ObjectReader:
    """Deserialize objects loaded from storage."""

    def __init__(self, database: Database):
        self._classes: Dict[str, type] = {}
        self._database = database

    def _get_class(self, object_type: str) -> type:
        cls = self._classes.get(object_type)
        if cls:
            return cls

        components = object_type.split(".")
        module_name = components[0]

        if module_name not in ["renku", "datetime", "BTrees"]:
            raise TypeError(f"Objects of type '{object_type}' are not allowed")

        module = __import__(module_name)

        for component in components[1:]:
            module = getattr(module, component)

        self._classes[object_type] = module
        return module

    def set_ghost_state(self, object: Persistent, data: Dict):
        """Set state of a Persistent ghost object."""
        state = self._deserialize_helper(data, create=False)
        if isinstance(object, BTrees.OOBTree.OOBTree):
            state = self._to_tuple(state)

        object.__setstate__(state)

    def _to_tuple(self, data):
        if isinstance(data, list):
            return tuple(self._to_tuple(value) for value in data)
        return data

    def deserialize(self, data):
        """Convert JSON to Persistent object."""
        oid = data["@oid"].encode("ascii")

        object = self._deserialize_helper(data)

        object._p_oid = oid
        object._p_jar = self._database

        return object

    def _deserialize_helper(self, data, create=True):
        # TODO WeakRef
        if data is None:
            return None
        elif isinstance(data, (int, float, str, bool)):
            return data
        elif isinstance(data, list):
            return [self._deserialize_helper(value) for value in data]
        elif isinstance(data, tuple):
            return tuple([self._deserialize_helper(value) for value in data])
        else:
            assert isinstance(data, dict), f"Data must be a list: '{type(data)}'"

            object_type = data.pop("@type", None)
            if not object_type:  # NOTE: A normal dict value
                assert "@oid" not in data
                for key, value in data.items():
                    data[key] = self._deserialize_helper(value)
                return data

            cls = self._get_class(object_type)

            if issubclass(cls, datetime.datetime):
                assert create
                value = data["@value"]
                return datetime.datetime.fromisoformat(value)

            oid: str = data.pop("@oid", None)
            if oid:
                assert isinstance(oid, str)
                oid: bytes = oid.encode("ascii")

                if "@reference" in data and data["@reference"]:  # A reference
                    object = self._database.get_cached(oid)
                    if object:
                        return object
                    object = cls.__new__(cls)
                    assert isinstance(object, Persistent)
                    self._database.new_ghost(oid, object)
                    return object

            if "@value" in data:
                data = data["@value"]

            if isinstance(data, dict):
                for key, value in data.items():
                    data[key] = self._deserialize_helper(value)
            else:
                data = self._deserialize_helper(data)

            if not create:
                return data

            if hasattr(cls, "__setstate__"):
                object = cls.__new__(cls)
                if isinstance(object, BTrees.OOBTree.OOBTree):
                    data = self._to_tuple(data)
                object.__setstate__(data)
            else:
                assert isinstance(data, dict)
                object = cls(**data)

            return object
