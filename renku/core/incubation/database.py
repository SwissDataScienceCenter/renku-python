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
from typing import Dict, List, Optional, Tuple, Union
from uuid import uuid4

from BTrees.OOBTree import OOBTree
from ZODB.POSException import POSKeyError
from ZODB.utils import z64
from persistent import GHOST, UPTODATE, Persistent
# NOTE These are used as _p_serial to mark if an object was read from storage or is new
from persistent.interfaces import IPickleCache
from zope.interface import implementer

NEW = z64  # NOTE: Do not change this value since this is the default when a Persistent object is created
PERSISTED = b"1" * 8

OID_TYPE = str


def get_type_name(object) -> str:
    """Return fully-qualified object's type name."""
    object_type = object if isinstance(object, type) else type(object)
    return f"{object_type.__module__}.{object_type.__qualname__}"


def get_class(type_name: str) -> type:
    """Return the class for a fully-qualified type name."""
    components = type_name.split(".")
    module_name = components[0]

    if module_name not in ["renku", "datetime", "BTrees"]:
        raise TypeError(f"Objects of type '{type_name}' are not allowed")

    module = __import__(module_name)

    for component in components[1:]:
        try:
            module = getattr(module, component)
        except AttributeError:
            raise AttributeError(f"Cannot find attribute '{component}' in module '{module.__name__}'")

    return module


class Database:
    """The Metadata Object Database.

    It is equivalent to a ZODB.Connection and a persistent.DataManager. It implements ZODB.interfaces.IConnection and
    persistent.interfaces.IPersistentDataManager interfaces.
    """

    ROOT_OID = "root"
    ROOT_TYPE_NAMES = ("Activity", "Entity", "Plan")

    def __init__(self, storage):
        self._storage: Storage = storage
        self._cache = Cache()
        # The pre-cache is used by get to avoid infinite loops when objects load their state
        self._pre_cache: Dict[OID_TYPE, Persistent] = {}
        # Objects added explicitly by add() or when serializing other objects. After commit they are moved to _cache.
        self._objects_to_commit: Dict[OID_TYPE, Persistent] = {}
        self._reader: ObjectReader = ObjectReader(database=self)
        self._writer: ObjectWriter = ObjectWriter(database=self)
        self._root: Optional[OOBTree] = None
        self._root_types: Tuple[type, ...] = (OOBTree, Index)

        from renku.core.models.entity import Entity
        from renku.core.models.provenance.activity import Activity
        from renku.core.models.workflow.plan import Plan

        self.add_index(name="activities", model=Activity, attribute="id")
        self.add_index(name="plans", model=Plan, attribute="id")
        self.add_index(name="entities", model=Entity, attribute="id")

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> "Database":
        """Create a Storage and Database using the given path."""
        storage = Storage(path)
        return Database(storage=storage)

    @staticmethod
    def new_oid():
        """Generate a random oid."""
        return f"{uuid4().hex}{uuid4().hex}"

    @staticmethod
    def generate_oid(object: Persistent) -> OID_TYPE:
        """Generate oid for a Persistent object based on its id."""
        oid = getattr(object, "_p_oid")
        if oid:
            assert isinstance(oid, OID_TYPE)
            return oid

        id: str = getattr(object, "id", None) or getattr(object, "_id", None)
        if id:
            return Database.hash_id(id)

        return Database.new_oid()

    @staticmethod
    def hash_id(id: str) -> OID_TYPE:
        """Return oid from id."""
        return hashlib.sha3_256(id.encode("utf-8")).hexdigest()

    @property
    def root(self):
        """Return the database root object."""
        if not self._root:
            try:
                self._root = self.get(Database.ROOT_OID)
                root_types = {i.model for i in self._root.values() if i.model not in self._root_types}
                self._root_types += root_types
            except POSKeyError:
                self._root = OOBTree()
                self._add_internal(self._root, Database.ROOT_OID)

        return self._root

    def add_index(self, name: str, model: type, attribute: str):
        """Add an index."""
        assert len(self._objects_to_commit) == 0 or set(self._objects_to_commit.keys()) == {Database.ROOT_OID}
        root = self.root
        assert name not in root, f"Index already exists: '{name}'"

        root[name] = Index(name=name, model=model, attribute=attribute)
        if model not in self._root_types:
            self._root_types += (model,)

    def add(self, object: Persistent):
        """Add a new object to the database."""
        assert isinstance(object, self._root_types), f"Cannot add objects of type '{type(object)}'"

        if object._p_oid is None:
            assert getattr(object, "id", None) is not None, f"Object does not have 'id': {object}"
        oid = self.generate_oid(object)

        cached_object = self.get_cached(oid)
        if cached_object:
            assert cached_object is object, f"An object with oid '{oid}' is in the cache: {cached_object} != {object}"
            return

        self._update_indexes(object)
        self._add_internal(object, oid)

    def _update_indexes(self, object: Persistent):
        for index in self.root.values():
            index.update(object)

    def _add_internal(self, object: Persistent, oid: OID_TYPE):
        """Allow adding non-root types; used for adding root object."""
        assert isinstance(object, Persistent), f"Cannot add non-Persistent object: '{object}'"
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"

        object._p_oid = oid
        object._p_serial = NEW
        self.register(object)

    def oldstate(self, object, tid):
        """See persistent.interfaces.IPersistentDataManager::oldstate."""
        raise NotImplementedError

    def setstate(self, object: Persistent):
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

    def get(self, oid: OID_TYPE) -> Persistent:
        """Get the object by oid."""
        if oid != Database.ROOT_OID and oid in self.root:  # NOTE: Avoid looping if getting "root"
            return self.root[oid]

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

    def get_cached(self, oid: OID_TYPE) -> Optional[Persistent]:
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

            self._store_object(object)

    def _store_object(self, object: Persistent):
        oid = object._p_oid

        data = self._writer.serialize(object)

        self._storage.store(filename=self._get_filename_from_oid(oid), data=data)
        self._cache[oid] = object

        object._p_estimated_size = 0

        object._p_changed = 0  # NOTE: transition from changed to up-to-date
        object._p_serial = PERSISTED

    @staticmethod
    def _get_filename_from_oid(oid: OID_TYPE) -> str:
        return oid.lower()

    def new_ghost(self, oid: OID_TYPE, object: Persistent):
        """Create a new ghost object."""
        object._p_jar = self
        self._cache.new_ghost(oid, object)

    def replace(self, object: Persistent):
        """Remove an object by creating a new oid for it."""
        try:
            del self._cache[object._p_oid]
        except KeyError:
            pass
        else:
            # TODO: Optimize this
            for index in self.root:
                to_delete = []
                for key, value in index.items():
                    if object._p_oid == value._p_oid:
                        to_delete.append(key)

                for key in to_delete:
                    del index[key]

        object._p_oid = None
        self.add(object)

    def readCurrent(self, object):
        """We don't use this method but some Persistent logic require its existence."""
        assert object._p_jar is self
        assert object._p_oid is not None


@implementer(IPickleCache)
class Cache:
    """Database Cache."""

    def __init__(self):
        self._entries = {}

    def __len__(self):
        return len(self._entries)

    def __getitem__(self, oid):
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        return self._entries[oid]

    def __setitem__(self, oid, object):
        assert isinstance(object, Persistent), f"Cannot cache non-Persistent objects: '{object}'"
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"

        assert object._p_jar is not None, "Cached object jar missing"
        assert oid == object._p_oid, f"Cache key does not match oid: {oid} != {object._p_oid}"

        if oid in self._entries:
            existing_data = self.get(oid)
            if existing_data is not object:
                raise ValueError("A different object already has the same oid")

        self._entries[oid] = object

    def __delitem__(self, oid):
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        self._entries.pop(oid)

    def get(self, oid, default=None):
        """See IPickleCache."""
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        return self._entries.get(oid, default)

    def new_ghost(self, oid, object):
        """See IPickleCache."""
        assert object._p_oid is None, f"Object already has an oid: {object}"
        assert object._p_jar is not None, f"Object does not have a jar: {object}"
        assert oid not in self._entries, f"Duplicate oid: {oid}"

        object._p_oid = oid
        if object._p_state != GHOST:
            object._p_invalidate()

        self[oid] = object


class Index(Persistent):
    """Database index."""

    def __init__(self, name: str, model: type, attribute: str):
        assert name == name.lower(), f"Index name must be all lowercase: '{name}'."

        super().__init__()

        self._p_oid = f"{name}-index"
        self._name: str = name
        self._model: type = model
        self._attribute: str = attribute
        self._entries: OOBTree = OOBTree()
        self._entries._p_oid = name

    def __len__(self):
        return len(self._entries)

    def __getitem__(self, key):
        return self._entries[key]

    def __setitem__(self, key, object: Persistent):
        assert object._p_jar is not None, "Object's Database is missing"

        existing_object = self._entries.get(key)
        if existing_object and existing_object is not object:
            raise ValueError("A different object already has the same key")

        self._entries[key] = object

    def __delitem__(self, oid):
        self._entries.pop(oid)

    def __getstate__(self):
        return {
            "name": self._name,
            "model": get_type_name(self._model),
            "attribute": self._attribute,
            "index": self._entries,
        }

    def __setstate__(self, data):
        self._name = data.pop("name")
        self._model = get_class(data.pop("model"))
        self._attribute = data.pop("attribute")
        self._entries = data.pop("index")

    @property
    def name(self) -> str:
        """Return Index's name."""
        return self._name

    @property
    def model(self) -> type:
        """Return Index's model."""
        return self._model

    def get(self, oid, default=None):
        """Return an entry based on its key."""
        return self._entries.get(oid, default)

    def values(self):
        """Return a generator of values."""
        return self._entries.values()

    def items(self):
        """Return list of keys and values."""
        return self._entries.items()

    def update(self, object: Persistent):
        """Update index with object."""
        if not isinstance(object, self._model):
            return

        key = getattr(object, self._attribute)

        current_object = self._entries.get(key)
        if current_object:
            assert object is current_object, f"Another object with the same key exists: {key}:{current_object}"
        else:
            self._entries[key] = object


class Storage:
    """Store Persistent objects on the disk."""

    MIN_COMPRESSED_FILENAME_LENGTH = 64

    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def store(self, filename: str, data: Union[Dict, List]):
        """Store object."""
        assert isinstance(filename, str)

        compressed = len(filename) >= Storage.MIN_COMPRESSED_FILENAME_LENGTH
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

        compressed = len(filename) >= Storage.MIN_COMPRESSED_FILENAME_LENGTH
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

    def serialize(self, object: Persistent):
        """Convert an object to JSON."""
        assert isinstance(object, Persistent), f"Cannot serialize object of type '{type(object)}': {object}"
        assert object._p_oid, f"Object does not have an oid: '{object}'"
        assert object._p_jar is not None, f"Object is not associated with a Database: '{object._p_oid}'"

        state = object.__getstate__()
        data = self._serialize_helper(state)

        if not isinstance(data, dict):
            data = {"@value": data}

        data["@type"] = get_type_name(object)
        data["@oid"] = object._p_oid

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
            return {"@type": get_type_name(object), "@value": object.isoformat()}
        elif isinstance(object, Index):
            # NOTE: Include Index objects directly to their parent object (i.e. root)
            assert object._p_oid is not None, f"Index has no oid: {object}"
            state = object.__getstate__()
            state = self._serialize_helper(state)
            state["@type"] = get_type_name(object)
            state["@oid"] = object._p_oid
            return state
        elif isinstance(object, Persistent):
            if not object._p_oid:
                object._p_oid = Database.generate_oid(object)
            if object._p_state not in [GHOST, UPTODATE] or (object._p_state == UPTODATE and object._p_serial == NEW):
                self._database.add(object)
            return {"@type": get_type_name(object), "@oid": object._p_oid, "@reference": True}
        elif hasattr(object, "__getstate__"):
            state = object.__getstate__()
            if isinstance(state, dict) and "_id" in state:  # TODO: Remove this once all Renku classes have 'id' field
                state["id"] = state.pop("_id")
            return self._serialize_helper(state)
        else:
            state = object.__dict__.copy()
            state = self._serialize_helper(state)
            state["@type"] = get_type_name(object)
            if "_id" in state:  # TODO: Remove this once all Renku classes have 'id' field
                state["id"] = state.pop("_id")
            return state


class ObjectReader:
    """Deserialize objects loaded from storage."""

    def __init__(self, database: Database):
        self._classes: Dict[str, type] = {}
        self._database = database

    def _get_class(self, type_name: str) -> type:
        cls = self._classes.get(type_name)
        if cls:
            return cls

        cls = get_class(type_name)

        self._classes[type_name] = cls
        return cls

    def set_ghost_state(self, object: Persistent, data: Dict):
        """Set state of a Persistent ghost object."""
        state = self._deserialize_helper(data, create=False)
        if isinstance(object, OOBTree):
            state = self._to_tuple(state)

        object.__setstate__(state)

    def _to_tuple(self, data):
        if isinstance(data, list):
            return tuple(self._to_tuple(value) for value in data)
        return data

    def deserialize(self, data):
        """Convert JSON to Persistent object."""
        oid = data["@oid"]

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

                if "@reference" in data and data["@reference"]:  # A reference
                    object = self._database.get_cached(oid)
                    if object:
                        return object
                    assert issubclass(cls, Persistent)
                    object = cls.__new__(cls)
                    self._database.new_ghost(oid, object)
                    return object
                elif issubclass(cls, Index):
                    object = self._database.get_cached(oid)
                    if object:
                        return object
                    object = cls.__new__(cls)
                    object._p_oid = oid
                    self.set_ghost_state(object, data)
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
                if isinstance(object, OOBTree):
                    data = self._to_tuple(data)

                object.__setstate__(data)
            else:
                assert isinstance(data, dict)
                object = cls(**data)

            return object
