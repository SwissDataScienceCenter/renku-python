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
import weakref
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import uuid4

from BTrees.OOBTree import OOBTree
from persistent import GHOST, UPTODATE, Persistent
from persistent.interfaces import IPickleCache
from ZODB.utils import z64
from zope.interface import implementer

from renku.core import errors
from renku.core.incubation.immutable import Immutable

OID_TYPE = str
MARKER = object()

"""NOTE: These are used as _p_serial to mark if an object was read from storage or is new"""
NEW = z64  # NOTE: Do not change this value since this is the default when a Persistent object is created
PERSISTED = b"1" * 8


def get_type_name(object) -> Optional[str]:
    """Return fully-qualified object's type name."""
    if object is None:
        return None

    object_type = object if isinstance(object, type) else type(object)
    return f"{object_type.__module__}.{object_type.__qualname__}"


def get_class(type_name: Optional[str]) -> Optional[type]:
    """Return the class for a fully-qualified type name."""
    if type_name is None:
        return None

    components = type_name.split(".")
    module_name = components[0]

    if module_name not in ["BTrees", "builtins", "datetime", "persistent", "renku"]:
        raise TypeError(f"Objects of type '{type_name}' are not allowed")

    module = __import__(module_name)

    return get_attribute(module, components[1:])


def get_attribute(object, name: Union[List[str], str]):
    """Return an attribute of an object."""
    components = name.split(".") if isinstance(name, str) else name

    for component in components:
        object = getattr(object, component)

    return object


class Database:
    """The Metadata Object Database.

    This class is equivalent to a persistent.DataManager and implements persistent.interfaces.IPersistentDataManager
    interface.
    """

    ROOT_OID = "root"

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

        self._initialize_root()

    @classmethod
    def from_path(cls, path: Union[Path, str]) -> "Database":
        """Create a Storage and Database using the given path."""
        storage = Storage(path)
        return Database(storage=storage)

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

    @staticmethod
    def new_oid():
        """Generate a random oid."""
        return f"{uuid4().hex}{uuid4().hex}"

    @staticmethod
    def _get_filename_from_oid(oid: OID_TYPE) -> str:
        return oid.lower()

    def __getitem__(self, key) -> "Index":
        return self._root[key]

    def _initialize_root(self):
        """Initialize root object."""
        if not self._root:
            try:
                self._root = self.get(Database.ROOT_OID)
            except errors.ObjectNotFoundError:
                self._root = OOBTree()
                self._root._p_oid = Database.ROOT_OID
                self.register(self._root)

    def add_index(self, name: str, object_type: type, attribute: str = None, key_type: type = None) -> "Index":
        """Add an index."""
        assert name not in self._root, f"Index already exists: '{name}'"

        index = Index(name=name, object_type=object_type, attribute=attribute, key_type=key_type)
        index._p_jar = self

        self._root[name] = index

        return index

    def add(self, object: Persistent, oid: OID_TYPE):
        """Add a new object to the database.

        NOTE: Normally, we add objects to indexes but this method adds objects directly to Dataset's root. Use it only
        for singleton objects that have no Index defined for them (e.g. Project).
        """
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        object._p_oid = oid

        self.register(object)

    def register(self, object: Persistent):
        """Register a Persistent object to be stored.

        NOTE: When a Persistent object is changed it calls this method.
        """
        assert isinstance(object, Persistent), f"Cannot add non-Persistent object: '{object}'"

        if object._p_oid is None:
            object._p_oid = self.generate_oid(object)

        object._p_jar = self
        # object._p_serial = NEW
        self._objects_to_commit[object._p_oid] = object

    def get(self, oid: OID_TYPE) -> Persistent:
        """Get the object by oid."""
        if oid != Database.ROOT_OID and oid in self._root:  # NOTE: Avoid looping if getting "root"
            return self._root[oid]

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

    def get_by_id(self, id: str) -> Persistent:
        """Return an object by its id."""
        oid = Database.hash_id(id)
        return self.get(oid)

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

    def new_ghost(self, oid: OID_TYPE, object: Persistent):
        """Create a new ghost object."""
        object._p_jar = self
        self._cache.new_ghost(oid, object)

    def setstate(self, object: Persistent):
        """Load the state for a ghost object."""
        data = self._storage.load(filename=self._get_filename_from_oid(object._p_oid))
        self._reader.set_ghost_state(object, data)
        object._p_serial = PERSISTED

    def commit(self):
        """Commit modified and new objects."""
        while self._objects_to_commit:
            _, object = self._objects_to_commit.popitem()
            if object._p_changed or object._p_serial == NEW:
                self._store_object(object)

    def _store_object(self, object: Persistent):
        data = self._writer.serialize(object)
        self._storage.store(filename=self._get_filename_from_oid(object._p_oid), data=data)

        self._cache[object._p_oid] = object

        object._p_changed = 0  # NOTE: transition from changed to up-to-date
        object._p_serial = PERSISTED

    def remove_from_cache(self, object: Persistent):
        """Remove an object from cache."""
        oid = object._p_oid
        self._cache.pop(oid, None)
        self._pre_cache.pop(oid, None)
        self._objects_to_commit.pop(oid, None)

    def readCurrent(self, object):
        """We don't use this method but some Persistent logic require its existence."""
        assert object._p_jar is self
        assert object._p_oid is not None

    def oldstate(self, object, tid):
        """See persistent.interfaces.IPersistentDataManager::oldstate."""
        raise NotImplementedError


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

        # FIXME: This was commented out because dataset migration was failing. Resolve the issue and uncomment this.
        # if oid in self._entries:
        #     existing_data = self.get(oid)
        #     if existing_data is not object:
        #         raise ValueError(f"The same oid exists: {existing_data} != {object}")

        self._entries[oid] = object

    def __delitem__(self, oid):
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        self._entries.pop(oid)

    def pop(self, oid, default=MARKER):
        """Remove and return an object."""
        return self._entries.pop(oid) if default is MARKER else self._entries.pop(oid, default)

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

    def __init__(self, *, name: str, object_type, attribute: Optional[str], key_type=None):
        """
        Create an index where keys are extracted using `attribute` from an object or a key.

        @param name: Index's name
        @param object_type: Type of objects that the index points to
        @param attribute: Name of an attribute to be used to automatically generate a key (e.g. `entity.path`)
        @param key_type: Type of keys. If not None then a key must be provided when updating the index
        """
        assert name == name.lower(), f"Index name must be all lowercase: '{name}'."

        super().__init__()

        self._p_oid = f"{name}-index"
        self._name: str = name
        self._object_type = object_type
        self._key_type = key_type
        self._attribute: Optional[str] = attribute
        self._entries: OOBTree = OOBTree()
        self._entries._p_oid = name

    def __len__(self):
        return len(self._entries)

    def __contains__(self, key):
        return key in self._entries

    def __getitem__(self, key):
        return self._entries[key]

    def __setitem__(self, key, value):
        # NOTE: if Index is using a key object then we cannot check if key is valid. It's safer to use `add` method
        # instead of setting values directly.
        self._verify_and_get_key(object=value, key_object=None, key=key, missing_key_object_ok=True)

        self._entries[key] = value

    def __getstate__(self):
        return {
            "name": self._name,
            "object_type": get_type_name(self._object_type),
            "key_type": get_type_name(self._key_type),
            "attribute": self._attribute,
            "entries": self._entries,
        }

    def __setstate__(self, data):
        self._name = data.pop("name")
        self._object_type = get_class(data.pop("object_type"))
        self._key_type = get_class(data.pop("key_type"))
        self._attribute = data.pop("attribute")
        self._entries = data.pop("entries")

    @property
    def name(self) -> str:
        """Return Index's name."""
        return self._name

    @property
    def object_type(self) -> type:
        """Return Index's object_type."""
        return self._object_type

    def get(self, key, default=None):
        """Return an entry based on its key."""
        return self._entries.get(key, default)

    def pop(self, key, default=MARKER):
        """Remove and return an object."""
        return self._entries.pop(key) if default is MARKER else self._entries.pop(key, default)

    def keys(self):
        """Return an iterator of keys."""
        return self._entries.keys()

    def values(self):
        """Return an iterator of values."""
        return self._entries.values()

    def items(self):
        """Return an iterator of keys and values."""
        return self._entries.items()

    def add(self, object: Persistent, *, key: Optional[str] = None, key_object=None):
        """Update index with object.

        If `Index._attribute` is not None then key is automatically generated.
        Key is extracted from `key_object` if it is not None; otherwise, it's extracted from `object`.
        """
        assert isinstance(object, self._object_type), f"Cannot add objects of type '{type(object)}'"

        key = self._verify_and_get_key(object=object, key_object=key_object, key=key, missing_key_object_ok=False)
        self._entries[key] = object

    def generate_key(self, object: Persistent, *, key_object=None):
        """Return index key for an object.

        Key is extracted from `key_object` if it is not None; otherwise, it's extracted from `object`.
        """
        return self._verify_and_get_key(object=object, key_object=key_object, key=None, missing_key_object_ok=False)

    def _verify_and_get_key(self, *, object: Persistent, key_object, key, missing_key_object_ok):
        if self._key_type:
            if not missing_key_object_ok:
                assert isinstance(key_object, self._key_type), f"Invalid key type: {type(key_object)} for '{self.name}'"
        else:
            assert key_object is None, f"Index '{self.name}' does not accept 'key_object'"

        if self._attribute:
            key_object = key_object or object
            correct_key = get_attribute(key_object, self._attribute)
            if key is not None:
                assert key == correct_key, f"Incorrect key for index '{self.name}': '{key}' != '{correct_key}'"
        else:
            assert key is not None, "No key is provided"
            correct_key = key

        return correct_key


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
            raise errors.ObjectNotFoundError(filename)

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
        assert object._p_jar is not None, f"Object is not associated with a Database: '{object}'"

        state = object.__getstate__()
        was_dict = isinstance(state, dict)
        data = self._serialize_helper(state)
        is_dict = isinstance(data, dict)

        if not is_dict or (is_dict and not was_dict):
            data = {"@value": data}

        data["@type"] = get_type_name(object)
        data["@oid"] = object._p_oid

        return data

    def _serialize_helper(self, object):
        # TODO: Raise an error if an unsupported object is being serialized
        if object is None:
            return None
        elif isinstance(object, (int, float, str, bool)):
            return object
        elif isinstance(object, list):
            return [self._serialize_helper(value) for value in object]
        elif isinstance(object, dict):
            for key, value in object.items():
                object[key] = self._serialize_helper(value)
            return object
        elif isinstance(object, Index):
            # NOTE: Index objects are not stored as references and are included in their parent object (i.e. root)
            state = object.__getstate__()
            state = self._serialize_helper(state)
            return {"@type": get_type_name(object), "@oid": object._p_oid, **state}
        elif isinstance(object, Persistent):
            if not object._p_oid:
                object._p_oid = Database.generate_oid(object)
            if object._p_state not in [GHOST, UPTODATE] or (object._p_state == UPTODATE and object._p_serial == NEW):
                self._database.register(object)
            return {"@type": get_type_name(object), "@oid": object._p_oid, "@reference": True}
        elif isinstance(object, datetime.datetime):
            value = object.isoformat()
        elif isinstance(object, tuple):
            value = tuple(self._serialize_helper(value) for value in object)
        elif hasattr(object, "__getstate__"):
            value = object.__getstate__()
            value = self._serialize_helper(value)
            assert not isinstance(value, dict) or "id" in value, f"Invalid object state: {value} for {object}"
        else:
            value = object.__dict__.copy()
            value = self._serialize_helper(value)
            assert "id" in value, f"Invalid object state: {value} for {object}"

        return {"@type": get_type_name(object), "@value": value}


class ObjectReader:
    """Deserialize objects loaded from storage."""

    def __init__(self, database: Database):
        self._classes: Dict[str, type] = {}
        self._database = database
        self._immutable_objects_cache = weakref.WeakValueDictionary()

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
        object.__setstate__(state)

    def deserialize(self, data):
        """Convert JSON to Persistent object."""
        oid = data["@oid"]

        object = self._deserialize_helper(data)

        object._p_oid = oid
        object._p_jar = self._database

        return object

    def _deserialize_helper(self, data, create=True):
        if data is None:
            return None
        elif isinstance(data, (int, float, str, bool)):
            return data
        elif isinstance(data, list):
            return [self._deserialize_helper(value) for value in data]
        else:
            assert isinstance(data, dict), f"Data must be a list: '{type(data)}'"

            if "@type" not in data:  # NOTE: A normal dict value
                assert "@oid" not in data
                for key, value in data.items():
                    data[key] = self._deserialize_helper(value)
                return data

            object_type = data.pop("@type")
            cls = self._get_class(object_type)

            if issubclass(cls, datetime.datetime):
                assert create
                data = data["@value"]
                return datetime.datetime.fromisoformat(data)
            elif issubclass(cls, tuple):
                data = data["@value"]
                return tuple(self._deserialize_helper(value) for value in data)

            oid: str = data.pop("@oid", None)
            if oid:
                assert isinstance(oid, str)

                if "@reference" in data and data["@reference"]:  # A reference
                    assert create, f"Cannot deserialize a reference without creating an instance {data}"
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

            data = self._deserialize_helper(data)

            if not create:
                return data

            if issubclass(cls, Persistent):
                object = cls.__new__(cls)
                object.__setstate__(data)
            else:
                assert isinstance(data, dict)

                if issubclass(cls, Immutable):
                    id = data["id"]
                    object = self._immutable_objects_cache.get(id)
                    if object:
                        return object

                    object = cls(**data)
                    self._immutable_objects_cache[id] = object
                else:
                    object = cls(**data)

            return object
