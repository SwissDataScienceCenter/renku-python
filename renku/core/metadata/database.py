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
import importlib
import io
import json
from pathlib import Path
from types import BuiltinFunctionType, FunctionType
from typing import Dict, List, Optional, Union
from uuid import uuid4

import persistent
import zstandard as zstd
from BTrees.Length import Length
from BTrees.OOBTree import BTree, OOBucket, OOSet, OOTreeSet
from persistent import GHOST, UPTODATE
from persistent.interfaces import IPickleCache
from zc.relation.catalog import Catalog
from ZODB.utils import z64
from zope.interface import implementer
from zope.interface.interface import InterfaceClass

from renku.core import errors
from renku.core.metadata.immutable import Immutable
from renku.core.models.project import Project

OID_TYPE = str
TYPE_TYPE = "type"
FUNCTION_TYPE = "function"
REFERENCE_TYPE = "reference"
SET_TYPE = "set"
FROZEN_SET_TYPE = "frozenset"
MARKER = object()

"""NOTE: These are used as _p_serial to mark if an object was read from storage or is new"""
NEW = z64  # NOTE: Do not change this value since this is the default when a Persistent object is created
PERSISTED = b"1" * 8


def _is_module_allowed(module_name: str, type_name: str):
    """Checks whether it is allowed to import from the given module for security purposes."""

    if module_name not in ["BTrees", "builtins", "datetime", "persistent", "renku", "zc", "zope"]:
        raise TypeError(f"Objects of type '{type_name}' are not allowed")


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

    _is_module_allowed(module_name, type_name)

    module = __import__(module_name)

    return get_attribute(module, components[1:])


def get_attribute(object, name: Union[List[str], str]):
    """Return an attribute of an object."""
    import sys

    components = name.split(".") if isinstance(name, str) else name

    def _module_name(o):
        return o.__module__ if hasattr(o, "__module__") else o.__name__

    module_name = _module_name(object)
    root_module_name = module_name.split(".")[0]

    for component in components:
        module_name = _module_name(object)
        if not hasattr(object, component) and f"{module_name}.{component}" not in sys.modules:
            try:
                _is_module_allowed(root_module_name, object.__name__)
                object = importlib.import_module(f".{component}", package=module_name)
                continue
            except ModuleNotFoundError:
                pass

        object = getattr(object, component)

    return object


class RenkuOOBTree(BTree):
    """Customize ``BTrees.OOBTree.BTree`` implementation."""

    max_leaf_size = 1000
    max_internal_size = 2000


class Persistent(persistent.Persistent):
    """Base Persistent class for renku classes.

    Subclasses are assumed to be immutable once persisted to the database. If a class shouldn't be immutable then
    subclass it directly from persistent.Persistent.
    """

    _v_immutable = False

    def reassign_oid(self):
        """Reassign ``oid`` (after assigning a new identifier for example)."""
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


class Database:
    """The Metadata Object Database.

    This class is equivalent to a ``persistent.DataManager`` and implements
    the ``persistent.interfaces.IPersistentDataManager`` interface.
    """

    ROOT_OID = "root"

    def __init__(self, storage):
        self._storage: Storage = storage
        self._cache = Cache()
        # The pre-cache is used by get to avoid infinite loops when objects load their state
        self._pre_cache: Dict[OID_TYPE, persistent.Persistent] = {}
        # Objects added explicitly by add() or when serializing other objects. After commit they are moved to _cache.
        self._objects_to_commit: Dict[OID_TYPE, persistent.Persistent] = {}
        self._reader: ObjectReader = ObjectReader(database=self)
        self._writer: ObjectWriter = ObjectWriter(database=self)
        self._root: Optional[RenkuOOBTree] = None

        self._initialize_root()

    @classmethod
    def from_path(cls, path: Union[Path, str]) -> "Database":
        """Create a Storage and Database using the given path."""
        storage = Storage(path)
        return Database(storage=storage)

    @staticmethod
    def generate_oid(object: persistent.Persistent) -> OID_TYPE:
        """Generate an ``oid`` for a ``persistent.Persistent`` object based on its id."""
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
        """Return ``oid`` from id."""
        return hashlib.sha3_256(id.encode("utf-8")).hexdigest()

    @staticmethod
    def new_oid():
        """Generate a random ``oid``."""
        return f"{uuid4().hex}{uuid4().hex}"

    @staticmethod
    def _get_filename_from_oid(oid: OID_TYPE) -> str:
        return oid.lower()

    def __getitem__(self, key) -> "Index":
        return self._root[key]

    def clear(self):
        """Remove all objects and clear all caches. Objects won't be deleted in the storage."""
        self._cache.clear()
        self._pre_cache.clear()
        self._objects_to_commit.clear()
        # NOTE: Clear root at the end because it will be added to _objects_to_commit when `register` is called.
        self._root.clear()

    def _initialize_root(self):
        """Initialize root object."""
        if not self._root:
            try:
                self._root = self.get(Database.ROOT_OID)
            except errors.ObjectNotFoundError:
                self._root = RenkuOOBTree()
                self._root._p_oid = Database.ROOT_OID
                self.register(self._root)

    def add_index(self, name: str, object_type: type, attribute: str = None, key_type: type = None) -> "Index":
        """Add an index."""
        assert name not in self._root, f"Index or object already exists: '{name}'"

        index = Index(name=name, object_type=object_type, attribute=attribute, key_type=key_type)
        index._p_jar = self

        self._root[name] = index

        return index

    def add_root_object(self, name: str, obj: Persistent):
        """Add an object to the DB root."""
        assert name not in self._root, f"Index or object already exists: '{name}'"

        obj._p_jar = self
        obj._p_oid = name

        self._root[name] = obj

    def add(self, object: persistent.Persistent, oid: OID_TYPE = None):
        """Add a new object to the database.

        NOTE: Normally, we add objects to indexes but this method adds objects directly to Dataset's root. Use it only
        for singleton objects that have no Index defined for them (e.g. Project).
        """
        assert not oid or isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        object._p_oid = oid

        self.register(object)

    def register(self, object: persistent.Persistent):
        """Register a persistent.Persistent object to be stored.

        NOTE: When a persistent.Persistent object is changed it calls this method.
        """
        assert isinstance(object, persistent.Persistent), f"Cannot add non-Persistent object: '{object}'"

        if object._p_oid is None:
            object._p_oid = self.generate_oid(object)
        elif isinstance(object, Persistent):
            # NOTE: A safety-net to make sure that all objects have correct p_oid
            id = getattr(object, "id")
            expected_oid = Database.hash_id(id)
            actual_oid = object._p_oid
            assert actual_oid == expected_oid, f"Object has wrong oid: {actual_oid} != {expected_oid}"

        object._p_jar = self
        # object._p_serial = NEW
        self._objects_to_commit[object._p_oid] = object

    def get(self, oid: OID_TYPE) -> persistent.Persistent:
        """Get the object by ``oid``."""
        if oid != Database.ROOT_OID and oid in self._root:  # NOTE: Avoid looping if getting "root"
            return self._root[oid]
        object = self.get_cached(oid)
        if object is not None:
            return object

        data = self._storage.load(filename=self._get_filename_from_oid(oid))
        object = self._reader.deserialize(data)
        object._p_changed = 0
        object._p_serial = PERSISTED
        if isinstance(object, Persistent):
            object.freeze()

        # NOTE: Avoid infinite loop if object tries to load its state before it is added to the cache
        self._pre_cache[oid] = object
        self._cache[oid] = object
        self._pre_cache.pop(oid)

        return object

    def get_by_id(self, id: str) -> persistent.Persistent:
        """Return an object by its id."""
        oid = Database.hash_id(id)
        return self.get(oid)

    def get_cached(self, oid: OID_TYPE) -> Optional[persistent.Persistent]:
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

    def remove_root_object(self, name: str) -> None:
        """Remove a root object from the database."""
        assert name in self._root, f"Index or object doesn't exist in root: '{name}'"

        obj = self.get(name)
        self.remove_from_cache(obj)

        del self._root[name]

    def new_ghost(self, oid: OID_TYPE, object: persistent.Persistent):
        """Create a new ghost object."""
        object._p_jar = self
        self._cache.new_ghost(oid, object)

    def setstate(self, object: persistent.Persistent):
        """Load the state for a ghost object."""
        data = self._storage.load(filename=self._get_filename_from_oid(object._p_oid))
        self._reader.set_ghost_state(object, data)
        object._p_serial = PERSISTED
        if isinstance(object, Persistent):
            object.freeze()

    def commit(self):
        """Commit modified and new objects."""
        while self._objects_to_commit:
            _, object = self._objects_to_commit.popitem()
            if object._p_changed or object._p_serial == NEW:
                self._store_object(object)

    def _store_object(self, object: persistent.Persistent):
        data = self._writer.serialize(object)
        compress = False if isinstance(object, (Catalog, RenkuOOBTree, OOBucket, Project, Index)) else True
        self._storage.store(filename=self._get_filename_from_oid(object._p_oid), data=data, compress=compress)

        self._cache[object._p_oid] = object

        object._p_changed = 0  # NOTE: transition from changed to up-to-date
        object._p_serial = PERSISTED

    def remove_from_cache(self, object: persistent.Persistent):
        """Remove an object from cache."""
        oid = object._p_oid

        def remove_from(cache):
            existing_entry = cache.get(oid)
            if existing_entry is object:
                cache.pop(oid)

        remove_from(self._cache)
        remove_from(self._pre_cache)
        remove_from(self._objects_to_commit)

    def readCurrent(self, object):
        """We don't use this method but some Persistent logic require its existence."""
        assert object._p_jar is self
        assert object._p_oid is not None

    def oldstate(self, object, tid):
        """See ``persistent.interfaces.IPersistentDataManager::oldstate``."""
        raise NotImplementedError


@implementer(IPickleCache)
class Cache:
    """Database ``Cache``."""

    def __init__(self):
        self._entries = {}

    def __len__(self):
        return len(self._entries)

    def __getitem__(self, oid):
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        return self._entries[oid]

    def __setitem__(self, oid, object):
        assert isinstance(object, persistent.Persistent), f"Cannot cache non-Persistent objects: '{object}'"
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"

        assert object._p_jar is not None, "Cached object jar missing"
        assert oid == object._p_oid, f"Cache key does not match oid: {oid} != {object._p_oid}"

        if oid in self._entries:
            existing_data = self.get(oid)
            if existing_data is not object:
                raise ValueError(f"The same oid exists: {existing_data} != {object}")

        self._entries[oid] = object

    def __delitem__(self, oid):
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        self._entries.pop(oid)

    def clear(self):
        """Remove all entries."""
        self._entries.clear()

    def pop(self, oid, default=MARKER):
        """Remove and return an object."""
        return self._entries.pop(oid) if default is MARKER else self._entries.pop(oid, default)

    def get(self, oid, default=None):
        """See ``IPickleCache``."""
        assert isinstance(oid, OID_TYPE), f"Invalid oid type: '{type(oid)}'"
        return self._entries.get(oid, default)

    def new_ghost(self, oid, object):
        """See ``IPickleCache``."""
        assert object._p_oid is None, f"Object already has an oid: {object}"
        assert object._p_jar is not None, f"Object does not have a jar: {object}"
        assert oid not in self._entries, f"Duplicate oid: {oid}"

        object._p_oid = oid
        if object._p_state != GHOST:
            object._p_invalidate()

        self[oid] = object


class Index(persistent.Persistent):
    """Database index."""

    def __init__(self, *, name: str, object_type, attribute: Optional[str], key_type=None):
        """
        Create an index where keys are extracted using `attribute` from an object or a key.

        :param name: Index's name
        :param object_type: Type of objects that the index points to
        :param attribute: Name of an attribute to be used to automatically generate a key (e.g. `entity.path`)
        :param key_type: Type of keys. If not None then a key must be provided when updating the index
        """
        assert name == name.lower(), f"Index name must be all lowercase: '{name}'."

        super().__init__()

        self._p_oid = f"{name}-index"
        self._name: str = name
        self._object_type = object_type
        self._key_type = key_type
        self._attribute: Optional[str] = attribute
        self._entries: RenkuOOBTree = RenkuOOBTree()
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
        if not key:
            return
        return self._entries.pop(key) if default is MARKER else self._entries.pop(key, default)

    def keys(self, min=None, max=None, excludemin=False, excludemax=False):
        """Return an iterator of keys."""
        return self._entries.keys(min=min, max=max, excludemin=excludemin, excludemax=excludemax)

    def values(self):
        """Return an iterator of values."""
        return self._entries.values()

    def items(self):
        """Return an iterator of keys and values."""
        return self._entries.items()

    def add(self, object: persistent.Persistent, *, key: Optional[str] = None, key_object=None, verify=True):
        """Update index with object.

        If `Index._attribute` is not None then key is automatically generated.
        Key is extracted from `key_object` if it is not None; otherwise, it's extracted from `object`.
        """
        assert isinstance(object, self._object_type), f"Cannot add objects of type '{type(object)}'"
        key = self._verify_and_get_key(
            object=object, key_object=key_object, key=key, missing_key_object_ok=False, verify=verify
        )
        self._entries[key] = object

    def generate_key(self, object: persistent.Persistent, *, key_object=None):
        """Return index key for an object.

        Key is extracted from `key_object` if it is not None; otherwise, it's extracted from `object`.
        """
        return self._verify_and_get_key(object=object, key_object=key_object, key=None, missing_key_object_ok=False)

    def _verify_and_get_key(
        self, *, object: persistent.Persistent, key_object, key, missing_key_object_ok, verify=True
    ):
        if self._key_type:
            if not missing_key_object_ok:
                assert isinstance(key_object, self._key_type), f"Invalid key type: {type(key_object)} for '{self.name}'"
        else:
            assert key_object is None, f"Index '{self.name}' does not accept 'key_object'"

        if self._attribute:
            key_object = key_object or object
            correct_key = get_attribute(key_object, self._attribute)
            if key is not None:
                if verify:
                    assert key == correct_key, f"Incorrect key for index '{self.name}': '{key}' != '{correct_key}'"
                else:
                    correct_key = key
        else:
            assert key is not None, "No key is provided"
            correct_key = key

        return correct_key


class Storage:
    """Store Persistent objects on the disk."""

    OID_FILENAME_LENGTH = 64

    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.zstd_compressor = zstd.ZstdCompressor()
        self.zstd_decompressor = zstd.ZstdDecompressor()

    def store(self, filename: str, data: Union[Dict, List], compress=False):
        """Store object."""
        assert isinstance(filename, str)

        is_oid_path = len(filename) == Storage.OID_FILENAME_LENGTH
        if is_oid_path:
            path = self.path / filename[0:2] / filename[2:4] / filename
            path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path = self.path / filename
            self.path.mkdir(parents=True, exist_ok=True)

        if compress:
            with open(path, "wb") as f, self.zstd_compressor.stream_writer(f) as compressor:
                with io.TextIOWrapper(compressor) as out:
                    json.dump(data, out, ensure_ascii=False)
        else:
            with open(path, "wt") as f:
                json.dump(data, f, ensure_ascii=False, sort_keys=True, indent=2)

    def load(self, filename: str):
        """Load data for object with object id oid."""
        assert isinstance(filename, str)

        is_oid_path = len(filename) == Storage.OID_FILENAME_LENGTH
        if is_oid_path:
            path = self.path / filename[0:2] / filename[2:4] / filename
        else:
            path = self.path / filename

        if not path.exists():
            raise errors.ObjectNotFoundError(filename)

        with open(path, "rb") as file:
            header = int.from_bytes(file.read(4), "little")
            file.seek(0)
            if header == zstd.MAGIC_NUMBER:
                with self.zstd_decompressor.stream_reader(file) as zfile:
                    data = json.load(zfile)
            else:
                data = json.load(file)
        return data


class ObjectWriter:
    """Serialize objects for storage in storage."""

    def __init__(self, database: Database):
        self._database: Database = database

    def serialize(self, object: persistent.Persistent):
        """Convert an object to JSON."""
        assert isinstance(object, persistent.Persistent), f"Cannot serialize object of type '{type(object)}': {object}"
        assert object._p_oid, f"Object does not have an oid: '{object}'"
        assert object._p_jar is not None, f"Object is not associated with a Database: '{object}'"

        self._serialization_cache = {}
        state = object.__getstate__()
        was_dict = isinstance(state, dict)
        data = self._serialize_helper(state)
        is_dict = isinstance(data, dict)

        if not is_dict or (is_dict and not was_dict):
            data = {"@renku_data_value": data}

        data["@renku_data_type"] = get_type_name(object)
        data["@renku_oid"] = object._p_oid

        return data

    def _serialize_helper(self, object):
        # TODO: Raise an error if an unsupported object is being serialized
        if object is None:
            return None
        elif isinstance(object, (int, float, str, bool)):
            return object
        elif isinstance(object, list):
            return [self._serialize_helper(value) for value in object]
        elif isinstance(object, set):
            return {
                "@renku_data_type": SET_TYPE,
                "@renku_data_value": [self._serialize_helper(value) for value in object],
            }
        elif isinstance(object, frozenset):
            return {
                "@renku_data_type": FROZEN_SET_TYPE,
                "@renku_data_value": [self._serialize_helper(value) for value in object],
            }
        elif isinstance(object, dict):
            result = dict()
            items = sorted(object.items(), key=lambda x: x[0])
            for key, value in items:
                result[key] = self._serialize_helper(value)
            return result
        elif isinstance(object, Index):
            # NOTE: Index objects are not stored as references and are included in their parent object (i.e. root)
            state = object.__getstate__()
            state = self._serialize_helper(state)
            return {"@renku_data_type": get_type_name(object), "@renku_oid": object._p_oid, **state}
        elif isinstance(object, (OOTreeSet, Length, OOSet)):
            state = object.__getstate__()
            state = self._serialize_helper(state)
            return {"@renku_data_type": get_type_name(object), "@renku_data_value": state}
        elif isinstance(object, persistent.Persistent):
            if not object._p_oid:
                object._p_oid = Database.generate_oid(object)
            if object._p_state not in [GHOST, UPTODATE] or (object._p_state == UPTODATE and object._p_serial == NEW):
                self._database.register(object)
            return {"@renku_data_type": get_type_name(object), "@renku_oid": object._p_oid, "@renku_reference": True}
        elif isinstance(object, datetime.datetime):
            value = object.isoformat()
        elif isinstance(object, tuple):
            value = tuple(self._serialize_helper(value) for value in object)
        elif isinstance(object, (InterfaceClass)):
            # NOTE: Zope interfaces are weird, they're a class with type InterfaceClass, but need to be deserialized
            # as the class (without instantiation)
            return {"@renku_data_type": TYPE_TYPE, "@renku_data_value": f"{object.__module__}.{object.__name__}"}
        elif isinstance(object, type):
            # NOTE: We're storing a type, not an instance
            return {"@renku_data_type": TYPE_TYPE, "@renku_data_value": get_type_name(object)}
        elif isinstance(object, (FunctionType, BuiltinFunctionType)):
            name = object.__name__
            module = getattr(object, "__module__", None)
            return {"@renku_data_type": FUNCTION_TYPE, "@renku_data_value": f"{module}.{name}"}
        elif hasattr(object, "__getstate__"):
            if id(object) in self._serialization_cache:
                # NOTE: We already serialized this -> circular/repeat reference.
                return {"@renku_data_type": REFERENCE_TYPE, "@renku_data_value": self._serialization_cache[id(object)]}

            # NOTE: The reference used for circular reference is just the position in the serialization cache,
            # as the order is deterministic. So the order in which objects are encoutered is their id for referencing.
            self._serialization_cache[id(object)] = len(self._serialization_cache)

            value = object.__getstate__().copy()
            value = {k: v for k, v in value.items() if not k.startswith("_v_")}
            value = self._serialize_helper(value)
            assert not isinstance(value, dict) or "id" in value, f"Invalid object state: {value} for {object}"
        else:
            if id(object) in self._serialization_cache:
                # NOTE: We already serialized this -> circular/repeat reference
                return {"@renku_data_type": REFERENCE_TYPE, "@renku_data_value": self._serialization_cache[id(object)]}

            # NOTE: The reference used for circular reference is just the position in the serialization cache,
            # as the order is deterministic So the order in which objects are encoutered is their id for referencing.
            self._serialization_cache[id(object)] = len(self._serialization_cache)

            value = object.__dict__.copy()
            value = {k: v for k, v in value.items() if not k.startswith("_v_")}
            value = self._serialize_helper(value)

        return {"@renku_data_type": get_type_name(object), "@renku_data_value": value}


class ObjectReader:
    """Deserialize objects loaded from storage."""

    def __init__(self, database: Database):
        self._classes: Dict[str, type] = {}
        self._database = database

        # a cache for normal (non-persistent objects with an id) to deduplicate them on load
        self._normal_object_cache = {}

    def _get_class(self, type_name: str) -> type:
        cls = self._classes.get(type_name)
        if cls:
            return cls

        cls = get_class(type_name)

        self._classes[type_name] = cls
        return cls

    def set_ghost_state(self, object: persistent.Persistent, data: Dict):
        """Set state of a Persistent ghost object."""
        previous_cache = self._deserialization_cache
        self._deserialization_cache = []

        state = self._deserialize_helper(data, create=False)
        object.__setstate__(state)

        self._deserialization_cache = previous_cache

    def deserialize(self, data):
        """Convert JSON to Persistent object."""
        oid = data["@renku_oid"]

        self._deserialization_cache = []

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
            assert isinstance(data, dict), f"Data must be a dict: '{type(data)}'"

            if "@renku_data_type" not in data:  # NOTE: A normal dict value
                assert "@renku_oid" not in data
                items = sorted(data.items(), key=lambda x: x[0])
                for key, value in items:
                    data[key] = self._deserialize_helper(value)
                return data

            object_type = data.pop("@renku_data_type")
            if object_type in (TYPE_TYPE, FUNCTION_TYPE):
                # NOTE: if we stored a type (not instance), return the type
                return self._get_class(data["@renku_data_value"])
            elif object_type == REFERENCE_TYPE:
                # NOTE: we had a circular reference, we return the (not yet finalized) class here
                return self._deserialization_cache[data["@renku_data_value"]]
            elif object_type == SET_TYPE:
                return set([self._deserialize_helper(value) for value in data["@renku_data_value"]])
            elif object_type == FROZEN_SET_TYPE:
                return frozenset([self._deserialize_helper(value) for value in data["@renku_data_value"]])

            cls = self._get_class(object_type)

            if issubclass(cls, datetime.datetime):
                assert create
                data = data["@renku_data_value"]
                return datetime.datetime.fromisoformat(data)
            elif issubclass(cls, tuple):
                data = data["@renku_data_value"]
                return tuple(self._deserialize_helper(value) for value in data)

            oid: str = data.pop("@renku_oid", None)
            if oid:
                assert isinstance(oid, str)

                if "@renku_reference" in data and data["@renku_reference"]:  # A reference
                    assert create, f"Cannot deserialize a reference without creating an instance {data}"
                    new_object = self._database.get_cached(oid)
                    if new_object is not None:
                        return new_object
                    assert issubclass(cls, persistent.Persistent)
                    new_object = cls.__new__(cls)
                    self._database.new_ghost(oid, new_object)
                    return new_object
                elif issubclass(cls, Index):
                    new_object = self._database.get_cached(oid)
                    if new_object:
                        return new_object
                    new_object = cls.__new__(cls)
                    new_object._p_oid = oid
                    self.set_ghost_state(new_object, data)
                    return new_object

            if "@renku_data_value" in data:
                data = data["@renku_data_value"]

            if not create:
                data = self._deserialize_helper(data)
                return data

            if issubclass(cls, persistent.Persistent):
                new_object = cls.__new__(cls)
                new_object._p_oid = oid
                self.set_ghost_state(new_object, data)
            else:
                new_object = cls.__new__(cls)

                # NOTE: we deserialize in the same order as we serialized, so the two stacks here match
                self._deserialization_cache.append(new_object)

                data = self._deserialize_helper(data)
                assert isinstance(data, dict)

                if "id" in data and data["id"] in self._normal_object_cache:
                    return self._normal_object_cache[data["id"]]

                for name, value in data.items():
                    object.__setattr__(new_object, name, value)

                if issubclass(cls, Immutable):
                    new_object = cls.make_instance(new_object)

                if "id" in data and isinstance(data["id"], str) and data["id"].startswith("/"):
                    self._normal_object_cache[data["id"]] = new_object

            return new_object
