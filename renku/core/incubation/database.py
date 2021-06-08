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
from typing import Dict, List, Union
from uuid import uuid4

import BTrees.OOBTree
from persistent import GHOST, UPTODATE, Persistent, PickleCache
from persistent.interfaces import IPersistentDataManager
from ZODB.interfaces import IConnection
from ZODB.POSException import POSKeyError
from ZODB.utils import z64
from zope.interface import implementer

from renku.core import errors

MARKER = object()


# NOTE These are used as _p_serial to mark if an object was read from storage or is new
NEW = z64  # NOTE: Do not change this value since this is the default when a Persistent object is created
PERSISTED = b"1" * 8


@implementer(IConnection, IPersistentDataManager)
class Database:
    """The Metadata Object Database."""

    def __init__(self, path: Union[str, Path], root_object_types: List[str] = ("Activity", "Entity", "Plan")):
        self._storage: Storage = Storage(path)
        self._root_object_types: List[str] = list(root_object_types)
        self._cache = PickleCache(self)
        # The pre-cache is used by get to avoid infinite loops when objects load their state
        self._pre_cache: Dict[bytes, Persistent] = {}
        # List of all objects registered as modified by the persistence machinery, or by add(). All objects of this list
        # are either in _cache or in _added.
        self._registered_objects: List[Persistent] = []
        # Objects added explicitly through add(). When committing they are moved to _cache.
        self._added: Dict[bytes, Persistent] = {}
        self._reader = ObjectReader(cache=self._cache, database=self)

    @property
    def root(self):
        """Return the database root object."""
        try:
            return self.get(b"root")
        except POSKeyError:
            root = BTrees.OOBTree.BTree()

            for root_object_type in self._root_object_types:
                self._create_root_object_type(root, root_object_type)

            self.add(root, b"root")
            root._p_changed = 1
            return root

    @staticmethod
    def _create_root_object_type(root, root_object_type):
        if root_object_type in root:
            return

        object = BTrees.OOBTree.BTree()
        object._p_oid = root_object_type.encode("ascii")
        root[root_object_type] = object

    @staticmethod
    def new_oid():
        """Generate a random oid."""
        return f"{uuid4().hex}{uuid4().hex}".encode("ascii")

    @staticmethod
    def generate_oid(object) -> bytes:
        """Generate oid for a Persistent object based on its id."""
        oid = getattr(object, "_p_oid", None)
        if oid:
            assert isinstance(oid, bytes)
            return oid

        id: str = getattr(object, "id", None) or getattr(object, "_id", None)
        if not id:
            return Database.new_oid()

        return hashlib.sha3_256(id.encode("utf-8")).hexdigest().encode("ascii")

    def add(self, object: Persistent, oid: bytes = None):
        """Add a new object to the database and assign an oid to it."""
        p_oid = getattr(object, "_p_oid", MARKER)
        if p_oid is MARKER:
            raise TypeError("Only first-class persistent objects may be added to database.", object)

        oid = oid or object._p_oid

        if object._p_jar is None:
            object._p_jar = self
            if oid is None:
                oid = self.generate_oid(object)
            object._p_oid = oid
            object._p_serial = NEW
            self._added[oid] = object
            self.register(object)
        elif object._p_jar is not self:
            raise errors.ObjectStateError(f"Object '{object._p_oid}' already has a Database: '{object._p_jar}'")

    def get(self, oid: Union[bytes, str]) -> Persistent:
        """Get the object by oid."""
        if oid in self._root_object_types:
            return self.root[oid]

        if isinstance(oid, str):
            oid = oid.encode("utf-8")

        object = self._cache.get(oid)
        if object is not None:
            return object
        object = self._added.get(oid)
        if object is not None:
            return object
        object = self._pre_cache.get(oid)
        if object is not None:
            return object

        data = self._storage.load(oid)
        object = self._reader.deserialize(data)
        object._p_changed = 0

        # NOTE: Avoid infinite loop if object tries to load its state before it is added to the cache
        self._pre_cache[oid] = object
        self._cache[oid] = object
        self._pre_cache.pop(oid)

        return object

    def commit(self):
        """Commit modified and new objects."""
        for object in self._registered_objects:
            oid = object._p_oid
            assert oid

            if object._p_jar is not self:
                raise errors.ObjectStateError(f"Object '{object}' doesn't belong to DataManager.")
            elif not object._p_changed and object._p_serial != NEW:
                continue

            object_type = type(object).__name__
            if object_type in self._root_object_types:
                self._create_root_object_type(self.root, object_type)
                oid = oid.decode("ascii")
                self.root[object_type][oid] = object

            self._store_object(object)

    def _store_object(self, object: Persistent):
        writer = ObjectWriter(object)

        for object in writer:
            oid = object._p_oid

            object._p_jar = self
            data = writer.serialize(object)

            self._storage.store(oid=oid, data=data)
            self._cache[oid] = object

            self._cache.update_object_size_estimation(oid, 1)
            object._p_estimated_size = 1

            object._p_changed = 0  # NOTE: transition from changed to up-to-date
            object._p_serial = PERSISTED

    def oldstate(self, object, tid):
        """See persistent.interfaces.IPersistentDataManager::oldstate."""
        raise NotImplementedError

    def setstate(self, object):
        """Load the state for a ghost object."""
        oid = object._p_oid

        data = self._storage.load(oid)
        self._reader.set_ghost_state(object, data)

        object._p_serial = PERSISTED

    def register(self, object):
        """See persistent.interfaces.IPersistentDataManager::register."""
        self._registered_objects.append(object)

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> "Database":
        """Return a Database."""
        return cls(path)

    def readCurrent(self, object):
        """We don't use this method but some Persistent logic require its existence."""
        assert object._p_jar is self
        assert object._p_oid is not None


class Storage:
    """Store Persistent objects on the disk."""

    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def store(self, oid: bytes, data: Union[Dict, List]):
        """Store data for object with identifier oid."""
        if not isinstance(oid, str):
            oid = str(oid, "ascii")

        filename = self.path / oid
        with open(filename, "w") as file:
            json.dump(data, file, ensure_ascii=False, sort_keys=True, indent=2)

    def load(self, oid: bytes):
        """Load data for object with object id oid."""
        if not isinstance(oid, str):
            oid = str(oid, "ascii")
        filename = self.path / oid
        if not filename.exists():
            raise POSKeyError(oid)

        with open(filename) as file:
            data = json.load(file)

        return data


class ObjectWriter:
    """Serialize objects for storage in storage."""

    def __init__(self, object: Persistent):
        assert object._p_oid, f"Object does not have an oid: '{object}'"
        assert object._p_jar is not None, f"Object is not associated with a DataManager: '{object._p_oid}'"
        self._stack = [object]

    def serialize(self, object):
        """Convert an object to JSON."""
        if not isinstance(object, Persistent):
            raise TypeError(f"Cannot serialize object of type '{type(object)}': {object}")

        if not object._p_oid:
            object._p_oid = Database.generate_oid(object)

        state = object.__getstate__()
        data = self._serialize_helper(state)

        if not isinstance(data, dict):
            data = {"@value": data}

        type_name = type(object).__name__
        data["@type"] = type_name
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
            return {"@type": "datetime", "@value": object.isoformat()}
        elif isinstance(object, Persistent):
            if not object._p_oid:
                object._p_oid = Database.generate_oid(object)
            if object._p_state not in [GHOST, UPTODATE] or (object._p_state == UPTODATE and object._p_serial == NEW):
                self._stack.append(object)
            type_name = type(object).__name__
            return {"@type": type_name, "@oid": object._p_oid.decode("ascii"), "@reference": True}
        elif hasattr(object, "__getstate__"):
            state = object.__getstate__()
            return self._serialize_helper(state)
        else:
            state = object.__dict__.copy()
            state = self._serialize_helper(state)

            type_name = type(object).__name__
            state["@type"] = type_name

            return state

    def __iter__(self):
        return self

    def __next__(self):
        if not self._stack:
            raise StopIteration
        else:
            return self._stack.pop()


class ObjectReader:
    """Deserialize objects loaded from storage."""

    def __init__(self, cache, database: Database):
        self._cache = cache
        self._classes: Dict[str, type] = ObjectReader._load_classes()
        self._database = database

    def _get_class(self, name):
        cls = self._classes.get(name)
        if not cls:
            raise TypeError(f"Class '{name}' not registered.")

        return cls

    @staticmethod
    def _load_classes() -> Dict[str, type]:
        from datetime import datetime

        from BTrees.OOBTree import OOBTree
        from persistent.mapping import PersistentMapping

        from renku.core.models.cwl.annotation import Annotation
        from renku.core.models.entity import Collection, Entity
        from renku.core.models.provenance.activity import Activity, ActivityCollection, Association, Generation, Usage
        from renku.core.models.provenance.agents import Person, SoftwareAgent
        from renku.core.models.provenance.parameter import PathParameterValue, VariableParameterValue
        from renku.core.models.provenance.provenance_graph import ProvenanceGraph
        from renku.core.models.workflow.dependency_graph import DependencyGraph
        from renku.core.models.workflow.parameter import CommandInput, CommandOutput, CommandParameter, MappedIOStream
        from renku.core.models.workflow.plan import Plan

        return {
            m.__name__: m
            for m in [
                Activity,
                ActivityCollection,
                Annotation,
                Association,
                Collection,
                CommandInput,
                CommandOutput,
                CommandParameter,
                CommandParameter,
                DependencyGraph,
                Entity,
                Generation,
                MappedIOStream,
                OOBTree,
                PathParameterValue,
                PersistentMapping,
                Person,
                Plan,
                ProvenanceGraph,
                SoftwareAgent,
                Usage,
                VariableParameterValue,
                datetime,
            ]
        }

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

            if object_type == "datetime":
                assert create
                value = data["@value"]
                return datetime.datetime.fromisoformat(value)

            cls = self._get_class(object_type)

            oid: str = data.pop("@oid", None)
            if oid:
                assert isinstance(oid, str)
                oid: bytes = oid.encode("ascii")

                if "@reference" in data and data["@reference"]:  # A reference
                    object = self._cache.get(oid)
                    if object:
                        return object
                    object = cls.__new__(cls)
                    self._cache.new_ghost(oid, object)
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
                if "_id" in data:
                    data["id"] = data.pop("_id")
                object = cls(**data)

            return object
