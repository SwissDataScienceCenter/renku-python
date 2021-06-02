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
from pathlib import Path
from typing import Any, Optional, Union

import BTrees.OOBTree
import transaction
from ZODB import DB, FileStorage
from ZODB.Connection import Connection
from ZODB.interfaces import IStorage


class MetadataStorage(DB):
    """The Metadata Object Database.

    The database provides a few methods intended for application code
    -- open, close, undo, and pack -- and a large collection of
    methods for inspecting the database and its connections' caches.
    """

    def __init__(self, storage: IStorage):
        super().__init__(storage=storage)
        # self._storage: IStorage = storage
        self._connection: Optional[Connection] = None

        # TODO: delete these
        self.large_record_size = 100000000
        self.dataset_name = "Renku"

    def open(self, transaction_manager=None, at=None, before=None):
        """Return a database Connection for use by application code."""
        self._connection = super().open(transaction_manager=transaction_manager, at=at, before=before)

        return self._connection
        # if self._connection is not None:
        #     raise RuntimeError("Trying to reopen an open connection.")
        #
        # self._connection = Connection(db=self)
        # self._connection.open()
        #
        # return self._connection

    def close(self):
        """Close a connection."""
        if self._connection is None:
            raise RuntimeError("Trying to close a closed connection.")

        self._connection.close()
        self._connection = None

        self._storage.close()  # TODO is this needed

    def get_object_by_id(self, type: str, id: str) -> Any:
        """Return an object with specific type and id."""
        root = self._connection.root()
        if type in root:
            objects = root[type]
            if id in objects:
                return objects[id]

    def store_object(self, type: str, id: str, object: Any):
        """Store an object."""
        root = self._connection.root()
        if type not in root:
            root[type] = BTrees.OOBTree.BTree()
        objects = root[type]
        objects[id] = object
        # TODO commit transaction properly
        transaction.commit()

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> "MetadataStorage":
        """Create a FileStorage and return a storage.

        :param str path: Path to the parent directory of storage file
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        path = path / "store.fs"
        storage = FileStorage.FileStorage(str(path))

        return cls(storage)

    def commit(self):
        """Save all changes to the underlying storage."""
        transaction.commit()
