# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku fixtures for metadata Database."""

import datetime

import pytest
from ZODB.POSException import POSKeyError

from renku.core.incubation.database import Database


class DummyStorage:
    """An in-memory storage class."""

    def __init__(self):
        self._files = {}
        self._modification_dates = {}

    def store(self, filename: str, data):
        """Store object."""
        assert isinstance(filename, str)

        self._files[filename] = data
        self._modification_dates[filename] = datetime.datetime.now()

    def load(self, filename: str):
        """Load data for object with object id oid."""
        assert isinstance(filename, str)

        if filename not in self._files:
            raise POSKeyError(filename)

        return self._files[filename]

    def get_modification_date(self, filename: str):
        """Return modification date of a file."""
        return self._modification_dates[filename]

    def exists(self, filename: str):
        """Return True if filename exists in the storage."""
        return filename in self._files


@pytest.fixture
def database():
    """A Database with in-memory storage."""
    from renku.core.models.provenance.activity import Activity
    from renku.core.models.workflow.plan import Plan

    storage = DummyStorage()
    database = Database(storage=storage)

    database.add_index(name="activities", value_type=Activity, attribute="id")
    database.add_index(name="plans", value_type=Plan, attribute="id")

    yield database, storage
