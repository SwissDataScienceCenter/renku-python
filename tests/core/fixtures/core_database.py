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

import copy
import datetime
from pathlib import Path
from typing import Iterator, Tuple, Union

import pytest

from renku.core import errors
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.infrastructure.database import Database
from renku.infrastructure.gateway.database_gateway import initialize_database


class DummyStorage:
    """An in-memory storage class."""

    def __init__(self):
        self._files = {}
        self._modification_dates = {}

    def store(self, filename: str, data, compress=False, absolute=False):
        """Store object."""
        assert isinstance(filename, str)

        self._files[filename] = data
        self._modification_dates[filename] = datetime.datetime.now()

    def load(self, filename: str, absolute=False):
        """Load data for object with object id oid."""
        assert isinstance(filename, str)

        if filename not in self._files:
            raise errors.ObjectNotFoundError(filename)

        return copy.deepcopy(self._files[filename])

    def get_modification_date(self, filename: str):
        """Return modification date of a file."""
        return self._modification_dates[filename]

    def exists(self, filename: str):
        """Return True if filename exists in the storage."""
        return filename in self._files


class DummyDatabaseDispatcher:
    """DatabaseDispatcher with DummyStorage.

    Handles getting current database (Database) and entering/exiting the stack for the database.
    """

    def __init__(self, database: Database):
        self.database = database

    @property
    def current_database(self) -> Database:
        """Get the currently active database."""
        return self.database

    def push_database_to_stack(self, path: Union[Path, str], commit: bool = False) -> None:
        """Create and push a new database to the stack."""
        pass

    def pop_database(self) -> None:
        """Remove the current database from the stack."""
        pass

    def finalize_dispatcher(self) -> None:
        """Close all database contexts."""
        pass


@pytest.fixture
def database() -> Iterator[Tuple[Database, DummyStorage]]:
    """A Database with in-memory storage."""
    storage = DummyStorage()
    database = Database(storage=storage)

    initialize_database(database)

    yield database, storage


@pytest.fixture
def database_injection_bindings():
    """Create injection bindings for a database."""

    def _add_database_injection_bindings(bindings):
        from renku.command.command_builder.database_dispatcher import DatabaseDispatcher
        from renku.core.interface.activity_gateway import IActivityGateway
        from renku.core.interface.client_dispatcher import IClientDispatcher
        from renku.core.interface.database_dispatcher import IDatabaseDispatcher
        from renku.core.interface.database_gateway import IDatabaseGateway
        from renku.core.interface.dataset_gateway import IDatasetGateway
        from renku.core.interface.plan_gateway import IPlanGateway
        from renku.core.interface.project_gateway import IProjectGateway
        from renku.infrastructure.gateway.activity_gateway import ActivityGateway
        from renku.infrastructure.gateway.database_gateway import DatabaseGateway
        from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
        from renku.infrastructure.gateway.plan_gateway import PlanGateway
        from renku.infrastructure.gateway.project_gateway import ProjectGateway

        dispatcher = DatabaseDispatcher()
        dispatcher.push_database_to_stack(
            bindings["bindings"][IClientDispatcher].current_client.database_path, commit=True
        )

        bindings["bindings"][IDatabaseDispatcher] = dispatcher

        bindings["constructor_bindings"][IPlanGateway] = lambda: PlanGateway()
        bindings["constructor_bindings"][IActivityGateway] = lambda: ActivityGateway()
        bindings["constructor_bindings"][IDatabaseGateway] = lambda: DatabaseGateway()
        bindings["constructor_bindings"][IDatasetGateway] = lambda: DatasetGateway()
        bindings["constructor_bindings"][IProjectGateway] = lambda: ProjectGateway()

        return bindings

    return _add_database_injection_bindings


@pytest.fixture
def dummy_database_injection_bindings(database):
    """Create injection bindings for a database."""

    def _add_database_injection_bindings(bindings):
        from renku.core.interface.activity_gateway import IActivityGateway
        from renku.core.interface.database_gateway import IDatabaseGateway
        from renku.core.interface.dataset_gateway import IDatasetGateway
        from renku.core.interface.plan_gateway import IPlanGateway
        from renku.core.interface.project_gateway import IProjectGateway
        from renku.infrastructure.gateway.activity_gateway import ActivityGateway
        from renku.infrastructure.gateway.database_gateway import DatabaseGateway
        from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
        from renku.infrastructure.gateway.plan_gateway import PlanGateway
        from renku.infrastructure.gateway.project_gateway import ProjectGateway

        bindings["bindings"][IDatabaseDispatcher] = DummyDatabaseDispatcher(database[0])

        bindings["constructor_bindings"][IPlanGateway] = lambda: PlanGateway()
        bindings["constructor_bindings"][IActivityGateway] = lambda: ActivityGateway()
        bindings["constructor_bindings"][IDatabaseGateway] = lambda: DatabaseGateway()
        bindings["constructor_bindings"][IDatasetGateway] = lambda: DatasetGateway()
        bindings["constructor_bindings"][IProjectGateway] = lambda: ProjectGateway()

        return bindings

    return _add_database_injection_bindings


@pytest.fixture
def injection_manager():
    """Factory fixture for injection manager."""

    def _injection_manager(bindings):
        from tests.utils import injection_manager

        return injection_manager(bindings)

    return _injection_manager


@pytest.fixture
def client_database_injection_manager(client_injection_bindings, database_injection_bindings, injection_manager):
    """Fixture for context manager with client and db injection."""

    def _inner(client):
        return injection_manager(database_injection_bindings(client_injection_bindings(client)))

    return _inner


@pytest.fixture
def dummy_database_injection_manager(dummy_database_injection_bindings, injection_manager):
    """Fixture for context manager with client and db injection."""

    def _inner(client):
        return injection_manager(dummy_database_injection_bindings({"bindings": {}, "constructor_bindings": {}}))

    return _inner


@pytest.fixture
def injected_client(client, client_database_injection_manager):
    """Fixture for context manager with client and database injection."""
    with client_database_injection_manager(client):
        yield client


@pytest.fixture
def injected_dummy_database(dummy_database_injection_manager):
    """Fixture for context manager with dummy database injection."""
    with dummy_database_injection_manager(None):
        yield


@pytest.fixture
def path_injection(injection_manager, database_injection_bindings, client_injection_bindings):
    """Fixture that accepts a path and adds required renku binding."""

    def create_client_and_bindings(path: Union[Path, str]):
        from renku.core.management.client import LocalClient
        from renku.core.util.contexts import chdir

        with chdir(path):
            client = LocalClient(path=path)
            return injection_manager(database_injection_bindings(client_injection_bindings(client)))

    return create_client_and_bindings
