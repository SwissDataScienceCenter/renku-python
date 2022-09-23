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

import contextlib
import copy
import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, Tuple

import pytest

from renku.core import errors
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.infrastructure.database import Database
    from renku.infrastructure.repository import Repository


class DummyStorage:
    """An in-memory storage class."""

    def __init__(self):
        self._files = {}
        self._modification_dates = {}

    def store(self, filename: str, data, compress: bool = False, absolute: bool = False):
        """Store object."""
        assert isinstance(filename, str)

        self._files[filename] = data
        self._modification_dates[filename] = datetime.datetime.now()

    def load(self, filename: str, absolute: bool = False):
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


@pytest.fixture
def database() -> Iterator[Tuple["Database", DummyStorage]]:
    """A Database with in-memory storage."""
    from renku.infrastructure.database import Database
    from renku.infrastructure.gateway.database_gateway import initialize_database

    storage = DummyStorage()
    database = Database(storage=storage)

    initialize_database(database)

    yield database, storage


def get_test_bindings() -> Dict[str, Any]:
    """Return all possible bindings."""
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

    constructor_bindings = {
        IPlanGateway: lambda: PlanGateway(),
        IActivityGateway: lambda: ActivityGateway(),
        IDatabaseGateway: lambda: DatabaseGateway(),
        IDatasetGateway: lambda: DatasetGateway(),
        IProjectGateway: lambda: ProjectGateway(),
    }

    return {"bindings": {}, "constructor_bindings": constructor_bindings}


def add_client_binding(bindings: Dict[str, Any]) -> Dict[str, Any]:
    """Add required client bindings."""
    from renku.command.command_builder.client_dispatcher import ClientDispatcher
    from renku.core.interface.client_dispatcher import IClientDispatcher
    from renku.core.management.client import LocalClient

    client = LocalClient()

    client_dispatcher = ClientDispatcher()
    client_dispatcher.push_created_client_to_stack(client)
    bindings["bindings"].update({"LocalClient": client, IClientDispatcher: client_dispatcher})

    return bindings


@pytest.fixture
def with_injections_manager() -> Callable[["Repository"], None]:
    """Factory fixture for test injections manager."""
    from renku.command.command_builder.command import inject, remove_injector

    @contextlib.contextmanager
    def with_injection(bindings, path: Path):
        """Context manager to temporarily do injections."""

        def _bind(binder):
            for key, value in bindings["bindings"].items():
                binder.bind(key, value)
            for key, value in bindings["constructor_bindings"].items():
                binder.bind_to_constructor(key, value)

            return binder

        inject.configure(_bind, bind_in_runtime=False)

        with project_context.with_path(path, save_changes=True):
            try:
                yield
            finally:
                remove_injector()

    def test_injection_manager_helper(repository: "Repository"):
        bindings = get_test_bindings()
        add_client_binding(bindings=bindings)
        return with_injection(bindings=bindings, path=repository.path)

    return test_injection_manager_helper


@pytest.fixture
def project_with_injection(repository, with_injections_manager):
    """Fixture for context manager with project and database injection."""
    with with_injections_manager(repository):
        yield repository


client_database_injection_manager = with_injections_manager
