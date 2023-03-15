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
from typing import Generator, Iterator, Optional, Tuple

import pytest

from renku.core import errors
from renku.infrastructure.database import Database
from tests.fixtures.repository import RenkuProject
from tests.utils import get_test_bindings


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


@pytest.fixture
def with_injection():
    """Factory fixture for test injections manager."""
    from renku.command.command_builder.command import inject, remove_injector
    from renku.domain_model.project_context import project_context

    @contextlib.contextmanager
    def with_injection_helper(bindings, constructor_bindings, path: Path):
        """Context manager to temporarily do injections."""

        def bind(binder):
            for key, value in bindings.items():
                binder.bind(key, value)
            for key, value in constructor_bindings.items():
                binder.bind_to_constructor(key, value)

            return binder

        inject.configure(bind, bind_in_runtime=False)

        with project_context.with_path(path, save_changes=True):
            try:
                yield
            finally:
                remove_injector()

    def test_injection_manager_helper(project: Optional["RenkuProject"] = None):
        path = project.path if project else project_context.path
        bindings, constructor_bindings = get_test_bindings()
        return with_injection_helper(bindings=bindings, constructor_bindings=constructor_bindings, path=path)

    return test_injection_manager_helper


@pytest.fixture
def project_with_injection(project, with_injection) -> Generator[RenkuProject, None, None]:
    """Fixture for context manager with project and database injection."""
    with with_injection():
        yield project
