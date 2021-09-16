# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Dispatcher tests."""
from pathlib import Path

import pytest

from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder.client_dispatcher import ClientDispatcher
from renku.core.management.command_builder.database_dispatcher import DatabaseDispatcher


def test_client_dispatcher(tmpdir):
    """Test getting correct current client."""
    test_dir = tmpdir.mkdir("test")
    other_test_dir = tmpdir.mkdir("other_test")

    dispatcher = ClientDispatcher()

    with pytest.raises(errors.ConfigurationError):
        dispatcher.current_client

    dispatcher.push_client_to_stack(Path(tmpdir))

    assert dispatcher.current_client.path == Path(tmpdir).resolve()

    dispatcher.push_client_to_stack(str(Path(test_dir)))

    assert dispatcher.current_client.path == Path(test_dir).resolve()

    dispatcher.push_created_client_to_stack(LocalClient(Path(other_test_dir)))

    assert dispatcher.current_client.path == Path(other_test_dir).resolve()

    dispatcher.pop_client()

    assert dispatcher.current_client.path == Path(test_dir).resolve()

    dispatcher.pop_client()

    assert dispatcher.current_client.path == Path(tmpdir).resolve()

    dispatcher.pop_client()

    with pytest.raises(errors.ConfigurationError):
        dispatcher.current_client


def test_database_dispatcher(tmpdir):
    """Test getting correct database."""

    test_dir = tmpdir.mkdir("test")
    other_test_dir = tmpdir.mkdir("other_test")

    dispatcher = DatabaseDispatcher()

    with pytest.raises(errors.ConfigurationError):
        dispatcher.current_database

    dispatcher.push_database_to_stack(Path(tmpdir))

    assert dispatcher.current_database._storage.path == Path(tmpdir).resolve()

    dispatcher.push_database_to_stack(str(Path(test_dir)))

    assert dispatcher.current_database._storage.path == Path(test_dir).resolve()

    dispatcher.push_database_to_stack(Path(other_test_dir), commit=True)

    assert dispatcher.current_database._storage.path == Path(other_test_dir).resolve()

    dispatcher.pop_database()

    # NOTE: Make sure the database was committed on pop
    assert (Path(other_test_dir) / "root").exists()

    assert dispatcher.current_database._storage.path == Path(test_dir).resolve()

    dispatcher.pop_database()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(test_dir) / "root").exists()

    assert dispatcher.current_database._storage.path == Path(tmpdir).resolve()

    dispatcher.pop_database()

    # NOTE: Make sure the database was not committed on pop
    assert not (Path(tmpdir) / "root").exists()

    with pytest.raises(errors.ConfigurationError):
        dispatcher.current_database
