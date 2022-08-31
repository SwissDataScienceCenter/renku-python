# -*- coding: utf-8 -*-
#
# Copyright 2019-2022 - Swiss Data Science Center (SDSC)
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

from renku.command.command_builder.database_dispatcher import DatabaseDispatcher
from renku.core import errors


def test_global_renku_config(tmpdir):
    """Test RenkuConfig object."""
    from renku.core.management.project_config import RenkuConfig

    config = RenkuConfig()

    with pytest.raises(IndexError):
        config.pop_path()

    previous_path = config.path
    path = Path(tmpdir.mkdir("push"))
    config.push_path(path)

    assert config.path == path

    assert config.pop_path() == path
    assert config.path == previous_path

    config.replace_path(path)
    assert config.path == path

    with pytest.raises(IndexError):
        config.pop_path()

    new_path = Path(tmpdir.mkdir("new"))

    with config.with_path(new_path):
        assert config.path == new_path

    assert config.path == path


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
