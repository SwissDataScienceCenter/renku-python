# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Test metadata Database."""
import pytest
from persistent import GHOST, UPTODATE

from renku.cli import cli
from renku.core.incubation.database import PERSISTED, Database
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity


def test_database_create(client, runner):
    """Test database files are created in an empty project."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code

    assert not client.repo.is_dirty()
    root_objects = ["root", "activity", "entity", "plan"]
    for filename in root_objects:
        assert (client.database_path / filename).exists()


def test_database_add(database):
    """Test adding an object to the database."""
    database, storage = database

    id = "/activities/42"
    activity = Activity(id=id)
    database.add(activity)
    database.commit()

    root_objects = ["root", "activity", "entity", "plan"]
    for filename in root_objects:
        assert storage.exists(filename)

    oid = Database.hash_id(id)
    assert storage.exists(oid.decode("ascii"))


def test_database_add_to_root_object(database):
    """Test adding an object to a root object."""
    database, storage = database

    root = database.root
    database.commit()

    id = "/activities/42"
    activity = Activity(id=id)
    activity_root = root["Activity"]
    activity_root[id] = activity
    database.commit()

    oid = Database.hash_id(id)
    assert storage.exists(oid.decode("ascii"))


def test_database_add_also_adds_to_root_object(database):
    """Test adding an object to the database also adds it to the root object before committing."""
    database, storage = database

    root = database.root
    database.commit()

    id = "/activities/42"
    activity_1 = Activity(id=id)
    database.add(activity_1)

    activity_2 = list(root["Activity"].values())[0]

    assert activity_1 is activity_2


def test_database_no_file_created_if_not_committed(database):
    """Test adding an object to a database does not create a file before commit."""
    database, storage = database

    _ = database.root
    database.commit()

    id = "/activities/42"
    activity = Activity(id=id)
    database.add(activity)

    oid = Database.hash_id(id)
    assert not storage.exists(oid.decode("ascii"))


def test_database_update_required_object_only(database):
    """Test adding an object to the database does not cause an update to all other objects."""
    database, storage = database

    id_1 = "/activities/42"
    activity_1 = Activity(id=id_1)
    database.add(activity_1)
    database.commit()
    oid_1 = Database.hash_id(id_1).decode("ascii")
    modification_time_before = storage.get_modification_date(oid_1)

    id_2 = "/activities/43"
    activity_2 = Activity(id=id_2)
    database.add(activity_2)
    database.commit()

    modification_time_after = storage.get_modification_date(oid_1)

    assert modification_time_before == modification_time_after


def test_database_update_required_root_objects_only(database):
    """Test adding an object to the database does not cause an update to other root objects."""
    database, storage = database

    _ = database.root
    database.commit()

    entity_modification_time_before = storage.get_modification_date("entity")
    activity_modification_time_before = storage.get_modification_date("activity")

    activity = Activity(id="/activities/42")
    database.add(activity)
    database.commit()

    entity_modification_time_after = storage.get_modification_date("entity")
    activity_modification_time_after = storage.get_modification_date("activity")

    assert entity_modification_time_before == entity_modification_time_after
    assert activity_modification_time_before != activity_modification_time_after


def test_database_add_non_persistent(database):
    """Test adding a non-Persistent object to the database raises an error."""
    database, _ = database

    class Dummy:
        id = 42

    with pytest.raises(AssertionError):
        object = Dummy()
        database.add(object)


def test_database_loads_only_required_objects(database):
    """Test loading an object does not load its Persistent members."""
    database, storage = database

    entity = Entity(checksum="42", path="dummy")
    id = "/activities/42"
    activity = Activity(id=id, invalidations=[entity])
    database.add(activity)
    database.commit()

    new_database = Database(storage=storage)
    oid = Database.hash_id(id)
    activity = new_database.get(oid)

    assert UPTODATE == activity._p_state
    assert PERSISTED == activity._p_serial
    assert GHOST == activity.invalidations[0]._p_state

    root = new_database.root
    assert GHOST == root["Entity"]._p_state
    assert GHOST == root["Activity"]._p_state  # NOTE: Object was loaded directly and not via "Activity" root


def test_database_load_multiple(database):
    """Test loading an object from multiple sources returns the same object."""
    database, storage = database

    entity = Entity(checksum="42", path="dummy")
    id = "/activities/42"
    activity = Activity(id=id, invalidations=[entity])
    database.add(activity)
    database.commit()

    new_database = Database(storage=storage)
    oid = Database.hash_id(id)
    activity_1 = new_database.get(oid)
    activity_2 = list(new_database.root["Activity"].values())[0]

    assert activity_1 is activity_2

    root = new_database.root
    assert GHOST == root["Entity"]._p_state
    assert UPTODATE == root["Activity"]._p_state
