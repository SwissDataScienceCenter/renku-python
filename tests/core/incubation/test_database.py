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
from renku.core.models.provenance.activity import Activity, Association, Usage
from renku.core.models.workflow.plan import Plan


def test_database_create(client, runner):
    """Test database files are created in an empty project."""
    assert 0 == runner.invoke(cli, ["graph", "generate"]).exit_code

    assert not client.repo.is_dirty()
    root_objects = ["root", "activities", "plans"]
    for filename in root_objects:
        assert (client.database_path / filename).exists()


def test_database_add(database):
    """Test adding an object to the database."""
    database, storage = database

    id = "/activities/42"
    activity = Activity(id=id)
    database.add(activity)
    database.commit()

    root_objects = ["root", "activities", "plans"]
    for filename in root_objects:
        assert storage.exists(filename)

    oid = Database.hash_id(id)
    assert storage.exists(oid)


def test_database_add_also_adds_to_root_object(database):
    """Test adding an object to the database also adds it to the root object before committing."""
    database, storage = database

    root = database.root
    database.commit()

    id = "/activities/42"
    activity_1 = Activity(id=id)
    database.add(activity_1)

    activity_2 = list(root["activities"].values())[0]

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
    assert not storage.exists(oid)


def test_database_update_required_object_only(database):
    """Test adding an object to the database does not cause an update to all other objects."""
    database, storage = database

    id_1 = "/activities/42"
    activity_1 = Activity(id=id_1)
    database.add(activity_1)
    database.commit()
    oid_1 = Database.hash_id(id_1)
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

    entity_modification_time_before = storage.get_modification_date("plans")
    activity_modification_time_before = storage.get_modification_date("activities")

    activity = Activity(id="/activities/42")
    database.add(activity)
    database.commit()

    entity_modification_time_after = storage.get_modification_date("plans")
    activity_modification_time_after = storage.get_modification_date("activities")

    assert entity_modification_time_before == entity_modification_time_after
    assert activity_modification_time_before != activity_modification_time_after


def test_database_add_non_persistent(database):
    """Test adding a non-Persistent object to the database raises an error."""
    database, _ = database

    class Dummy:
        id = 42

    with pytest.raises(AssertionError) as e:
        object = Dummy()
        database.add(object)

    assert "Cannot add objects of type" in str(e)


def test_database_loads_only_required_objects(database):
    """Test loading an object does not load its Persistent members."""
    database, storage = database

    plan = Plan(id="/plan/9")
    association = Association(id="association", plan=plan)
    id = "/activities/42"
    activity = Activity(id=id, association=association)
    database.add(activity)
    database.commit()

    new_database = Database(storage=storage)
    oid = Database.hash_id(id)
    activity = new_database.get(oid)

    # Access a field to make sure that activity is loaded
    _ = activity.id

    assert UPTODATE == activity._p_state
    assert PERSISTED == activity._p_serial
    assert GHOST == activity.association.plan._p_state

    assert UPTODATE == new_database.root["plans"]._p_state
    assert UPTODATE == new_database.root["activities"]._p_state


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
    activity_2 = list(new_database.root["activities"].values())[0]

    assert activity_1 is activity_2


def test_database_index_list(database):
    """Test adding an IndexList."""
    database, storage = database

    index_name = "associations"
    database.add_index(name=index_name, value_type=Activity, attribute="association.id", is_list=True)

    plan = Plan(id="/plan/9")
    association = Association(id="/association/42", plan=plan)

    id_1 = "/activities/1"
    activity_1 = Activity(id=id_1, association=association)
    database.add(activity_1)

    id_2 = "/activities/2"
    activity_2 = Activity(id=id_2, association=association)
    database.add(activity_2)

    database.commit()

    new_database = Database(storage=storage)
    usages = new_database.get(index_name)
    activities = usages.get("/association/42")

    assert {id_1, id_2} == {a.id for a in activities}


def test_database_index_update(database):
    """Test adding objects with the same key, updates the Index."""
    database, storage = database

    index_name = "plan-names"
    database.add_index(name=index_name, value_type=Plan, attribute="name")

    name = "same-name"

    plan_1 = Plan(id="/plans/42", name=name, description="old")
    database.add(plan_1)
    plan_2 = Plan(id="/plans/43", name=name, description="new")
    database.add(plan_2)
    assert plan_2 is database.get(index_name).get(name)

    database.commit()

    plan_3 = Plan(id="/plans/44", name=name, description="newer")
    database.add(plan_3)
    database.commit()

    new_database = Database(storage=storage)
    plans = new_database.get(index_name)
    plan = plans.get(name)

    assert "newer" == plan.description


def test_database_add_duplicate_index(database):
    """Test cannot add an index with the same name."""
    database, _ = database

    same_name = "plans"

    with pytest.raises(AssertionError) as e:
        database.add_index(name=same_name, value_type=Plan, attribute="name")

    assert "Index already exists: 'plans'" in str(e)


def test_database_index_different_key_type(database):
    """Test adding an Index with a different key type."""
    database, storage = database

    index_name = "usages"
    database.add_index(name=index_name, value_type=Activity, attribute="entity.path", key_type=Usage)

    entity = Entity(checksum="42", path="/dummy/path")
    usage = Usage(entity=entity, id="/usages/42")

    activity = Activity(id="/activities/42", usages=[usage])
    database.add(activity, key_object=usage)
    database.commit()

    new_database = Database(storage=storage)
    usages = new_database.get(index_name)
    activity = usages.get("/dummy/path")

    assert "/activities/42" == activity.id
    assert "42" == activity.usages[0].entity.checksum
    assert "/dummy/path" == activity.usages[0].entity.path


def test_database_wrong_index_key_type(database):
    """Test adding an Index with a wrong key type."""
    database, _ = database

    index_name = "usages"
    database.add_index(name=index_name, value_type=Activity, attribute="id", key_type=Usage)

    activity = Activity(id="/activities/42")

    with pytest.raises(AssertionError) as e:
        database.add(activity)

    assert "Invalid key type" in str(e)
