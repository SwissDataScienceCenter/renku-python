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
from persistent.list import PersistentList
from persistent.mapping import PersistentMapping

from renku.core.metadata.database import PERSISTED, Database
from renku.core.models.entity import Entity
from renku.core.models.provenance.activity import Activity, Association, Usage
from renku.core.models.workflow.plan import Plan


def test_database_add(database):
    """Test adding an object to an index."""
    database, storage = database

    id = "/activities/42"
    activity = Activity(id=id)
    index = database.get("activities")
    index.add(activity)
    database.commit()

    root_objects = ["root", "activities", "plans"]
    for filename in root_objects:
        assert storage.exists(filename)

    oid = Database.hash_id(id)
    assert storage.exists(oid)


def test_database_add_using_set_item(database):
    """Test adding an object to the database using __setitem__."""
    database, storage = database

    id = "/activities/42"
    activity = Activity(id=id)
    database["activities"][id] = activity

    assert {id} == set(database["activities"].keys())
    assert {activity} == set(database["activities"].values())


def test_database_index_with_no_automatic_key(database):
    """Test indexes with no automatic key attribute."""
    database, storage = database
    index = database.add_index(name="manual", object_type=Activity)

    id = "/activities/42"
    activity = Activity(id=id)
    index.add(activity, key=id)

    database.commit()

    new_database = Database(storage=storage)
    activity = new_database["manual"][id]

    assert id == activity.id

    oid = Database.hash_id(id)
    assert storage.exists(oid)


def test_database_add_with_incorrect_key(database):
    """Test adding an object to the database using __setitem__ with an incorrect key should fail."""
    database, storage = database

    id = "/activities/42"
    activity_1 = Activity(id=id)

    with pytest.raises(AssertionError) as e:
        database["activities"]["incorrect-key"] = activity_1

    assert "Incorrect key for index 'activities': 'incorrect-key' != '/activities/42'" in str(e)


def test_database_add_fails_when_no_key_and_no_automatic_key(database):
    """Test adding to an index with no automatic key fails if no key is provided."""
    database, storage = database
    index = database.add_index(name="manual", object_type=Activity)

    activity = Activity(id="/activities/42")

    with pytest.raises(AssertionError) as e:
        index.add(activity)

    assert "No key is provided" in str(e)


def test_database_no_file_created_if_not_committed(database):
    """Test adding an object to a database does not create a file before commit."""
    database, storage = database
    database.commit()

    assert storage.exists("root")

    id = "/activities/42"
    activity = Activity(id=id)
    database.get("activities").add(activity)

    oid = Database.hash_id(id)
    assert not storage.exists(oid)


def test_database_update_required_object_only(database):
    """Test adding an object to the database does not cause an update to all other objects."""
    database, storage = database

    index = database.get("activities")

    id_1 = "/activities/42"
    activity_1 = Activity(id=id_1)
    index.add(activity_1)
    database.commit()
    oid_1 = Database.hash_id(id_1)
    modification_time_before = storage.get_modification_date(oid_1)

    id_2 = "/activities/43"
    activity_2 = Activity(id=id_2)
    index.add(activity_2)
    database.commit()

    modification_time_after = storage.get_modification_date(oid_1)

    assert modification_time_before == modification_time_after


def test_database_update_required_root_objects_only(database):
    """Test adding an object to an index does not cause an update to other indexes."""
    database, storage = database

    database.commit()

    entity_modification_time_before = storage.get_modification_date("plans")
    activity_modification_time_before = storage.get_modification_date("activities")

    activity = Activity(id="/activities/42")
    database.get("activities").add(activity)
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
        database.get("activities").add(object)

    assert "Cannot add objects of type" in str(e)


def test_database_loads_only_required_objects(database):
    """Test loading an object does not load its Persistent members."""
    database, storage = database

    plan = Plan(id="/plan/9")
    association = Association(id="association", plan=plan)
    id = "/activities/42"
    activity = Activity(id=id, association=association)
    database.get("activities").add(activity)
    database.commit()

    new_database = Database(storage=storage)
    oid = Database.hash_id(id)
    activity = new_database.get(oid)

    # Access a field to make sure that activity is loaded
    _ = activity.id

    assert UPTODATE == activity._p_state, activity._p_status
    assert PERSISTED == activity._p_serial
    assert GHOST == activity.association.plan._p_state

    assert UPTODATE == new_database["plans"]._p_state
    assert UPTODATE == new_database["activities"]._p_state


def test_database_load_multiple(database):
    """Test loading an object from multiple indexes returns the same object."""
    database, storage = database
    database.add_index(name="associations", object_type=Activity, attribute="association.id")

    plan = Plan(id="/plan/9")
    association = Association(id="/association/42", plan=plan)
    id = "/activities/42"
    activity = Activity(id=id, association=association)
    database.get("activities").add(activity)
    database.get("associations").add(activity)
    database.commit()

    new_database = Database(storage=storage)
    oid = Database.hash_id(id)
    activity_1 = new_database.get(oid)
    activity_2 = new_database.get("activities").get(id)
    activity_3 = new_database.get("associations").get("/association/42")

    assert activity_1 is activity_2
    assert activity_2 is activity_3


def test_database_index_update(database):
    """Test adding objects with the same key, updates the Index."""
    database, storage = database

    index_name = "plan-names"
    database.add_index(name=index_name, object_type=Plan, attribute="name")

    name = "same-name"

    plan_1 = Plan(id="/plans/42", name=name, description="old")
    database.get(index_name).add(plan_1)
    plan_2 = Plan(id="/plans/43", name=name, description="new")
    database.get(index_name).add(plan_2)
    assert plan_2 is database.get(index_name).get(name)

    database.commit()

    plan_3 = Plan(id="/plans/44", name=name, description="newer")
    database.get(index_name).add(plan_3)
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
        database.add_index(name=same_name, object_type=Plan, attribute="name")

    assert "Index or object already exists: 'plans'" in str(e)


def test_database_index_different_key_type(database):
    """Test adding an Index with a different key type."""
    database, storage = database

    index_name = "usages"
    index = database.add_index(name=index_name, object_type=Activity, attribute="entity.path", key_type=Usage)

    entity = Entity(checksum="42", path="dummy/path")
    usage = Usage(entity=entity, id="/usages/42")

    activity = Activity(id="/activities/42", usages=[usage])
    database.get(index_name).add(activity, key_object=usage)
    database.commit()

    new_database = Database(storage=storage)
    usages = new_database[index_name]
    activity = usages.get("dummy/path")

    assert "/activities/42" == activity.id
    assert "42" == activity.usages[0].entity.checksum
    assert "dummy/path" == activity.usages[0].entity.path

    key = index.generate_key(activity, key_object=usage)

    assert activity is usages[key]


def test_database_wrong_index_key_type(database):
    """Test adding to an Index with a wrong key type."""
    database, _ = database

    index_name = "usages"
    database.add_index(name=index_name, object_type=Activity, attribute="id", key_type=Usage)

    activity = Activity(id="/activities/42")

    with pytest.raises(AssertionError) as e:
        database.get(index_name).add(activity)

    assert "Invalid key type" in str(e)


def test_database_missing_attribute(database):
    """Test adding to an Index while object does not have the requires attribute."""
    database, _ = database

    index_name = "usages"
    database.add_index(name=index_name, object_type=Activity, attribute="missing.attribute")

    activity = Activity(id="/activities/42")

    with pytest.raises(AttributeError) as e:
        database.get(index_name).add(activity)

    assert "'Activity' object has no attribute 'missing'" in str(e)


def test_database_remove(database):
    """Test removing an object from an index."""
    database, storage = database

    id = "/activities/42"
    activity = Activity(id=id)
    database.get("activities").add(activity)
    database.commit()

    database = Database(storage=storage)
    database.get("activities").pop(id)
    database.commit()

    database = Database(storage=storage)
    activity = database.get("activities").get(id, None)

    assert activity is None
    # However, the file still exists in the storage
    oid = Database.hash_id(id)
    assert storage.exists(oid)


def test_database_remove_non_existing(database):
    """Test removing a non-existing object from an index."""
    database, storage = database

    with pytest.raises(KeyError):
        database.get("activities").pop("non-existing-key")

    object = database.get("activities").pop("non-existing-key", None)

    assert object is None


def test_database_persistent_collections(database):
    """Test using Persistent collections."""
    database, storage = database
    index_name = "collections"
    database.add_index(name=index_name, object_type=PersistentMapping)

    entity_checksum = "42"
    entity_path = "dummy/path"
    usage = Usage(entity=Entity(checksum=entity_checksum, path=entity_path), id="/usages/42")
    id_1 = "/activities/1"
    activity_1 = Activity(id=id_1, usages=[usage])
    id_2 = "/activities/2"
    activity_2 = Activity(id=id_2, usages=[usage])

    p_mapping = PersistentMapping()

    database[index_name][entity_path] = p_mapping

    p_list = PersistentList()
    p_mapping[entity_checksum] = p_list
    p_list.append(activity_1)
    p_list.append(activity_2)

    database.commit()

    new_database = Database(storage=storage)
    collections = new_database[index_name]

    id_3 = "/activities/3"
    activity_3 = Activity(id=id_3, usages=[usage])
    collections[entity_path][entity_checksum].append(activity_3)

    assert {id_1, id_2, id_3} == {activity.id for activity in collections[entity_path][entity_checksum]}


def test_database_immutable_object(database):
    """Test storage and retrieval of immutable objects."""
    database, storage = database
    index_name = "collections"
    database.add_index(name=index_name, object_type=PersistentMapping)

    usage = Usage(entity=Entity(checksum="42", path="dummy/path"), id="/usages/42")
    id_1 = "/activities/1"
    activity_1 = Activity(id=id_1, usages=[usage])
    id_2 = "/activities/2"
    activity_2 = Activity(id=id_2, usages=[usage])

    database.get("activities").add(activity_1)
    database.get("activities").add(activity_2)
    database.commit()

    database = Database(storage=storage)
    activity_1 = database["activities"][id_1]
    activity_2 = database["activities"][id_2]

    usage_1 = activity_1.usages[0]
    usage_2 = activity_1.usages[0]

    assert isinstance(usage_1, Usage)
    assert usage_1 is usage_2
