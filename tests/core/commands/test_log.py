#
# Copyright 2017-2023- Swiss Data Science Center (SDSC)
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
"""Renku log command tests."""


from datetime import datetime, timedelta
from typing import List

from renku.command.command_builder.command import inject, remove_injector
from renku.command.log import _log
from renku.command.view_model.log import DatasetLogViewModel, LogType
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.domain_model.dataset import Url
from renku.domain_model.provenance.activity import Activity, Association
from renku.domain_model.provenance.agent import Person, SoftwareAgent


def test_log_activities(mocker):
    """Test getting activity viewmodels on log."""
    agents = [
        Person(id="mailto:john.doe@example.com", email="john.doe@example.com", name="John Doe"),
        SoftwareAgent(name="renku 99.1.1", id="<https://github.com/swissdatasciencecenter/renku-python/tree/v0.16.1>"),
    ]

    plan1 = mocker.MagicMock()
    plan1.to_argv.return_value = ["touch", "A"]
    plan1.copy.return_value = plan1

    plan2 = mocker.MagicMock()
    plan2.to_argv.return_value = ["cp", "A", "B"]
    plan2.copy.return_value = plan2

    plan3 = mocker.MagicMock()
    plan3.to_argv.return_value = ["cp", "B", "C"]
    plan3.copy.return_value = plan3

    previous_id = Activity.generate_id()
    previous = Activity(
        id=previous_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=15),
        ended_at_time=datetime.utcnow() - timedelta(hours=1, seconds=10),
        association=Association(id=Association.generate_id(previous_id), plan=plan1, agent=agents[1]),
        agents=agents,
    )

    intermediate_id = Activity.generate_id()
    intermediate = Activity(
        id=intermediate_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=10),
        ended_at_time=datetime.utcnow() - timedelta(hours=1, seconds=5),
        association=Association(id=Association.generate_id(intermediate_id), plan=plan2, agent=agents[1]),
        agents=agents,
    )

    following_id = Activity.generate_id()
    following = Activity(
        id=following_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=5),
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(following_id), plan=plan3, agent=agents[1]),
        agents=agents,
    )

    activity_gateway = mocker.MagicMock(spec=IActivityGateway)
    activity_gateway.get_all_activities.return_value = [previous, intermediate, following]
    full_agents = [a.full_identity for a in agents]
    result = _log(
        activity_gateway=activity_gateway,
        dataset_gateway=mocker.MagicMock(spec=IDatasetGateway),
        workflows_only=True,
        datasets_only=False,
    )
    assert 3 == len(result)
    assert all(log.type == LogType.ACTIVITY for log in result)
    assert result[0].date == previous.ended_at_time
    assert result[1].date == intermediate.ended_at_time
    assert result[2].date == following.ended_at_time
    assert result[0].agents == full_agents
    assert result[1].agents == full_agents
    assert result[2].agents == full_agents
    assert result[0].description == "touch A"
    assert result[1].description == "cp A B"
    assert result[2].description == "cp B C"


def test_log_dataset_create_simple(mocker):
    """Test getting dataset viewmodels on create."""

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.name = None
    new_dataset.description = None
    new_dataset.derived_from = None
    new_dataset.same_as = None
    new_dataset.dataset_files = []
    new_dataset.date_removed = None
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.is_derivation.return_value = False

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result: List[DatasetLogViewModel] = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 1 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert not entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': created" == entry.description

    assert entry.details.name_changed is None
    assert not entry.details.description_changed
    assert not entry.details.creators_added
    assert not entry.details.creators_removed
    assert not entry.details.keywords_added
    assert not entry.details.keywords_removed
    assert not entry.details.images_changed_to
    assert not entry.details.source
    assert entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert not entry.details.modified


def test_log_dataset_create_complex(mocker):
    """Test getting dataset viewmodels on create."""

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.derived_from = None
    new_dataset.same_as = None
    new_dataset.name = "new-name"
    new_dataset.description = "new-description"
    new_dataset.dataset_files = []
    new_dataset.creators = [mocker.MagicMock(full_identity="John")]
    new_dataset.keywords = ["a", "b"]
    new_dataset.images = [mocker.MagicMock(content_url="./img/img1.png")]
    new_dataset.date_removed = None
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.is_derivation.return_value = False

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 1 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert ["John"] == entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': created" == entry.description

    assert "new-name" == entry.details.name_changed
    assert "new-description" == entry.details.description_changed
    assert ["John"] == entry.details.creators_added
    assert not entry.details.files_added
    assert not entry.details.files_removed
    assert not entry.details.creators_removed
    assert {"a", "b"} == set(entry.details.keywords_added)
    assert not entry.details.keywords_removed
    assert ["./img/img1.png"] == entry.details.images_changed_to
    assert not entry.details.source
    assert entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert not entry.details.modified


def test_log_dataset_add_create(mocker):
    """Test getting dataset viewmodels on create."""

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.derived_from = None
    new_dataset.same_as = None
    new_dataset.name = "new-name"
    new_dataset.description = "new-description"
    new_dataset.dataset_files = [
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_a")),
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_b")),
    ]
    new_dataset.date_removed = None
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.is_derivation.return_value = False

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 1 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert not entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': created, 2 file(s) added" == entry.description

    assert "new-name" == entry.details.name_changed
    assert "new-description" == entry.details.description_changed
    assert not entry.details.creators_added
    assert {"file_b", "file_a"} == set(entry.details.files_added)
    assert not entry.details.files_removed
    assert not entry.details.creators_removed
    assert not entry.details.keywords_added
    assert not entry.details.keywords_removed
    assert not entry.details.images_changed_to
    assert not entry.details.source
    assert entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert not entry.details.modified


def test_log_dataset_import(mocker):
    """Test getting dataset viewmodels for an imported dataset."""
    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.derived_from = None
    new_dataset.name = "new-name"
    new_dataset.description = "new-description"
    new_dataset.same_as = mocker.MagicMock(value="http://renkulab.io/my/dataset")
    new_dataset.dataset_files = [
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_a")),
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_b")),
    ]
    new_dataset.date_removed = None
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.is_derivation.return_value = False

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 1 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert not entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': imported, 2 file(s) added" == entry.description

    assert "new-name" == entry.details.name_changed
    assert "new-description" == entry.details.description_changed
    assert not entry.details.creators_added
    assert {"file_b", "file_a"} == set(entry.details.files_added)
    assert not entry.details.files_removed
    assert not entry.details.creators_removed
    assert not entry.details.keywords_added
    assert not entry.details.keywords_removed
    assert not entry.details.images_changed_to
    assert "http://renkulab.io/my/dataset" == entry.details.source

    assert not entry.details.created
    assert entry.details.imported
    assert not entry.details.migrated
    assert not entry.details.modified


def test_log_dataset_deleted(mocker):
    """Test getting dataset viewmodels for deleted dataset."""
    old_dataset = mocker.MagicMock()
    old_dataset.id = "old"
    old_dataset.slug = "ds"
    old_dataset.derived_from = None
    old_dataset.same_as = None
    old_dataset.dataset_files = []
    old_dataset.is_derivation.return_value = False

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.name = None
    new_dataset.description = None
    new_dataset.derived_from = Url(url_id="old")
    new_dataset.same_as = None
    new_dataset.dataset_files = []
    new_dataset.date_removed = datetime.utcnow()
    new_dataset.is_derivation.return_value = True

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    def _mock_get_by_id(id):
        if id == "new":
            return new_dataset
        if id == "old":
            return old_dataset

        return None

    dataset_gateway.get_by_id.side_effect = _mock_get_by_id

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result: List[DatasetLogViewModel] = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 2 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert not entry.agents
    assert new_dataset.date_removed == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': deleted" == entry.description

    assert entry.details.name_changed is None
    assert not entry.details.description_changed
    assert not entry.details.files_added
    assert not entry.details.files_removed
    assert not entry.details.creators_added
    assert not entry.details.creators_removed
    assert not entry.details.keywords_added
    assert not entry.details.keywords_removed
    assert not entry.details.images_changed_to
    assert not entry.details.source
    assert not entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert not entry.details.modified
    assert entry.details.deleted


def test_log_dataset_files_removed(mocker):
    """Test getting dataset viewmodels for dataset with files removed."""
    old_dataset = mocker.MagicMock()
    old_dataset.id = "old"
    old_dataset.slug = "ds"
    old_dataset.name = None
    old_dataset.description = None
    old_dataset.derived_from = None
    old_dataset.same_as = None
    old_dataset.dataset_files = [
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_a")),
        mocker.MagicMock(date_removed=None, entity=mocker.MagicMock(path="file_b")),
    ]
    old_dataset.is_derivation.return_value = False

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.name = None
    new_dataset.description = None
    new_dataset.derived_from = Url(url_id="old")
    new_dataset.same_as = None
    new_dataset.dataset_files = [old_dataset.dataset_files[0]]
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.date_removed = None
    new_dataset.is_derivation.return_value = True

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    def _mock_get_by_id(id):
        if id == "new":
            return new_dataset
        if id == "old":
            return old_dataset

        return None

    dataset_gateway.get_by_id.side_effect = _mock_get_by_id

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result: List[DatasetLogViewModel] = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 2 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert not entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': 1 file(s) removed" == entry.description

    assert entry.details.name_changed is None
    assert not entry.details.description_changed
    assert not entry.details.files_added
    assert ["file_b"] == entry.details.files_removed
    assert not entry.details.creators_added
    assert not entry.details.creators_removed
    assert not entry.details.keywords_added
    assert not entry.details.keywords_removed
    assert not entry.details.images_changed_to
    assert not entry.details.source
    assert not entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert entry.details.modified
    assert not entry.details.deleted


def test_log_dataset_metadata_modified(mocker):
    """Test getting dataset viewmodels for dataset with files removed."""
    old_dataset = mocker.MagicMock()
    old_dataset.id = "old"
    old_dataset.slug = "ds"
    old_dataset.name = "old-name"
    old_dataset.description = "old-description"
    old_dataset.dataset_files = []
    old_dataset.creators = [mocker.MagicMock(full_identity="John")]
    old_dataset.keywords = ["a", "b"]
    old_dataset.images = [mocker.MagicMock(content_url="./img/img1.png")]
    old_dataset.derived_from = None
    old_dataset.same_as = None
    old_dataset.dataset_files = []
    old_dataset.is_derivation.return_value = False

    new_dataset = mocker.MagicMock()
    new_dataset.id = "new"
    new_dataset.slug = "ds"
    new_dataset.name = "new-name"
    new_dataset.description = "new-description"
    new_dataset.derived_from = Url(url_id="old")
    new_dataset.same_as = None
    new_dataset.creators = [mocker.MagicMock(full_identity="Jane")]
    new_dataset.keywords = ["a", "c"]
    new_dataset.images = [mocker.MagicMock(content_url="./img/img2.png")]
    new_dataset.dataset_files = []
    new_dataset.date_modified = datetime.utcnow()
    new_dataset.date_removed = None
    new_dataset.is_derivation.return_value = True

    dataset_gateway = mocker.MagicMock(spec=IDatasetGateway)
    dataset_gateway.get_provenance_tails.return_value = [new_dataset]

    def _mock_get_by_id(id):
        if id == "new":
            return new_dataset
        if id == "old":
            return old_dataset

        return None

    dataset_gateway.get_by_id.side_effect = _mock_get_by_id

    inject.configure(lambda binder: binder.bind(IDatasetGateway, dataset_gateway), bind_in_runtime=False)

    try:
        result: List[DatasetLogViewModel] = _log(
            activity_gateway=mocker.MagicMock(spec=IActivityGateway),
            dataset_gateway=dataset_gateway,
            workflows_only=False,
            datasets_only=True,
        )
    finally:
        remove_injector()

    assert 2 == len(result)

    entry = result[0]
    assert LogType.DATASET == entry.type
    assert ["Jane"] == entry.agents
    assert new_dataset.date_modified == entry.date
    assert "ds" == entry.id
    assert "Dataset 'ds': metadata modified" == entry.description

    assert "new-name" == entry.details.name_changed
    assert "new-description" == entry.details.description_changed
    assert not entry.details.files_added
    assert not entry.details.files_removed
    assert ["Jane"] == entry.details.creators_added
    assert ["John"] == entry.details.creators_removed
    assert ["c"] == entry.details.keywords_added
    assert ["b"] == entry.details.keywords_removed
    assert ["./img/img2.png"] == entry.details.images_changed_to
    assert not entry.details.source
    assert not entry.details.created
    assert not entry.details.imported
    assert not entry.details.migrated
    assert entry.details.modified
    assert not entry.details.deleted
