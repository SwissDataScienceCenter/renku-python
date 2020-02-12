# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Test ``migrate`` command."""
from pathlib import Path

import pytest

from renku import LocalClient
from renku.cli import cli
from renku.core.management.config import RENKU_HOME
from renku.core.models.datasets import Dataset
from renku.core.utils.urls import url_to_string


@pytest.mark.migration
def test_status_with_old_repository(isolated_runner, old_project):
    """Test status on all old repositories created by old version of renku."""
    runner = isolated_runner
    result = runner.invoke(cli, ['status'])
    assert 0 == result.exit_code

    output = result.output.split('\n')
    assert output.pop(0) == 'On branch master'
    assert output.pop(0) == 'All files were generated from the latest inputs.'


@pytest.mark.migration
def test_update_with_old_repository(isolated_runner, old_project):
    """Test update on all old repositories created by old version of renku."""
    runner = isolated_runner

    result = runner.invoke(cli, ['update'])
    assert 0 == result.exit_code

    output = result.output.split('\n')
    assert output.pop(0) == 'All files were generated from the latest inputs.'


@pytest.mark.migration
def test_list_with_old_repository(isolated_runner, old_project):
    """Test dataset list on old repository."""
    result = isolated_runner.invoke(cli, ['dataset'])

    assert old_project['exit_code'] == result.exit_code
    assert not old_project['repo'].is_dirty()


@pytest.mark.migration
def test_migrate_datasets_with_old_repository(isolated_runner, old_project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code
    assert not old_project['repo'].is_dirty()


@pytest.mark.migration
def test_correct_path_migrated(isolated_runner, old_project):
    """Check if path on dataset files has been correctly migrated."""
    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project['path'])
    assert client.datasets

    for ds in client.datasets.values():
        for file_ in ds.files:
            path_ = Path(file_.path)
            assert path_.exists()
            assert not path_.is_absolute()
            assert file_._label
            assert file_._id
            assert file_.path in file_._label
            assert file_.path in file_._id


@pytest.mark.migration
def test_author_to_creator_migration(isolated_runner, old_project):
    """Check renaming of author to creator migration."""
    client = LocalClient(path=old_project['path'])
    if client.datasets:
        dataset = client.datasets.popitem()[1]
        dataset_path_pre40 = Path(dataset.path.replace('-', ''))
        if dataset_path_pre40.exists():
            metadata = (dataset_path_pre40 / client.METADATA).read_text()

            assert 'authors:' in metadata
            result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
            assert 0 == result.exit_code

            after_metadata = (Path(dataset.path) / client.METADATA).read_text()
            assert 'creator:' in after_metadata
            assert 'authors:' not in after_metadata


@pytest.mark.migration
def test_correct_relative_path(isolated_runner, old_project):
    """Check if path on dataset has been correctly migrated."""
    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project['path'])
    assert client.datasets

    for ds in client.datasets.values():
        assert not Path(ds.path).is_absolute()
        assert ds.path.startswith(RENKU_HOME)


@pytest.mark.migration
def test_remove_committed_lock_file(isolated_runner, old_project):
    """Check that renku lock file has been successfully removed from git."""
    repo = old_project['repo']
    repo_path = Path(old_project['path'])
    with open(str(repo_path / '.renku.lock'), 'w') as f:
        f.write('lock')

    repo.index.add(['.renku.lock'])
    repo.index.commit('locked')

    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code

    assert (repo_path / '.renku.lock').exists() is False
    assert repo.is_dirty() is False

    ignored = (repo_path / '.gitignore').read_text()
    assert '.renku.lock' in ignored


@pytest.mark.migration
def test_graph_building_after_migration(isolated_runner, old_project):
    """Check that structural migration did not break graph building."""
    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code

    result = isolated_runner.invoke(cli, ['log'])
    assert 0 == result.exit_code


@pytest.mark.migration
def test_migrations_run_once(isolated_runner, old_project):
    """Check that migration commit is not empty."""
    result = isolated_runner.invoke(cli, ['dataset'])
    assert old_project['exit_code'] == result.exit_code

    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 0 == result.exit_code

    result = isolated_runner.invoke(cli, ['migrate', 'datasets'])
    assert 1 == result.exit_code


@pytest.mark.migration
def test_migration_broken_urls(dataset_metadata):
    """Check that migration of broken dataset file URLs is string."""
    dataset = Dataset.from_jsonld(
        dataset_metadata,
        client=LocalClient('.'),
    )

    for file_ in dataset.files:
        assert isinstance(url_to_string(file_.url), str)
