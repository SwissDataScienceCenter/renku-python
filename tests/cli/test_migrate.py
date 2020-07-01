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
from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION, \
    get_migrations
from renku.core.models.datasets import Dataset
from renku.core.utils.urls import url_to_string


@pytest.mark.migration
@pytest.mark.parametrize(
    'command',
    [['config', 'key', 'value'], ['dataset', 'create', 'new'],
     ['dataset', 'add', 'new', 'README.md'], ['dataset', 'edit', 'new'],
     ['dataset', 'unlink', 'new'], ['dataset', 'rm', 'new'],
     ['dataset', 'update'], ['dataset', 'export', 'new', 'zenodo'],
     ['dataset', 'import', 'uri'], ['dataset', 'rm-tags', 'news'], ['log'],
     ['mv', 'news'], ['rerun', 'data'], ['run', 'echo'], ['show', 'inputs'],
     ['show', 'outputs'], ['show', 'siblings'], ['status'], ['update'],
     ['workflow']]
)
def test_commands_fail_on_old_repository(
    isolated_runner, old_repository_with_submodules, command
):
    """Test commands that fail on projects created by old version of renku."""
    runner = isolated_runner
    result = runner.invoke(cli, command)
    assert 1 == result.exit_code, result.output
    output = result.output
    assert 'Project version is outdated and a migration is required' in output


@pytest.mark.migration
@pytest.mark.parametrize(
    'command',
    [['clone', 'uri'], ['config', 'key'], ['dataset'], ['dataset', 'ls-files'],
     ['dataset', 'ls-tags', 'new'], ['doctor'], ['githooks', 'install'],
     ['help'], ['init'], ['storage', 'check']]
)
def test_commands_work_on_old_repository(
    isolated_runner, old_repository_with_submodules, command
):
    """Test commands that do not require migration."""
    runner = isolated_runner
    result = runner.invoke(cli, command)
    assert 'a migration is required' not in result.output


@pytest.mark.migration
def test_migrate_datasets_with_old_repository(isolated_runner, old_project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code
    assert not old_project['repo'].is_dirty()


@pytest.mark.migration
def test_correct_path_migrated(isolated_runner, old_project):
    """Check if path on dataset files has been correctly migrated."""
    result = isolated_runner.invoke(cli, ['migrate'])
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
    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project['path'])
    for dataset in client.datasets.values():
        after_metadata = (Path(dataset.path) / client.METADATA).read_text()
        assert 'creator:' in after_metadata
        assert 'authors:' not in after_metadata


@pytest.mark.migration
def test_correct_relative_path(isolated_runner, old_project):
    """Check if path on dataset has been correctly migrated."""
    result = isolated_runner.invoke(cli, ['migrate'])
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

    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code

    assert (repo_path / '.renku.lock').exists() is False
    assert repo.is_dirty() is False

    ignored = (repo_path / '.gitignore').read_text()
    assert '.renku.lock' in ignored


@pytest.mark.migration
def test_graph_building_after_migration(isolated_runner, old_project):
    """Check that structural migration did not break graph building."""
    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code

    result = isolated_runner.invoke(cli, ['log'])
    assert 0 == result.exit_code


@pytest.mark.migration
def test_migrations_runs(isolated_runner, old_project):
    """Check that migration can be run more than once."""
    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code
    assert 'Successfully applied' in result.output
    assert 'OK' in result.output

    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code
    assert 'No migrations required.' in result.output


@pytest.mark.migration
def test_migration_broken_urls(dataset_metadata):
    """Check that migration of broken dataset file URLs is string."""
    dataset = Dataset.from_jsonld(
        dataset_metadata,
        client=LocalClient('.'),
    )

    for file_ in dataset.files:
        assert isinstance(url_to_string(file_.url), str)


@pytest.mark.migration
def test_migration_version():
    """Test migrations and project version match."""
    migrations = get_migrations()
    max_migration_version = max([m[0] for m in migrations])

    assert max_migration_version == SUPPORTED_PROJECT_VERSION


@pytest.mark.migration
def test_migrations_no_commit(isolated_runner, old_project):
    """Check --no-commit flag doesn't commit changes."""
    client = LocalClient(path=old_project['path'])
    sha_before = client.repo.head.object.hexsha

    result = isolated_runner.invoke(cli, ['migrate', '--no-commit'])
    assert 0 == result.exit_code
    assert 'OK' in result.output
    assert sha_before == client.repo.head.object.hexsha


@pytest.mark.migration
def test_workflow_migration(isolated_runner, old_workflow_project):
    """Check that *.cwl workflows can be migrated."""
    result = isolated_runner.invoke(cli, ['migrate'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    result = isolated_runner.invoke(
        cli, ['log', old_workflow_project['log_path']]
    )
    assert 0 == result.exit_code

    for expected in old_workflow_project['expected_strings']:
        assert expected in result.output
