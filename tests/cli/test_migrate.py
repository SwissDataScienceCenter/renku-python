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
    'command', [
        'config', 'dataset', 'log', 'mv', 'rerun', 'run', 'show', 'status',
        'update', 'workflow'
    ]
)
def test_commands_fail_on_old_repository(
    isolated_runner, old_project, command
):
    """Test commands that fail on projects created by old version of renku."""
    runner = isolated_runner
    result = runner.invoke(cli, [command])
    assert 1 == result.exit_code
    output = result.output
    assert 'Project version is outdated and a migration is required' in output


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
def test_calamus(isolated_runner, repository_before_calamus):
    """Check Calamus loads project correctly."""
    from datetime import datetime

    def str2date(s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f%z')

    assert 0 == isolated_runner.invoke(cli, ['migrate']).exit_code

    project_path = repository_before_calamus.working_dir
    client = LocalClient(path=project_path)

    project = client.project
    assert 'old-datasets-v0.10.4' == project.name
    assert str2date('2020-06-12T12:30:56.621721+00:00') == project.created
    assert str2date('2020-06-12T12:30:56.646982+00:00') == project.updated
    assert 'mailto:mohammad.alisafaee@' in project.creator._id

    dataverse = client.load_dataset('dataverse')
    assert 'Open Source at Harvard' == dataverse.name
    assert 'ecad371b-d03e-4609-a5e9-3774004aaece' == dataverse.identifier
    assert 'Harvard University' == dataverse.creator[0].affiliation
    assert str2date('2020-06-12T12:31:05.161363+00:00') == dataverse.created
    assert dataverse.date_published is None
    assert 'The tabular file contains information' in dataverse.description
    assert 'https://doi.org/10.7910/DVN/TJCLKP' == dataverse.same_as.url
    assert '3' == dataverse.tags[0].name

    file_ = dataverse.find_file('data/dataverse/IQSS-UNF.json')
    assert (
        'https://dataverse.harvard.edu/api/access/datafile/3371500' ==
        file_.url
    )
    assert str2date('2020-06-12T12:33:51.828279+00:00') == file_.added

    git = client.load_dataset('git')
    assert 'git' == git.name
    assert 'git' == git.short_name
    assert '22343ecc-329d-4f14-95c5-32c88476f60a' == git.identifier
    assert '.renku/datasets/22343ecc-329d-4f14-95c5-32c88476f60a' == git.path
    assert (
        'https://localhost/datasets/22343ecc-329d-4f14-95c5-32c88476f60a' ==
        git.url
    )
    assert git.same_as is None
    assert git.license is None
    assert 0 == len(git.tags)
    assert 'mailto:mohammad.alisafaee@' in git.creator[0]._id
    assert str2date('2020-06-12T12:36:30.836352+00:00') == git.created
    assert (
        'https://localhost/datasets/22343ecc-329d-4f14-95c5-32c88476f60a' ==
        git._id
    )

    file_ = git.files[0]
    assert (
        'https://github.com/SwissDataScienceCenter/r10e-ds-py.git' ==
        file_.based_on.url
    )
    assert (
        'notebooks/index.ipynb@f98325d81c700f4b86ee05c2154e94d43ca068b8' ==
        file_.based_on._label
    )
    assert file_.based_on.based_on is None
    assert 'mailto:cramakri@' in file_.based_on.creator[0]._id

    local = client.load_dataset('local')
    assert 'local' == local.name
    assert '.renku/datasets/61435c1f-bbc7-4dfb-a316-dc86b9c87758' == local.path

    file_ = local.find_file('data/local/data.txt')
    assert file_.external is True
    assert 'file://../../../../tmp/data.txt' == file_.url

    file_ = local.find_file('data/local/result.csv')
    assert file_.external is False
    assert 'file://../../../../tmp/result.csv' == file_.url
