# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
import json
import os
from pathlib import Path

import pytest

from renku import LocalClient
from renku.cli import cli
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION, get_migrations
from renku.core.models.dataset import RemoteEntity
from tests.utils import format_result_exception


@pytest.mark.migration
def test_migrate_datasets_with_old_repository(isolated_runner, old_project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not old_project.is_dirty()


@pytest.mark.migration
def test_migrate_project(isolated_runner, old_project, client_database_injection_manager):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert not old_project.is_dirty()

    client = LocalClient(path=old_project.working_dir)
    with client_database_injection_manager(client):
        assert client.project
        assert client.project.name


@pytest.mark.migration
@pytest.mark.serial
def test_migration_check(isolated_runner, project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrationscheck"])
    assert 0 == result.exit_code, format_result_exception(result)
    output = json.loads(result.output)
    assert output.keys() == {
        "latest_version",
        "project_version",
        "migration_required",
        "project_supported",
        "template_update_possible",
        "current_template_version",
        "latest_template_version",
        "template_source",
        "template_ref",
        "template_id",
        "automated_update",
        "docker_update_possible",
    }


@pytest.mark.migration
def test_correct_path_migrated(isolated_runner, old_project, client_database_injection_manager):
    """Check if path on dataset files has been correctly migrated."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    client = LocalClient(path=old_project.working_dir)
    with client_database_injection_manager(client):
        assert client.datasets

        for ds in client.datasets.values():
            for file in ds.files:
                path = Path(file.entity.path)
                assert path.exists()
                assert not path.is_absolute()
                assert file.id


@pytest.mark.migration
def test_correct_relative_path(isolated_runner, old_project, client_database_injection_manager):
    """Check if path on dataset has been correctly migrated."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    client = LocalClient(path=old_project.working_dir)

    with client_database_injection_manager(client):
        datasets_provenance = DatasetsProvenance()

        assert len(list(datasets_provenance.datasets)) > 0


@pytest.mark.migration
def test_remove_committed_lock_file(isolated_runner, old_project):
    """Check that renku lock file has been successfully removed from git."""
    repo = old_project
    repo_path = Path(old_project.working_dir)
    with open(str(repo_path / ".renku.lock"), "w") as f:
        f.write("lock")

    repo.index.add([".renku.lock"])
    repo.index.commit("locked")

    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    assert not (repo_path / ".renku.lock").exists()
    assert not repo.is_dirty()

    ignored = (repo_path / ".gitignore").read_text()
    assert ".renku.lock" in ignored


@pytest.mark.migration
def test_graph_building_after_migration(isolated_runner, old_project):
    """Check that structural migration did not break graph building."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = isolated_runner.invoke(cli, ["graph", "export", "--full"])
    assert 0 == result.exit_code, format_result_exception(result)


@pytest.mark.migration
def test_migrations_runs(isolated_runner, old_project):
    """Check that migration can be run more than once."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully applied" in result.output
    assert "OK" in result.output

    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "No migrations required." in result.output


@pytest.mark.migration
def test_migration_version():
    """Test migrations and project version match."""
    migrations = get_migrations()
    max_migration_version = max([m[0] for m in migrations])

    assert max_migration_version == SUPPORTED_PROJECT_VERSION


@pytest.mark.migration
def test_workflow_migration(isolated_runner, old_workflow_project):
    """Check that *.cwl workflows can be migrated."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    result = isolated_runner.invoke(cli, ["graph", "export", "--full"])
    assert 0 == result.exit_code, format_result_exception(result)

    for expected in old_workflow_project["expected_strings"]:
        assert expected in result.output


@pytest.mark.migration
def test_comprehensive_dataset_migration(
    isolated_runner, old_dataset_project, load_dataset_with_injection, get_datasets_provenance_with_injection
):
    """Test migration of old project with all dataset variations."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "OK" in result.output

    client = old_dataset_project
    dataset = load_dataset_with_injection("dataverse", client)
    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        tags = datasets_provenance.get_all_tags(dataset)

    assert "/datasets/1d2ed1e43aeb4f2590b238084ee3d86c" == dataset.id
    assert "1d2ed1e43aeb4f2590b238084ee3d86c" == dataset.identifier
    assert "Cornell University" == dataset.creators[0].affiliation
    assert "Rooth, Mats" == dataset.creators[0].name
    assert dataset.date_published is None
    assert "2020-08-10T21:35:05.115412+00:00" == dataset.date_created.isoformat("T")
    assert "Replication material for a paper to be presented" in dataset.description
    assert "https://doi.org/10.7910/DVN/EV6KLF" == dataset.same_as.url
    assert "1" == tags[0].name
    assert "Tag 1 created by renku import" == tags[0].description
    assert isinstance(dataset.license, str)
    assert "https://creativecommons.org/publicdomain/zero/1.0/" in str(dataset.license)

    file_ = dataset.find_file("data/dataverse/copy.sh")
    assert "https://dataverse.harvard.edu/api/access/datafile/3050656" == file_.source
    assert "2020-08-10T21:35:10.877832+00:00" == file_.date_added.isoformat("T")
    assert file_.based_on is None
    assert not hasattr(file_, "creators")

    dataset = load_dataset_with_injection("mixed", client)
    with get_datasets_provenance_with_injection(client) as datasets_provenance:
        tags = datasets_provenance.get_all_tags(dataset)
    assert "v1" == tags[0].name

    file_ = dataset.find_file("data/mixed/Makefile")
    assert file_.entity.id.endswith("/data/mixed/Makefile")
    assert "https://github.com/SwissDataScienceCenter/renku-jupyter.git" == file_.source
    assert isinstance(file_.based_on, RemoteEntity)
    assert file_.source == file_.based_on.url
    assert "Makefile" == file_.based_on.path
    assert "49f331d7388785208ccfb3cfb9156b226d9b59ea" == file_.based_on.commit_sha

    file_ = dataset.find_file("data/mixed/data.txt")
    assert file_.entity.id.endswith("/data/mixed/data.txt")
    assert "../../../../tmp/data.txt" == file_.source
    assert file_.based_on is None

    file_ = dataset.find_file("README.md")
    assert file_.entity.id.endswith("/README.md")
    assert "README.md" == file_.source
    assert file_.based_on is None


@pytest.mark.migration
def test_migrate_renku_dataset_same_as(isolated_runner, old_client_before_database, load_dataset_with_injection):
    """Test migration of imported renku datasets remove dashes from the same_as field."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = load_dataset_with_injection("renku-dataset", old_client_before_database)

    assert "https://dev.renku.ch/datasets/860f6b5b46364c83b6a9b38ef198bcc0" == dataset.same_as.value


@pytest.mark.migration
def test_migrate_renku_dataset_derived_from(isolated_runner, old_client_before_database, load_dataset_with_injection):
    """Test migration of datasets remove dashes from the derived_from field."""
    result = isolated_runner.invoke(cli, ["migrate", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)

    dataset = load_dataset_with_injection("local", old_client_before_database)

    assert "/datasets/535b6e1ddb85442a897b2b3c72aec0c6" == dataset.derived_from.url_id


@pytest.mark.migration
def test_no_blank_node_after_dataset_migration(isolated_runner, old_dataset_project, load_dataset_with_injection):
    """Test migration of datasets with blank nodes creates IRI identifiers."""
    assert 0 == isolated_runner.invoke(cli, ["migrate", "--strict"]).exit_code

    dataset = load_dataset_with_injection("2019-01_us_fligh_1", old_dataset_project)

    assert not dataset.creators[0].id.startswith("_:")
    assert not dataset.same_as.id.startswith("_:")
    assert isinstance(dataset.license, str)


@pytest.mark.migration
def test_migrate_non_renku_repository(isolated_runner):
    """Test migration prints proper message when run on non-renku repo."""
    from git import Repo

    Repo.init(".")
    os.mkdir(".renku")

    result = isolated_runner.invoke(cli, ["migrate", "--strict"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Error: Not a renku project." in result.output


@pytest.mark.migration
def test_migrate_check_on_old_project(isolated_runner, old_repository_with_submodules):
    """Test migration check on an old project."""
    result = isolated_runner.invoke(cli, ["migrate", "--check"])

    assert 3 == result.exit_code
    assert "Project version is outdated and a migration is required" in result.output


@pytest.mark.migration
def test_migrate_check_on_unsupported_project(isolated_runner, unsupported_project):
    """Test migration check on an unsupported project."""
    result = isolated_runner.invoke(cli, ["migrate", "--check"])

    assert 4 == result.exit_code
    assert "Project is not supported by this version of Renku." in result.output


@pytest.mark.migration
def test_migrate_check_on_non_renku_repository(isolated_runner):
    """Test migration check on non-renku repo."""
    from git import Repo

    Repo.init(".")
    os.mkdir(".renku")

    result = isolated_runner.invoke(cli, ["migrate", "--check"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Error: Not a renku project." in result.output


@pytest.mark.migration
@pytest.mark.parametrize(
    "command",
    [
        ["config", "set", "key", "value"],
        ["dataset", "add", "new", "README.md"],
        ["dataset", "create", "new"],
        ["dataset", "edit", "new"],
        ["dataset", "export", "new", "zenodo"],
        ["dataset", "import", "uri"],
        ["dataset", "ls"],
        ["dataset", "ls-files"],
        ["dataset", "ls-tags", "new"],
        ["dataset", "rm", "new"],
        ["dataset", "rm-tags", "news"],
        ["dataset", "show", "new"],
        ["dataset", "unlink", "new"],
        ["dataset", "update"],
        ["graph", "export"],
        ["mv", "news"],
        ["rerun", "data"],
        ["run", "echo"],
        ["status"],
        ["update", "--all"],
        ["workflow", "ls"],
        ["workflow", "inputs"],
        ["workflow", "outputs"],
    ],
)
def test_commands_fail_on_old_repository(isolated_runner, old_repository_with_submodules, command):
    """Test commands that fail on projects created by old version of renku."""
    result = isolated_runner.invoke(cli, command)
    assert 3 == result.exit_code, format_result_exception(result)
    assert "Project version is outdated and a migration is required" in result.output


@pytest.mark.migration
@pytest.mark.parametrize(
    "command",
    [
        ["clone", "uri"],
        ["config", "show", "key"],
        ["dataset"],
        ["doctor"],
        ["githooks", "install"],
        ["help"],
        ["init", "-i", "1", "--force"],
        ["storage", "check"],
    ],
)
def test_commands_work_on_old_repository(isolated_runner, old_repository_with_submodules, command):
    """Test commands that do not require migration."""
    result = isolated_runner.invoke(cli, command)
    assert "Project version is outdated and a migration is required" not in result.output


def test_commit_hook_with_immutable_modified_files(runner, local_client, mocker, template_update):
    """Test repository update from a template with modified local immutable files."""
    from renku.core.utils.contexts import chdir

    template_update(immutable_files=["README.md"])

    with chdir(local_client.path):
        result = runner.invoke(cli, ["check-immutable-template-files", "Dockerfile"])
        assert result.exit_code == 0

        result = runner.invoke(cli, ["check-immutable-template-files", "README.md"])
        assert result.exit_code == 1
        assert "README.md" in result.output
