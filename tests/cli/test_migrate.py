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
from renku.core.management.config import RENKU_HOME
from renku.core.management.migrate import SUPPORTED_PROJECT_VERSION, get_migrations


@pytest.mark.migration
def test_migrate_datasets_with_old_repository(isolated_runner, old_project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code
    assert not old_project.is_dirty()


@pytest.mark.migration
def test_migrate_project(isolated_runner, old_project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code
    assert not old_project.is_dirty()

    client = LocalClient(path=old_project.working_dir)
    assert client.project
    assert client.project.name


@pytest.mark.migration
@pytest.mark.parametrize("old_project", ["old-workflows-v0.10.0.git"], indirect=["old_project"])
def test_migrate_duplicated_input_binding(isolated_runner, old_project):
    """Check migrating CWLs with multiple outputs binding to the same input."""
    result = isolated_runner.invoke(cli, ["migrate", "--no-commit"])

    assert 0 == result.exit_code, result.output


@pytest.mark.migration
@pytest.mark.serial
def test_migration_check(isolated_runner, project):
    """Test migrate on old repository."""
    result = isolated_runner.invoke(cli, ["migrationscheck"])
    assert 0 == result.exit_code
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
def test_correct_path_migrated(isolated_runner, old_project):
    """Check if path on dataset files has been correctly migrated."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project.working_dir)
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
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project.working_dir)
    for dataset in client.datasets.values():
        after_metadata = (Path(dataset.path) / client.METADATA).read_text()
        assert "creator:" in after_metadata
        assert "authors:" not in after_metadata


@pytest.mark.migration
def test_correct_relative_path(isolated_runner, old_project):
    """Check if path on dataset has been correctly migrated."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code

    client = LocalClient(path=old_project.working_dir)
    assert client.datasets

    for ds in client.datasets.values():
        assert not Path(ds.path).is_absolute()
        assert ds.path.startswith(RENKU_HOME)


@pytest.mark.migration
def test_remove_committed_lock_file(isolated_runner, old_project):
    """Check that renku lock file has been successfully removed from git."""
    repo = old_project
    repo_path = Path(old_project.working_dir)
    with open(str(repo_path / ".renku.lock"), "w") as f:
        f.write("lock")

    repo.index.add([".renku.lock"])
    repo.index.commit("locked")

    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code

    assert not (repo_path / ".renku.lock").exists()
    assert not repo.is_dirty()

    ignored = (repo_path / ".gitignore").read_text()
    assert ".renku.lock" in ignored


@pytest.mark.migration
def test_graph_building_after_migration(isolated_runner, old_project):
    """Check that structural migration did not break graph building."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code

    result = isolated_runner.invoke(cli, ["log"])
    assert 0 == result.exit_code


@pytest.mark.migration
def test_migrations_runs(isolated_runner, old_project):
    """Check that migration can be run more than once."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code
    assert "Successfully applied" in result.output
    assert "OK" in result.output

    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code
    assert "No migrations required." in result.output


@pytest.mark.migration
def test_migration_version():
    """Test migrations and project version match."""
    migrations = get_migrations()
    max_migration_version = max([m[0] for m in migrations])

    assert max_migration_version == SUPPORTED_PROJECT_VERSION


@pytest.mark.migration
def test_migrations_no_commit(isolated_runner, old_project):
    """Check --no-commit flag doesn't commit changes."""
    client = LocalClient(path=old_project.working_dir)
    sha_before = client.repo.head.object.hexsha

    result = isolated_runner.invoke(cli, ["migrate", "--no-commit"])
    assert 0 == result.exit_code
    assert "OK" in result.output
    assert sha_before == client.repo.head.object.hexsha


@pytest.mark.migration
def test_workflow_migration(isolated_runner, old_workflow_project):
    """Check that *.cwl workflows can be migrated."""
    result = isolated_runner.invoke(cli, ["migrate"])

    assert 0 == result.exit_code
    assert "OK" in result.output

    result = isolated_runner.invoke(cli, ["log", old_workflow_project["log_path"]])
    assert 0 == result.exit_code

    for expected in old_workflow_project["expected_strings"]:
        assert expected in result.output


@pytest.mark.migration
def test_comprehensive_dataset_migration(isolated_runner, old_dataset_project):
    """Test migration of old project with all dataset variations."""
    result = isolated_runner.invoke(cli, ["migrate"])
    assert 0 == result.exit_code
    assert "OK" in result.output

    client = old_dataset_project

    dataset = client.load_dataset("dataverse")
    assert dataset._id.endswith("/datasets/1d2ed1e4-3aeb-4f25-90b2-38084ee3d86c")
    assert "1d2ed1e4-3aeb-4f25-90b2-38084ee3d86c" == dataset.identifier
    assert "1d2ed1e4-3aeb-4f25-90b2-38084ee3d86c" == dataset._label
    assert "Cornell University" == dataset.creators[0].affiliation
    assert "Rooth, Mats" == dataset.creators[0].name
    assert "Rooth, Mats" == dataset.creators[0].label
    assert dataset.date_published is None
    assert "2020-08-10T21:35:05.115412+00:00" == dataset.date_created.isoformat("T")
    assert "Replication material for a paper to be presented" in dataset.description
    assert "https://doi.org/10.7910/DVN/EV6KLF" == dataset.same_as.url
    assert "1" == dataset.tags[0].name
    assert "Tag 1 created by renku import" == dataset.tags[0].description
    assert isinstance(dataset.license, str)
    assert "https://creativecommons.org/publicdomain/zero/1.0/" in str(dataset.license)

    file_ = dataset.find_file("data/dataverse/copy.sh")
    assert "https://dataverse.harvard.edu/api/access/datafile/3050656" == file_.source
    assert file_.url.endswith("/projects/mohammad.alisafaee/old-datasets-v0.9.1/files/blob/data/dataverse/copy.sh")
    assert "2020-08-10T21:35:10.877832+00:00" == file_.added.isoformat("T")
    assert file_.based_on is None
    assert not hasattr(file_, "creators")

    dataset = client.load_dataset("mixed")
    assert "v1" == dataset.tags[0].name

    file_ = dataset.find_file("data/mixed/Makefile")
    assert file_._id.endswith("/blob/a5f6c3700616e005ac599d24feb7a770430bd93a/data/mixed/Makefile")
    assert "https://github.com/SwissDataScienceCenter/renku-jupyter.git" == file_.source
    assert file_.source == file_.based_on.source
    assert file_.source == file_.based_on.url
    assert "Makefile@49f331d7388785208ccfb3cfb9156b226d9b59ea" == file_.based_on._label
    assert file_.based_on.based_on is None
    assert file_.url.endswith("/projects/mohammad.alisafaee/old-datasets-v0.9.1/files/blob/data/mixed/Makefile")

    file_ = dataset.find_file("data/mixed/data.txt")
    assert file_._id.endswith("/blob/b32138c1bcb2b53da974bbeb842f4d621e155355/data/mixed/data.txt")
    assert "../../../../tmp/data.txt" == file_.source
    assert file_.based_on is None
    assert file_.url.endswith("/projects/mohammad.alisafaee/old-datasets-v0.9.1/files/blob/data/mixed/data.txt")

    file_ = dataset.find_file("README.md")
    assert file_._id.endswith("/blob/0bfb07be3b538e6683e1d2055b5ae4d3a4c593dd/README.md")
    assert "README.md" == file_.source
    assert file_.based_on is None
    assert file_.url.endswith("/projects/mohammad.alisafaee/old-datasets-v0.9.1/files/blob/README.md")


@pytest.mark.migration
def test_no_blank_node_after_dataset_migration(isolated_runner, old_dataset_project):
    """Test migration of datasets with blank nodes creates IRI identifiers."""
    assert 0 == isolated_runner.invoke(cli, ["migrate"]).exit_code

    dataset = old_dataset_project.load_dataset("201901_us_flights_1")

    assert not dataset.creators[0]._id.startswith("_:")
    assert not dataset.same_as._id.startswith("_:")
    assert not dataset.tags[0]._id.startswith("_:")
    assert isinstance(dataset.license, str)


@pytest.mark.migration
def test_migrate_non_renku_repository(isolated_runner):
    """Test migration prints proper message when run on non-renku repo."""
    from git import Repo

    Repo.init(".")
    os.mkdir(".renku")

    result = isolated_runner.invoke(cli, ["migrate"])

    assert 0 == result.exit_code, result.output
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

    assert 0 == result.exit_code, result.output
    assert "Error: Not a renku project." in result.output


@pytest.mark.migration
@pytest.mark.parametrize(
    "command",
    [
        ["config", "set", "key", "value"],
        ["dataset", "create", "new"],
        ["dataset", "add", "new", "README.md"],
        ["dataset", "edit", "new"],
        ["dataset", "unlink", "new"],
        ["dataset", "rm", "new"],
        ["dataset", "update"],
        ["dataset", "export", "new", "zenodo"],
        ["dataset", "import", "uri"],
        ["dataset", "rm-tags", "news"],
        ["log"],
        ["mv", "news"],
        ["rerun", "data"],
        ["run", "echo"],
        ["show", "inputs"],
        ["show", "outputs"],
        ["show", "siblings"],
        ["status"],
        ["update", "--all"],
        ["workflow", "ls"],
    ],
)
def test_commands_fail_on_old_repository(isolated_runner, old_repository_with_submodules, command):
    """Test commands that fail on projects created by old version of renku."""
    result = isolated_runner.invoke(cli, command)
    assert 3 == result.exit_code, result.output
    assert "Project version is outdated and a migration is required" in result.output


@pytest.mark.migration
@pytest.mark.parametrize(
    "command",
    [
        ["clone", "uri"],
        ["config", "show", "key"],
        ["dataset"],
        ["dataset", "ls-files"],
        ["dataset", "ls-tags", "new"],
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
