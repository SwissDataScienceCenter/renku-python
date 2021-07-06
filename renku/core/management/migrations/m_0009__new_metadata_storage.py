# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Migrate old metadata to new storage format."""

import os
import shutil
import traceback
import uuid
from pathlib import Path
from typing import List

from git import NULL_TREE, Commit, GitCommandError

import renku.core.management.migrate
from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.metadata import initialize_database
from renku.core.management.repository import RepositoryApiMixin
from renku.core.metadata.database import Database
from renku.core.models.dataset import DatasetsProvenance
from renku.core.models.jsonld import load_yaml
from renku.core.models.project import Project
from renku.core.models.provenance.activities import Activity
from renku.core.models.provenance.activity import ActivityCollection
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.utils import communication
from renku.core.utils.metadata import convert_dataset
from renku.core.utils.migrate import MigrationType, read_project_version_from_yaml
from renku.core.utils.scm import git_unicode_unescape

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / RepositoryApiMixin.DATABASE_PATH,
    Path(RENKU_HOME) / RepositoryApiMixin.DEPENDENCY_GRAPH,
    Path(RENKU_HOME) / RepositoryApiMixin.PROVENANCE_GRAPH,
    Path(RENKU_HOME) / DatasetsApiMixin.DATASETS_PROVENANCE,
]


def migrate(client):
    """Migration function."""
    committed = _commit_previous_changes(client)
    # TODO: set remove=True once the migration to the new metadata is finalized
    generate_new_metadata(remove=False, committed=committed)
    _remove_dataset_metadata_files(client)
    _migrate_project(client)


def _commit_previous_changes(client):
    client.repo.git.add(str(client.renku_path))

    staged_files = client.repo.index.diff("HEAD")
    if staged_files:
        client._project = None  # NOTE: force reloading of project metadata
        client.project.version = "8"
        client.project.to_yaml()

        client.repo.git.add(str(client.renku_path))

        client.repo.index.commit("renku migrate: committing structural changes", skip_hooks=True)
        return True

    return False


@inject.autoparams()
def generate_new_metadata(client: LocalClient, database: Database, force=True, remove=True, committed=False):
    """Generate graph and dataset provenance metadata."""
    if force:
        client.remove_graph_files()
    elif client.has_graph_files():
        raise errors.OperationError("Graph metadata exists.")

    initialize_database(database)

    dependency_graph = DependencyGraph.from_database(database)
    datasets_provenance = DatasetsProvenance(database)

    commits = list(
        client.repo.iter_commits(paths=[f"{client.workflow_path}/*.yaml", ".renku/datasets/*/*.yml"], reverse=True)
    )
    n_commits = len(commits)

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"Processing commits {n}/{n_commits} {commit.hexsha}", end="\n")

        # NOTE: Treat the last commit differently if it was done by this migration
        is_last_commit = committed and n == n_commits

        try:
            # NOTE: Don't migrate workflows for dataset-only migrations
            if MigrationType.WORKFLOWS in client.migration_type:
                _process_workflows(
                    client=client, commit=commit, database=database, dependency_graph=dependency_graph, remove=remove
                )
            _process_datasets(
                client=client, commit=commit, datasets_provenance=datasets_provenance, is_last_commit=is_last_commit
            )
        except errors.MigrationError:
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Migration failed: {traceback.format_exc()}")
        except Exception:
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Exception: {traceback.format_exc()}")

        # NOTE: Commit changes after each step
        database.commit()


def _process_workflows(
    client: LocalClient, commit: Commit, database: Database, dependency_graph: DependencyGraph, remove: bool
):
    for file_ in commit.diff(commit.parents or NULL_TREE, paths=f"{client.workflow_path}/*.yaml"):
        # Ignore deleted files (they appear as ADDED in this backwards diff)
        if file_.change_type == "A":
            continue

        path: str = git_unicode_unescape(file_.a_path)

        if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
            continue

        if not (client.path / path).exists():
            communication.warn(f"Workflow file does not exists: '{path}'")
            continue

        workflow = Activity.from_yaml(path=path, client=client)

        activity_collection = ActivityCollection.from_activity(workflow, dependency_graph)

        for activity in activity_collection.activities:
            database["activities"].add(activity)
            # database["plans"].add(activity.association.plan)  # TODO: Plan should be added by dependency_graph

        if remove:
            try:
                os.remove(file_.a_path)
            except FileNotFoundError:
                pass


def _process_datasets(client: LocalClient, commit: Commit, datasets_provenance: DatasetsProvenance, is_last_commit):
    files_diff = list(commit.diff(commit.parents or NULL_TREE, paths=".renku/datasets/*/*.yml"))
    paths = [git_unicode_unescape(f.a_path) for f in files_diff]
    paths = [p for p in paths if len(Path(p).parents) == 4]  # Exclude files that are not in the right place
    deleted_paths = [git_unicode_unescape(f.a_path) for f in files_diff if f.change_type == "A"]

    datasets, deleted_datasets = _fetch_datasets(
        client=client, revision=commit.hexsha, paths=paths, deleted_paths=deleted_paths
    )

    revision = commit.hexsha
    date = commit.authored_datetime

    for dataset in datasets:
        dataset = convert_dataset(dataset=dataset, client=client, revision=revision)
        if is_last_commit:
            datasets_provenance.add_or_replace(dataset, date=date)
        else:
            datasets_provenance.add_or_update(dataset, revision=revision, date=date)
    for dataset in deleted_datasets:
        dataset = convert_dataset(dataset=dataset, client=client, revision=revision)
        datasets_provenance.remove(dataset, revision=revision, date=date)


def _fetch_datasets(client: LocalClient, revision: str, paths: List[str], deleted_paths: List[str]):
    # TODO: Make Datasets as v9 models and use them here
    from renku.core.management.migrations.models.v9 import Dataset

    datasets_path = client.path / ".renku" / "tmp" / "datasets"
    shutil.rmtree(datasets_path, ignore_errors=True)
    datasets_path.mkdir(parents=True, exist_ok=True)

    client.dataset_migration_context = _DatasetMigrationContext(client=client, revision=revision)

    def read_project_version():
        """Read project version at revision."""
        try:
            project_file_content = client.repo.git.show(f"{revision}:.renku/metadata.yml")
        except GitCommandError:  # Project metadata file does not exist
            return 1

        try:
            yaml_data = load_yaml(project_file_content)
            return int(read_project_version_from_yaml(yaml_data))
        except ValueError:
            return 1

    def get_dataset_identifier(path):
        identifier = Path(path).parent.name
        try:
            identifier = str(uuid.UUID(identifier))
        except ValueError:
            identifier = identifier.rsplit("%2F", maxsplit=1)[-1]
            try:
                identifier = str(uuid.UUID(identifier))
            except ValueError:
                return
            else:
                return identifier
        else:
            return identifier

    def copy_and_migrate_datasets():
        existing = []
        deleted = []

        for path in paths:
            rev = revision
            if path in deleted_paths:
                rev = client.find_previous_commit(path, revision=f"{revision}~")
            identifier = get_dataset_identifier(path)
            if not identifier:
                continue
            new_path = datasets_path / identifier / "metadata.yml"
            new_path.parent.mkdir(parents=True, exist_ok=True)
            content = client.repo.git.show(f"{rev}:{path}")
            new_path.write_text(content)
            if path in deleted_paths:
                deleted.append(new_path)
            else:
                existing.append(new_path)

        try:
            project_version = read_project_version()
            client.set_temporary_datasets_path(datasets_path)
            communication.disable()
            client.migration_type = MigrationType.DATASETS
            renku.core.management.migrate.migrate(
                project_version=project_version,
                skip_template_update=True,
                skip_docker_update=True,
                max_version=8,
            )
        finally:
            communication.enable()
            client.clear_temporary_datasets_path()

        return existing, deleted

    paths, deleted_paths = copy_and_migrate_datasets()

    datasets = []
    for metadata_path in paths:
        dataset = Dataset.from_yaml(metadata_path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        datasets.append(dataset)

    deleted_datasets = []
    for metadata_path in deleted_paths:
        dataset = Dataset.from_yaml(metadata_path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        deleted_datasets.append(dataset)

    return datasets, deleted_datasets


class _DatasetMigrationContext:
    def __init__(self, client, revision):
        self.client = client
        self.revision = revision

    def exists(self, path) -> bool:
        try:
            self.client.repo.git.cat_file("-e", f"{self.revision}:{str(path)}")
        except GitCommandError:
            return False
        else:
            return True

    def is_dir(self, path) -> bool:
        try:
            result = self.client.repo.git.cat_file("-t", f"{self.revision}:{str(path)}")
        except GitCommandError:
            return False
        else:
            return "tree" in result

    def find_previous_commit(self, path):
        return self.client.find_previous_commit(path, revision=self.revision)


def _remove_dataset_metadata_files(client: LocalClient):
    """Remove old dataset metadata."""
    try:
        shutil.rmtree(os.path.join(client.renku_path, client.DATASETS))
    except FileNotFoundError:
        pass
    try:
        shutil.rmtree(os.path.join(client.renku_path, "refs", client.DATASETS))
    except FileNotFoundError:
        pass


def _migrate_project(client: LocalClient):
    """Create new project metadata."""
    database = Database.from_path(client.database_path)
    project = Project.from_project(client.project)
    project.version = 9
    database.add(project)
    database.commit()
