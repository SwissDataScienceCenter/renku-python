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
"""Migrate datasets based on Git submodules."""
import glob
import os
import shutil
from pathlib import Path

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.management.client import LocalClient
from renku.core.migration.m_0009__new_metadata_storage import _fetch_datasets
from renku.core.migration.models.v3 import DatasetFileSchemaV3, get_client_datasets
from renku.core.migration.models.v9 import DatasetFile, OldDatasetFileSchema, generate_file_id, generate_label
from renku.core.project.project_properties import project_properties
from renku.core.util.urls import remove_credentials
from renku.infrastructure.repository import Repository


def migrate(migration_context):
    """Migration function."""
    _migrate_submodule_based_datasets(migration_context.client)


@inject.autoparams()
def _migrate_submodule_based_datasets(client, client_dispatcher: IClientDispatcher):
    from renku.core.management.migrate import is_project_unsupported, migrate

    submodules = project_properties.repository.submodules
    if len(submodules) == 0:
        return

    repo_paths = []
    symlinks = []

    submodules.update()

    for dataset in get_client_datasets(client):
        for file_ in dataset.files:
            path = project_properties.path / file_.path
            if not path.is_symlink():
                continue

            target = path.resolve()

            if "/.renku/vendors/" not in str(target):
                continue

            repo_path = Repository(path=target.parent, search_parent_directories=True).path
            if repo_path not in repo_paths:
                repo_paths.append(repo_path)

            symlinks.append((file_.path, target, repo_path))

    if not symlinks:
        return

    submodules_urls = {s.relative_path: s.url for s in submodules}

    remote_clients = dict()
    for repo_path in repo_paths:
        with project_properties.with_path(repo_path, save_changes=True):
            remote_client = LocalClient()
            remote_clients[repo_path] = remote_client
            client_dispatcher.push_created_client_to_stack(remote_client)

            try:
                if not is_project_unsupported():
                    migrate(skip_template_update=True, skip_docker_update=True)
            finally:
                client_dispatcher.pop_client()

    metadata = {}

    for path, target, repo_path in symlinks:
        remote_client = remote_clients[repo_path]
        path_within_repo = target.relative_to(repo_path)

        repo_is_remote = ".renku/vendors/local" not in str(repo_path)
        based_on = None
        submodule_path = repo_path.relative_to(project_properties.path)

        url = submodules_urls.get(submodule_path, "")

        if repo_is_remote:
            with project_properties.with_path(repo_path):
                based_on = _fetch_file_metadata(remote_client, path_within_repo)
                if based_on:
                    based_on.url = url
                    based_on.based_on = None
                else:
                    based_on = DatasetFile.from_revision(remote_client, path=path_within_repo, url=url)
                data = OldDatasetFileSchema(client=remote_client).dump(based_on)
                based_on = DatasetFileSchemaV3(client=remote_client).load(data)
        else:
            if url:
                full_path = Path(url) / path_within_repo
                rel_path = os.path.relpath(full_path, project_properties.path)
                url = f"file://{rel_path}"

        metadata[path] = (based_on, url)

        path = project_properties.path / path
        path.unlink()

        try:
            shutil.move(target, path)
        except FileNotFoundError:
            raise errors.InvalidFileOperation(f"File was not found: {target}")

    repository = project_properties.repository

    for submodule in submodules:
        if str(submodule.relative_path).startswith(".renku/vendors/"):
            try:
                repository.submodules.remove(submodule, force=True)
            except errors.GitError:
                pass

    for dataset in get_client_datasets(client):
        for file_ in dataset.files:
            if file_.path in metadata:
                based_on, url = metadata[file_.path]
                file_.based_on = based_on
                file_.url = remove_credentials(url)
                file_.commit = repository.get_previous_commit(file_.path)
                file_._id = generate_file_id(client, hexsha=file_.commit.hexsha, path=file_.path)
                file_._label = generate_label(file_.path, file_.commit.hexsha)

        dataset.to_yaml()


def _fetch_file_metadata(client, path):
    """Return metadata for a single file."""
    paths = glob.glob(f"{project_properties.path}/.renku/datasets/*/*.yml" "")
    for dataset in _fetch_datasets(client, project_properties.repository.head.commit.hexsha, paths, [])[0]:
        for file in dataset.files:
            if file.entity.path == path:
                return file
