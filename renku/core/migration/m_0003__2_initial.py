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
"""Initial migrations."""

import os
import shutil
import urllib
from pathlib import Path

from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.constant import RENKU_HOME
from renku.core.migration.models.refs import LinkReference
from renku.core.migration.models.v3 import Collection, Dataset, Project, get_project_datasets
from renku.core.migration.models.v9 import generate_file_id, generate_label
from renku.core.migration.utils import (
    OLD_METADATA_PATH,
    generate_dataset_id,
    get_datasets_path,
    get_pre_0_3_4_datasets_metadata,
    is_using_temporary_datasets_path,
)
from renku.core.util.contexts import with_project_metadata
from renku.core.util.git import get_in_submodules
from renku.core.util.urls import url_to_string
from renku.domain_model.dataset import generate_default_name
from renku.domain_model.project_context import project_context


def migrate(migration_context):
    """Migration function."""
    _ensure_clean_lock()
    _do_not_track_lock_file()
    _migrate_datasets_pre_v0_3()
    _migrate_broken_dataset_paths(migration_context=migration_context)
    _fix_labels_and_ids(migration_context)
    _fix_dataset_urls()
    _migrate_dataset_and_files_project()


def _ensure_clean_lock():
    """Make sure Renku lock file is not part of repository."""
    if is_using_temporary_datasets_path():
        return

    lock_file = project_context.path / ".renku.lock"
    try:
        lock_file.unlink()
    except FileNotFoundError:
        pass


def _do_not_track_lock_file():
    """Add lock file to .gitignore if not already exists."""
    if is_using_temporary_datasets_path():
        return

    lock_file = ".renku.lock"
    gitignore = project_context.path / ".gitignore"
    if not gitignore.exists() or lock_file not in gitignore.read_text():
        gitignore.open("a").write(f"\n{lock_file}\n")


def _migrate_datasets_pre_v0_3():
    """Migrate datasets from Renku 0.3.x."""
    if is_using_temporary_datasets_path():
        return

    changed = False
    repository = project_context.repository

    for old_path in get_pre_0_3_4_datasets_metadata():
        changed = True
        name = str(old_path.parent.relative_to(project_context.path / DATA_DIR))

        dataset = Dataset.from_yaml(old_path)
        dataset.title = name
        dataset.name = generate_default_name(name)
        new_path = get_datasets_path() / dataset.identifier / OLD_METADATA_PATH
        new_path.parent.mkdir(parents=True, exist_ok=True)

        with with_project_metadata(read_only=True) as meta:
            for submodule in repository.submodules:
                if Path(submodule.url).name == meta.name:
                    repository.submodules.remove(submodule)

        for file_ in dataset.files:
            if not Path(file_.path).exists():
                expected_path = project_context.path / DATA_DIR / dataset.name / file_.path
                if expected_path.exists():
                    file_.path = expected_path.relative_to(project_context.path)

        dataset.to_yaml(new_path)

        Path(old_path).unlink()
        ref = LinkReference.create(name="datasets/{0}".format(name), force=True)
        ref.set_reference(new_path)

    if changed:
        project_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
        project = Project.from_yaml(project_path)
        project.version = "3"
        project.to_yaml(project_path)

        repository.add(all=True)
        repository.commit("renku migrate: committing structural changes" + project_context.transaction_id)


def _migrate_broken_dataset_paths(migration_context):
    """Ensure all paths are using correct directory structure."""
    for dataset in get_project_datasets():
        if not dataset.name:
            dataset.name = generate_default_name(dataset.title, dataset.version)
        else:
            dataset.name = generate_default_name(dataset.name)

        expected_path = get_datasets_path() / dataset.identifier

        # migrate the refs
        if not is_using_temporary_datasets_path():
            ref = LinkReference.create(name="datasets/{0}".format(dataset.name), force=True)
            ref.set_reference(expected_path / OLD_METADATA_PATH)

        if not expected_path.exists():
            old_dataset_path = dataset.path
            if not is_using_temporary_datasets_path():
                expected_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(old_dataset_path, expected_path)
            else:
                expected_path.mkdir(parents=True, exist_ok=True)
                shutil.move(str(Path(old_dataset_path) / OLD_METADATA_PATH), expected_path)

        dataset.path = os.path.relpath(expected_path, project_context.path)

        if not is_using_temporary_datasets_path():
            base_path = project_context.path
        else:
            base_path = project_context.path / RENKU_HOME

        collections = [f for f in dataset.files if isinstance(f, Collection)]
        files = [f for f in dataset.files if not isinstance(f, Collection)]

        while collections:
            collection = collections.pop()
            for file in collection.members:
                if isinstance(file, Collection):
                    collections.append(file)
                else:
                    files.append(file)

        dataset.files = files

        for file in dataset.files:
            if _is_dir(migration_context=migration_context, path=file.path):
                continue
            if file.path.startswith(".."):
                file_absolute_path = os.path.abspath(get_datasets_path() / dataset.identifier / file.path)
                file.path = Path(file_absolute_path).relative_to(base_path)
            elif not _exists(migration_context=migration_context, path=file.path):
                file.path = (project_context.path / DATA_DIR / file.path).relative_to(project_context.path)

            file.name = os.path.basename(file.path)

        dataset.to_yaml(expected_path / "metadata.yml")


def _fix_labels_and_ids(migration_context):
    """Ensure files have correct label instantiation."""
    for dataset in get_project_datasets():
        dataset._id = generate_dataset_id(identifier=dataset.identifier)
        dataset._label = dataset.identifier

        for file in dataset.files:
            if not _exists(migration_context=migration_context, path=file.path):
                continue

            commit = _get_previous_commit(migration_context=migration_context, path=file.path)
            _, commit, _ = get_in_submodules(repository=project_context.repository, commit=commit, path=file.path)

            if not _is_file_id_valid(file._id, file.path, commit.hexsha):
                file._id = generate_file_id(hexsha=commit.hexsha, path=file.path)

            if not file._label or commit.hexsha not in file._label or file.path not in file._label:
                file._label = generate_label(file.path, commit.hexsha)

        dataset.to_yaml()


def _fix_dataset_urls():
    """Ensure dataset and its files have correct url format."""
    for dataset in get_project_datasets():
        dataset.url = dataset._id
        for file_ in dataset.files:
            if file_.url:
                file_.url = url_to_string(file_.url)

        dataset.to_yaml()


def _migrate_dataset_and_files_project():
    """Ensure dataset files have correct project."""
    project_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
    project = Project.from_yaml(project_path)
    if not is_using_temporary_datasets_path():
        project.to_yaml(project_path)

    for dataset in get_project_datasets():
        dataset._project = project
        if not dataset.creators:
            dataset.creators = [project.creator]
        for file_ in dataset.files:
            file_._project = project

        dataset.to_yaml()


def _is_file_id_valid(id_, path, hexsha):
    if not id_ or not isinstance(id_, str) or not id_.startswith("https"):
        return False

    u = urllib.parse.urlparse(id_)
    return u.scheme and u.netloc and u.path.startswith("/blob") and hexsha in u.path and path in u.path


def _exists(migration_context, path):
    dmc = migration_context.dataset_migration_context
    if dmc:
        return dmc.exists(path)

    path = project_context.path / path
    return path.exists() or (path.is_symlink() and os.path.lexists(path))


def _is_dir(migration_context, path):
    dmc = migration_context.dataset_migration_context
    if dmc:
        return dmc.is_dir(path)

    return (project_context.path / path).is_dir()


def _get_previous_commit(migration_context, path):
    dmc = migration_context.dataset_migration_context
    if dmc:
        return dmc.get_previous_commit(path)
    return project_context.repository.get_previous_commit(path, revision="HEAD")
