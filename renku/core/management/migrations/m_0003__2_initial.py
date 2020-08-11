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
import uuid
from pathlib import Path

from renku.core.management.migrations.models.v3 import Dataset, Project, get_client_datasets
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.datasets import generate_dataset_id
from renku.core.models.entities import generate_file_id, generate_label
from renku.core.models.refs import LinkReference
from renku.core.utils.migrate import get_pre_0_3_4_datasets_metadata
from renku.core.utils.urls import url_to_string


def migrate(client):
    """Migration function."""
    _ensure_clean_lock(client)
    _do_not_track_lock_file(client)
    _migrate_datasets_pre_v0_3(client)
    _migrate_broken_dataset_paths(client)
    _fix_labels_and_ids(client)
    _fix_dataset_urls(client)
    _migrate_dataset_and_files_project(client)


def _ensure_clean_lock(client):
    """Make sure Renku lock file is not part of repository."""
    lock_file = client.path / ".renku.lock"
    try:
        lock_file.unlink()
    except FileNotFoundError:
        pass


def _do_not_track_lock_file(client):
    """Add lock file to .gitingore if not already exists."""
    # Add lock file to .gitignore.
    lock_file = ".renku.lock"
    gitignore = client.path / ".gitignore"
    if lock_file not in gitignore.read_text():
        gitignore.open("a").write("\n{0}\n".format(lock_file))


def _migrate_datasets_pre_v0_3(client):
    """Migrate datasets from Renku 0.3.x."""

    for old_path in get_pre_0_3_4_datasets_metadata(client):
        name = str(old_path.parent.relative_to(client.path / DATA_DIR))

        dataset = Dataset.from_yaml(old_path, client=client)
        dataset.title = name
        dataset.name = name
        new_path = client.renku_datasets_path / dataset.identifier / client.METADATA
        new_path.parent.mkdir(parents=True, exist_ok=True)

        with client.with_metadata(read_only=True) as meta:
            for module in client.repo.submodules:
                if Path(module.url).name == meta.name:
                    module.remove()

        for file_ in dataset.files:
            if not Path(file_.path).exists():
                expected_path = client.path / DATA_DIR / dataset.name / file_.path
                if expected_path.exists():
                    file_.path = expected_path.relative_to(client.path)

        dataset.to_yaml(new_path)

        Path(old_path).unlink()
        ref = LinkReference.create(client=client, name="datasets/{0}".format(name), force=True,)
        ref.set_reference(new_path)


def _migrate_broken_dataset_paths(client):
    """Ensure all paths are using correct directory structure."""
    for dataset in get_client_datasets(client):
        expected_path = client.renku_datasets_path / dataset.identifier
        if not dataset.name:
            dataset.name = dataset.title

        # migrate the refs
        ref = LinkReference.create(client=client, name="datasets/{0}".format(dataset.name), force=True,)
        ref.set_reference(expected_path / client.METADATA)

        old_dataset_path = client.renku_datasets_path / uuid.UUID(dataset.identifier).hex

        dataset.path = os.path.relpath(expected_path, client.path)

        if not expected_path.exists():
            shutil.move(old_dataset_path, expected_path)

        for file_ in dataset.files:
            file_path = Path(file_.path)
            if not file_path.exists() or file_.path.startswith(".."):
                new_path = Path(
                    os.path.abspath(client.renku_datasets_path / dataset.identifier / file_path)
                ).relative_to(client.path)

                file_.path = new_path

            file_.name = os.path.basename(file_.path)

        dataset.to_yaml(expected_path / client.METADATA)


def _fix_labels_and_ids(client):
    """Ensure files have correct label instantiation."""
    for dataset in get_client_datasets(client):
        dataset._id = generate_dataset_id(client=client, identifier=dataset.identifier)
        dataset._label = dataset.identifier

        for file_ in dataset.files:
            _, commit, _ = client.resolve_in_submodules(
                client.find_previous_commit(file_.path, revision="HEAD"), file_.path,
            )

            if not _is_file_id_valid(file_._id, file_.path, commit.hexsha):
                file_._id = generate_file_id(client=client, hexsha=commit.hexsha, path=file_.path)

            if not file_._label or commit.hexsha not in file_._label or file_.path not in file_._label:
                file_._label = generate_label(file_.path, commit.hexsha)

        dataset.to_yaml()


def _fix_dataset_urls(client):
    """Ensure dataset and its files have correct url format."""
    for dataset in get_client_datasets(client):
        dataset.url = dataset._id
        for file_ in dataset.files:
            if file_.url:
                file_.url = url_to_string(file_.url)

        dataset.to_yaml()


def _migrate_dataset_and_files_project(client):
    """Ensure dataset files have correct project."""
    project = Project.from_yaml(client.renku_metadata_path, client)
    project.to_yaml(client.renku_metadata_path)

    for dataset in get_client_datasets(client):
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
