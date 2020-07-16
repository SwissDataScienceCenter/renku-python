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
from pathlib import Path, posixpath
from urllib.parse import quote

from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.datasets import Dataset
from renku.core.models.refs import LinkReference
from renku.core.utils.urls import url_to_string


def migrate(client):
    """Migration function."""
    _ensure_clean_lock(client)
    _do_not_track_lock_file(client)
    _migrate_datasets_pre_v0_3(client)
    _migrate_broken_dataset_paths(client)
    _fix_uncommitted_labels(client)
    _fix_dataset_files_urls(client)
    _fix_broken_dataset_file_project(client)
    _dataset_file_id_migration(client)
    _migrate_files_project(client)


def _ensure_clean_lock(client):
    """Make sure Renku lock file is not part of repository."""
    lock_file = client.path / '.renku.lock'
    try:
        lock_file.unlink()
    except FileNotFoundError:
        pass


def _do_not_track_lock_file(client):
    """Add lock file to .gitingore if not already exists."""
    # Add lock file to .gitignore.
    lock_file = '.renku.lock'
    gitignore = client.path / '.gitignore'
    if lock_file not in gitignore.read_text():
        gitignore.open('a').write('\n{0}\n'.format(lock_file))


def _migrate_datasets_pre_v0_3(client):
    """Migrate datasets from Renku 0.3.x."""

    def _dataset_pre_0_3(client):
        """Return paths of dataset metadata for pre 0.3.4."""
        project_is_pre_0_3 = int(client.project.version) < 2
        if project_is_pre_0_3:
            return (client.path / DATA_DIR).rglob(client.METADATA)
        return []

    for old_path in _dataset_pre_0_3(client):
        name = str(old_path.parent.relative_to(client.path / DATA_DIR))

        dataset = Dataset.from_yaml(old_path, client=client)
        new_path = (client.renku_datasets_path / dataset.uid / client.METADATA)
        new_path.parent.mkdir(parents=True, exist_ok=True)

        with client.with_metadata(read_only=True) as meta:
            for module in client.repo.submodules:
                if Path(module.url).name == meta.name:
                    module.remove()

        for file_ in dataset.files:
            if not Path(file_.path).exists():
                expected_path = (
                    client.path / DATA_DIR / dataset.name / file_.path
                )
                if expected_path.exists():
                    file_.path = expected_path.relative_to(client.path)

        dataset.__reference__ = new_path.relative_to(client.path)
        dataset.to_yaml()

        Path(old_path).unlink()
        ref = LinkReference.create(
            client=client,
            name='datasets/{0}'.format(name),
            force=True,
        )
        ref.set_reference(new_path)


def _migrate_broken_dataset_paths(client):
    """Ensure all paths are using correct directory structure."""
    for dataset in client.datasets.values():
        dataset_path = client.path / dataset.path

        expected_path = (
            client.renku_datasets_path /
            Path(quote(dataset.identifier, safe=''))
        )

        # migrate the refs
        ref = LinkReference.create(
            client=client,
            name='datasets/{0}'.format(dataset.name),
            force=True,
        )
        ref.set_reference(expected_path / client.METADATA)

        if not dataset_path.exists():
            dataset_path = (
                client.renku_datasets_path / uuid.UUID(dataset.identifier).hex
            )

        if not expected_path.exists():
            shutil.move(dataset_path, expected_path)
            dataset.path = expected_path
            dataset.__reference__ = expected_path / client.METADATA

        for file_ in dataset.files:
            file_path = Path(file_.path)
            if not file_path.exists() and file_.path.startswith('..'):
                new_path = (
                    client.renku_datasets_path / dataset.uid / file_path
                ).resolve().relative_to(client.path)

                file_.path = new_path

                _, commit, _ = client.resolve_in_submodules(
                    client.find_previous_commit(file_.path, revision='HEAD'),
                    file_.path,
                )
                host = client.remote.get('host') or 'localhost'
                host = os.environ.get('RENKU_DOMAIN') or host

                # always set the id by the identifier
                file_._id = urllib.parse.urljoin(
                    'https://{host}'.format(host=host),
                    posixpath.join(
                        '/blob/{hexsha}/{path}'.format(
                            hexsha=commit.hexsha, path=new_path
                        )
                    )
                )
                file_._label = '{}@{}'.format(new_path, commit.hexsha)

        dataset.to_yaml()


def _fix_uncommitted_labels(client):
    """Ensure files have correct label instantiation."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            try:
                _, commit, _ = client.resolve_in_submodules(
                    client.find_previous_commit(file_.path, revision='HEAD'),
                    file_.path,
                )
                file_.commit = commit
                if (
                    not file_._label or 'UNCOMMITTED' in file_._label or
                    '@' not in file_._label
                ):
                    file_._label = file_.default_label()
                    file_._id = file_.default_id()
            except KeyError:
                pass
        dataset.to_yaml()


def _fix_dataset_files_urls(client):
    """Ensure dataset files have correct url format."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            if file_.url:
                file_.url = url_to_string(file_.url)

        dataset.to_yaml()


def _fix_broken_dataset_file_project(client):
    """Ensure project is correctly set on ``DatasetFile``."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            if not file_._project or 'NULL/NULL' in file_._project._id:
                file_._project = client.project

        dataset.to_yaml()


def _dataset_file_id_migration(client):
    """Ensure dataset files have a fully qualified url."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            if not file_._id.startswith('https'):
                file_._id = file_.default_id()

        dataset.to_yaml()


def _migrate_files_project(client):
    """Ensure dataset files have correct project."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            file_._project = dataset._project

        dataset.to_yaml()
