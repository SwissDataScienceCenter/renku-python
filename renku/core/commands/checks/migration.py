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
"""Checks needed to determine dataset migration policy."""
import shutil
import uuid
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import click

from renku.core.models.datasets import Dataset
from renku.core.models.refs import LinkReference
from renku.core.utils.urls import url_to_string

from ..echo import WARNING


def dataset_pre_0_3(client):
    """Return paths of dataset metadata for pre 0.3.4."""
    project_is_pre_0_3 = int(client.project.version) < 2
    if project_is_pre_0_3:
        return (client.path / 'data').rglob(client.METADATA)
    return []


def check_dataset_metadata(client):
    """Check location of dataset metadata."""
    # Find pre 0.3.4 metadata files.
    old_metadata = list(dataset_pre_0_3(client))

    if not old_metadata:
        return True, None

    problems = (
        WARNING + 'There are metadata files in the old location.'
        '\n  (use "renku migrate datasets" to move them)\n\n\t' + '\n\t'.join(
            click.style(str(path.relative_to(client.path)), fg='yellow')
            for path in old_metadata
        ) + '\n'
    )

    return False, problems


def check_missing_files(client):
    """Find missing files listed in datasets."""
    missing = defaultdict(list)

    for path, dataset in client.datasets.items():
        for file_ in dataset.files:
            filepath = Path(file_.path)
            if not filepath.exists():
                missing[str(
                    path.parent.relative_to(client.renku_datasets_path)
                )].append(str(filepath))

    if not missing:
        return True, None

    problems = (WARNING + 'There are missing files in datasets.')

    for dataset, files in missing.items():
        problems += (
            '\n\t' + click.style(dataset, fg='yellow') + ':\n\t  ' +
            '\n\t  '.join(click.style(path, fg='red') for path in files)
        )

    return False, problems


def check_dataset_resources(client):
    """Find missing datasets or files listed in metadata."""
    missing_datasets = defaultdict(list)
    missing_files = defaultdict(list)

    for path, dataset in client.datasets.items():
        metadata_path = Path(dataset.path) / client.METADATA
        expected_path = str(client.renku_datasets_path / dataset.identifier)

        if not metadata_path.exists() or expected_path != dataset.path:
            missing_datasets[path] = dataset

        for file_ in dataset.files:
            filepath = Path(file_.path)
            if not filepath.exists():
                relative = path.parent.relative_to(client.renku_datasets_path)
                missing_files[str(relative)].append(str(filepath))

    return missing_datasets, missing_files


def ensure_clean_lock(client):
    """Make sure Renku lock file is not part of repository."""
    lock_file = client.path / '.renku.lock'
    if lock_file.exists():
        lock_file.unlink()

    # Add lock file to .gitignore.
    gitignore = client.path / '.gitignore'
    if str(lock_file.name) not in gitignore.read_text():
        gitignore.open('a').write('\n{0}\n'.format(lock_file.name))


def migrate_datasets_pre_v0_3(client):
    """Migrate datasets from Renku 0.3.x."""
    for old_path in dataset_pre_0_3(client):
        name = str(old_path.parent.relative_to(client.path / 'data'))

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
                    client.path / 'data' / dataset.name / file_.path
                )
                if expected_path.exists():
                    file_.path = expected_path.relative_to(client.path)

        dataset.__reference__ = new_path
        dataset.to_yaml()

        Path(old_path).unlink()
        ref = LinkReference.create(
            client=client,
            name='datasets/{0}'.format(name),
            force=True,
        )
        ref.set_reference(new_path)


def migrate_broken_dataset_paths(client):
    """Ensure all paths are using correct directory structure."""
    for dataset in client.datasets.values():
        dataset_path = Path(dataset.path)

        expected_path = (
            client.renku_datasets_path /
            Path(quote(dataset.identifier, safe=''))
        )

        # migrate the refs
        ref = LinkReference.create(
            client=client,
            name='datasets/{0}'.format(dataset.short_name),
            force=True,
        )
        ref.set_reference(expected_path / client.METADATA)

        if not dataset_path.exists():
            dataset_path = (
                client.renku_datasets_path / uuid.UUID(dataset.identifier).hex
            )

        if not expected_path.exists():
            shutil.move(str(dataset_path), str(expected_path))
            dataset.path = expected_path
            dataset.__reference__ = expected_path / client.METADATA

        for file_ in dataset.files:
            file_path = Path(file_.path)
            if not file_path.exists() and file_.path.startswith('..'):
                new_path = (
                    client.renku_datasets_path / dataset.uid / file_path
                ).resolve().relative_to(client.path)

                file_.path = new_path
                file_._label = new_path

                _, commit, _ = client.resolve_in_submodules(
                    client.find_previous_commit(file_.path, revision='HEAD'),
                    file_.path,
                )
                id_format = 'blob/{commit}/{path}'
                file_._id = id_format.format(
                    commit=commit.hexsha, path=new_path
                )

        dataset.to_yaml()


def dataset_file_path_migration(client, dataset, file_):
    """Migrate a DatasetFile file path."""
    file_path = Path(file_.path)
    if not file_path.exists():
        # old-style migrated paths relative to dataset root
        if file_.path.startswith('..'):
            new_path = (client.renku_datasets_path / dataset.uid /
                        file_path).resolve().relative_to(client.path)
        else:
            new_path = ((
                client.path / client.datadir / dataset.name / file_path
            ).relative_to(client.path))
        file_.path = new_path

        _, commit, _ = client.resolve_in_submodules(
            client.find_previous_commit(file_.path, revision='HEAD'),
            file_.path,
        )
    file_._id = file_.default_id()
    file_._label = file_.default_label()


def fix_broken_dataset_file_project(client):
    """Ensure project is correctly set on ``DatasetFile``."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            if not file_._project or 'NULL/NULL' in file_._project._id:
                file_._project = client.project

        dataset.to_yaml()


def fix_uncommitted_labels(client):
    """Ensure files have correct label instantiation."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            _, commit, _ = client.resolve_in_submodules(
                client.find_previous_commit(file_.path, revision='HEAD'),
                file_.path,
            )
            file_.commit = commit
            if 'UNCOMMITTED' in file_._label:
                file_._label = file_.default_label()
                file_._id = file_.default_id()
        dataset.to_yaml()


def fix_dataset_files_urls(client):
    """Ensure dataset files have correct url format."""
    for dataset in client.datasets.values():
        for file_ in dataset.files:
            if file_.url:
                file_.url = url_to_string(file_.url)

        dataset.to_yaml()


STRUCTURE_MIGRATIONS = [
    ensure_clean_lock,
    migrate_datasets_pre_v0_3,
    migrate_broken_dataset_paths,
    fix_uncommitted_labels,
    fix_dataset_files_urls,
    fix_broken_dataset_file_project,
]
