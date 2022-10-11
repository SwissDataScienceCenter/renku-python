# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Repository datasets management."""

from renku.command.command_builder.command import Command
from renku.core.constant import CONFIG_LOCAL_PATH, DATASET_METADATA_PATHS
from renku.core.dataset.dataset import (
    create_dataset,
    edit_dataset,
    export_dataset,
    file_unlink,
    import_dataset,
    list_dataset_files,
    list_datasets,
    mount_external_storage,
    pull_external_data,
    remove_dataset,
    search_datasets,
    show_dataset,
    unmount_external_storage,
    update_datasets,
)
from renku.core.dataset.dataset_add import add_to_dataset
from renku.core.dataset.tag import add_dataset_tag, list_dataset_tags, remove_dataset_tags


def search_datasets_command():
    """Command to get all the datasets whose name starts with the given string."""
    return Command().command(search_datasets).require_migration().with_database()


def list_datasets_command():
    """Command for listing datasets."""
    return Command().command(list_datasets).with_database().require_migration()


def create_dataset_command():
    """Return a command for creating an empty dataset in the current repository."""
    command = Command().command(create_dataset).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def edit_dataset_command():
    """Command for editing dataset metadata."""
    command = Command().command(edit_dataset).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def show_dataset_command():
    """Command for showing detailed dataset information."""
    return Command().command(show_dataset).with_database().require_migration()


def add_to_dataset_command():
    """Create a command for adding data to datasets."""
    command = Command().command(add_to_dataset).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(raise_if_empty=True, commit_only=DATASET_METADATA_PATHS)


def list_files_command():
    """Command for listing dataset files."""
    return Command().command(list_dataset_files).with_database().require_migration()


def file_unlink_command():
    """Command for removing matching files from a dataset."""
    command = Command().command(file_unlink).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def remove_dataset_command():
    """Command for deleting a dataset."""
    command = Command().command(remove_dataset).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def export_dataset_command():
    """Command for exporting a dataset to 3rd party provider."""
    command = Command().command(export_dataset).with_database()
    return command.require_migration().require_clean()


def import_dataset_command():
    """Create a command for importing datasets."""
    command = Command().command(import_dataset).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def update_datasets_command(dry_run=False):
    """Command for updating datasets."""
    command = Command().command(update_datasets).lock_dataset().with_database(write=not dry_run).require_migration()

    if not dry_run:
        command = command.with_commit(commit_only=DATASET_METADATA_PATHS)

    return command


def add_dataset_tag_command():
    """Command for creating a new tag for a dataset."""
    command = Command().command(add_dataset_tag).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def remove_dataset_tags_command():
    """Command for removing tags from a dataset."""
    command = Command().command(remove_dataset_tags).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def list_tags_command():
    """Command for listing a dataset's tags."""
    return Command().command(list_dataset_tags).with_database().require_migration()


def pull_external_data_command():
    """Command for pulling/copying data from an external storage."""
    command = Command().command(pull_external_data).lock_dataset().with_database(write=True)
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS + [CONFIG_LOCAL_PATH])


def mount_external_storage_command(unmount: bool):
    """Command for mounting an external storage."""
    command = unmount_external_storage if unmount else mount_external_storage
    return Command().command(command).lock_dataset().with_database(write=False).require_migration()
