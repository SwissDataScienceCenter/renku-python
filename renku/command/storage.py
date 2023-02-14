# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Renku storage command."""

from typing import List

from pydantic import validate_arguments

from renku.command.command_builder.command import Command
from renku.core.storage import (
    check_lfs_migrate_info,
    check_requires_tracking,
    clean_storage_cache,
    migrate_files_to_lfs,
    pull_paths_from_storage,
)
from renku.core.util import communication
from renku.domain_model.project_context import project_context


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _check_lfs(everything: bool = False):
    """Check if large files are not in lfs.

    Args:
        everything: Whether to check whole history (Default value = False).

    Returns:
        List of large files.
    """
    files = check_lfs_migrate_info(everything)

    if files:
        communication.warn("Git history contains large files\n\t" + "\n\t".join(files))

    return files


def check_lfs_command():
    """Check lfs command."""
    return Command().command(_check_lfs)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _fix_lfs(paths: List[str]):
    """Migrate large files into lfs.

    Args:
        paths(List[str]): Paths to migrate to LFS.
    """
    migrate_files_to_lfs(paths)


def fix_lfs_command():
    """Fix lfs command."""
    return (
        Command()
        .command(_fix_lfs)
        .require_clean()
        .require_migration()
        .with_database(write=True)
        .with_commit(commit_if_empty=False)
    )


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _pull(paths: List[str]):
    """Pull the specified paths from external storage.

    Args:
        paths(List[str]): Paths to pull from LFS.
    """
    pull_paths_from_storage(project_context.repository, *paths)


def pull_command():
    """Command to pull the specified paths from external storage."""
    return Command().command(_pull)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _clean(paths: List[str]):
    """Remove files from lfs cache/turn them back into pointer files.

    Args:
        paths:List[str]: Paths to turn back to pointer files.
    """
    untracked_paths, local_only_paths = clean_storage_cache(*paths)

    if untracked_paths:
        communication.warn(
            "These paths were ignored as they are not tracked"
            + " in git LFS:\n\t{}\n".format("\n\t".join(untracked_paths))
        )

    if local_only_paths:
        communication.warn(
            "These paths were ignored as they are not pushed to "
            + "a remote with git LFS:\n\t{}\n".format("\n\t".join(local_only_paths))
        )


def clean_command():
    """Command to remove files from lfs cache/turn them back into pointer files."""
    return Command().command(_clean)


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _check_lfs_hook(paths: List[str]):
    """Check if paths should be in LFS.

    Args:
        paths(List[str]): Paths to check

    Returns:
        List of files that should be in LFS.
    """
    return check_requires_tracking(*paths)


def check_lfs_hook_command():
    """Command to pull the specified paths from external storage."""
    return Command().command(_check_lfs_hook)
