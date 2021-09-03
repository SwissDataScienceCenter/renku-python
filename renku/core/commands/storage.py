# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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

from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.utils import communication


@inject.autoparams()
def _check_lfs(client_dispatcher: IClientDispatcher, everything=False):
    """Check if large files are not in lfs."""
    files = client_dispatcher.current_client.check_lfs_migrate_info(everything)

    if files:
        communication.warn("Git history contains large files\n\t" + "\n\t".join(files))

    return files


def check_lfs_command():
    """Check lfs command."""
    return Command().command(_check_lfs)


@inject.autoparams()
def _fix_lfs(paths, client_dispatcher: IClientDispatcher):
    """Migrate large files into lfs."""
    client_dispatcher.current_client.migrate_files_to_lfs(paths)


def fix_lfs_command():
    """Fix lfs command."""
    return Command().command(_fix_lfs).require_clean().with_database(write=True).with_commit(commit_if_empty=False)


@inject.autoparams()
def _pull(paths, client_dispatcher: IClientDispatcher):
    """Pull the specified paths from external storage."""
    client_dispatcher.current_client.pull_paths_from_storage(*paths)


def pull_command():
    """Command to pull the specified paths from external storage."""
    return Command().command(_pull)


@inject.autoparams()
def _clean(paths, client_dispatcher: IClientDispatcher):
    """Remove files from lfs cache/turn them back into pointer files."""
    untracked_paths, local_only_paths = client_dispatcher.current_client.clean_storage_cache(*paths)

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


@inject.autoparams()
def _check_lfs_hook(paths, client_dispatcher: IClientDispatcher):
    """Pull the specified paths from external storage."""
    return client_dispatcher.current_client.check_requires_tracking(*paths)


def check_lfs_hook_command():
    """Command to pull the specified paths from external storage."""
    return Command().command(_check_lfs_hook)
