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
"""Renku save commands."""

from renku.core import errors
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher


@inject.autoparams()
def _save_and_push(client_dispatcher: IClientDispatcher, message=None, remote=None, paths=None):
    """Save and push local changes."""
    from renku.core.utils.git import commit_changes, get_remote, push_changes

    client = client_dispatcher.current_client

    client.setup_credential_helper()
    if not paths:
        paths = client.dirty_paths
    else:
        staged_changes = client.repository.staged_changes
        if staged_changes:
            staged_paths = {c.a_path for c in staged_changes}
            not_passed = staged_paths - set(paths)

            if not_passed:
                raise errors.RenkuSaveError(
                    "These files are in the git staging area, but weren't passed to renku save. Unstage them or pass"
                    + " them explicitly: \n"
                    + "\n".join(not_passed)
                )

    # NOTE: Check if a remote is setup for the repository
    if not remote:
        default_remote = get_remote(client.repository)
        if not default_remote:
            raise errors.GitError("No remote has been set up for the current branch")

    if paths:
        client.track_paths_in_storage(*paths)
        paths = commit_changes(*paths, repository=client.repository, message=message)

    branch = push_changes(repository=client.repository, remote=remote)

    return paths, branch


def save_and_push_command():
    """Command to save and push."""
    return Command().command(_save_and_push)
