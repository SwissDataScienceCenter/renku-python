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
"""Renku save commands."""

from typing import List, Optional, Tuple

from pydantic import validate_arguments

from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.storage import track_paths_in_storage
from renku.domain_model.project_context import project_context


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _save_and_push(
    message: Optional[str] = None, remote: Optional[str] = None, paths: Optional[List[str]] = None
) -> Tuple[List[str], str]:
    """Save and push local changes.

    Args:
        message(Optional[str]): The commit message (Default value = None).
        remote(Optional[str]): The remote to push to (Default value = None).
        paths(Optional[List[str]]): The paths to include in the commit (Default value = None).

    Returns:
        Tuple[List[str], str]: Tuple of paths that were committed and branch that was pushed.
    """
    from renku.core.util.git import commit_changes, get_dirty_paths, get_remote, push_changes

    repository = project_context.repository

    setup_credential_helper()

    if not paths:
        paths = list(get_dirty_paths(repository))
    else:
        staged_changes = repository.staged_changes
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
        default_remote = get_remote(repository)
        if not default_remote:
            raise errors.GitError("No remote has been set up for the current branch")

    if paths:
        track_paths_in_storage(*paths)
        paths = commit_changes(*paths, repository=repository, message=message)

    branch = push_changes(repository=repository, remote=remote)

    return paths, branch


def setup_credential_helper():
    """Setup git credential helper to ``cache`` if not set already."""
    repository = project_context.repository

    credential_helper = repository.get_configuration().get_value("credential", "helper", "")

    if not credential_helper:
        with repository.get_configuration(writable=True) as w:
            w.set_value("credential", "helper", "cache")


def save_and_push_command():
    """Command to save and push."""
    return Command().command(_save_and_push)
