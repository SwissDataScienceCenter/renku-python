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

from functools import reduce
from uuid import uuid4

from renku.core import errors
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.metadata.repository import Repository
from renku.core.utils import communication


@inject.autoparams()
def _save_and_push(client_dispatcher: IClientDispatcher, message=None, remote=None, paths=None):
    """Save and push local changes."""
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

    if paths:
        client.track_paths_in_storage(*paths)

    return repo_sync(client.repository, message, remote, paths)


def save_and_push_command():
    """Command to save and push."""
    return Command().command(_save_and_push)


def repo_sync(repository: Repository, message=None, remote=None, paths=None):
    """Commit and push paths."""
    origin = None
    saved_paths = []

    # get branch that's pushed
    if repository.active_branch.remote_branch:
        ref = repository.active_branch.remote_branch.name
        pushed_branch = ref.split("/")[-1]
    else:
        pushed_branch = repository.active_branch.name

    if remote:
        # get/setup supplied remote for pushing
        if len(repository.remotes) > 0:
            existing = next((r for r in repository.remotes if r.url == remote), None)
            if not existing:
                existing = next((r for r in repository.remotes if r.name == remote), None)
            origin = next((r for r in repository.remotes if r.name == "origin"), None)
            if existing:
                origin = existing
            elif origin:
                pushed_branch = uuid4().hex
                origin = repository.remotes.add(name=pushed_branch, url=remote)
        if not origin:
            origin = repository.remotes.add(name="origin", url=remote)
    elif not repository.active_branch.remote_branch:
        # No remote set on branch, push to available remote if only a single one is available
        if len(repository.remotes) == 1:
            origin = repository.remotes[0]
        else:
            raise errors.ConfigurationError("No remote has been set up for the current branch")
    else:
        # get remote that's set up to track the local branch
        origin = repository.active_branch.remote_branch.remote

    if paths:
        # commit uncommitted changes
        try:
            staged_files = {c.a_path for c in repository.staged_changes} if repository.head.is_valid() else set()
            path_to_save = set(paths) - staged_files

            if path_to_save:
                repository.add(*path_to_save)

            saved_paths = [c.b_path for c in repository.staged_changes]

            if not message:
                # Show saved files in message
                max_len = 100
                message = "Saved changes to: "
                paths_with_lens = reduce(
                    lambda c, x: c + [(x, c[-1][1] + len(x))], saved_paths, [(None, len(message))]
                )[1:]
                # limit first line to max_len characters
                message += " ".join(p if l < max_len else "\n\t" + p for p, l in paths_with_lens)

            repository.commit(message)
        except errors.GitCommandError as e:
            raise errors.GitError("Cannot commit changes") from e

    try:
        # NOTE: Push local changes to remote branch.
        merge_conflict = False
        if len(origin.references) > 0 and repository.active_branch.remote_branch in origin.references:
            repository.fetch("origin")
            try:
                repository.pull("origin", repository.active_branch)
            except errors.GitCommandError:
                # NOTE: Couldn't pull, probably due to conflicts, try a merge.
                # NOTE: the error sadly doesn't tell any details.
                unmerged_blobs = repository.unmerged_blobs.values()
                conflicts = (stage != 0 for blobs in unmerged_blobs for stage, _ in blobs)
                if any(conflicts):
                    merge_conflict = True

                    if communication.confirm(
                        "There were conflicts when updating the local data with remote changes,"
                        " do you want to resolve them (if not, a new remote branch will be created)?",
                        warning=True,
                    ):
                        repository.run_git_command("mergetool", "-g")
                        repository.commit("merging conflict", no_edit=True)
                        merge_conflict = False
                    else:
                        repository.reset(hard=True)
                else:
                    raise

        result = None
        failed_push = None

        if not merge_conflict:
            result = repository.push(origin, repository.active_branch)
            failed_push = [r for r in result if r.failed]

        if merge_conflict or (result and "[remote rejected] (pre-receive hook declined)" in result[0].summary):
            # NOTE: Push to new remote branch if original one is protected and reset the cache.
            old_pushed_branch = pushed_branch
            old_active_branch = repository.active_branch
            pushed_branch = uuid4().hex
            try:
                repository.branches.add(pushed_branch)
                result = repository.push("origin", pushed_branch)
                failed_push = [r for r in result if r.failed]
            finally:
                # Reset cache
                repository.checkout(old_active_branch)
                repository.reset(reference=f"{origin}/{old_pushed_branch}", hard=True)

        if result and failed_push:
            # NOTE: Couldn't push for some reason
            msg = "\n".join(info.summary for info in failed_push)
            raise errors.GitError(f"Couldn't push changes. Reason:\n{msg}")

    except errors.GitCommandError as e:
        raise errors.GitError("Cannot push changes") from e

    return saved_paths, pushed_branch
