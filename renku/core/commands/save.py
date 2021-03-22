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

import git

from renku.core import errors
from renku.core.incubation.command import Command
from renku.core.utils import communication
from renku.core.utils.git import add_to_git
from renku.core.utils.scm import git_unicode_unescape


def _save_and_push(client, message=None, remote=None, paths=None):
    """Save and push local changes."""
    client.setup_credential_helper()
    if not paths:
        paths = client.dirty_paths
    else:
        staged = client.repo.index.diff("HEAD")
        if staged:
            staged = {git_unicode_unescape(p.a_path) for p in staged}
            not_passed = staged - set(paths)

            if not_passed:
                raise errors.RenkuSaveError(
                    "These files are in the git staging area, but weren't passed to renku save. Unstage them or pass"
                    + " them explicitly: \n"
                    + "\n".join(not_passed)
                )

    if paths:
        client.track_paths_in_storage(*paths)

    return repo_sync(client.repo, message, remote, paths)


def save_and_push_command():
    """Command to save and push."""
    return Command().command(_save_and_push)


def repo_sync(repo, message=None, remote=None, paths=None):
    """Commit and push paths."""
    origin = None
    saved_paths = []

    # get branch that's pushed
    if repo.active_branch.tracking_branch():
        ref = repo.active_branch.tracking_branch().name
        pushed_branch = ref.split("/")[-1]
    else:
        pushed_branch = repo.active_branch.name

    if remote:
        # get/setup supplied remote for pushing
        if repo.remotes:
            existing = next((r for r in repo.remotes if r.url == remote), None)
            if not existing:
                existing = next((r for r in repo.remotes if r.name == remote), None)
            origin = next((r for r in repo.remotes if r.name == "origin"), None)
            if existing:
                origin = existing
            elif origin:
                pushed_branch = uuid4().hex
                origin = repo.create_remote(pushed_branch, remote)
        if not origin:
            origin = repo.create_remote("origin", remote)
    elif not repo.active_branch.tracking_branch():
        # No remote set on branch, push to available remote if only a single
        # one is available
        if len(repo.remotes) == 1:
            origin = repo.remotes[0]
        else:
            raise errors.ConfigurationError("No remote has been set up for the current branch")
    else:
        # get remote that's set up to track the local branch
        origin = repo.remotes[repo.active_branch.tracking_branch().remote_name]

    if paths:
        # commit uncommitted changes
        try:
            staged_files = (
                {git_unicode_unescape(d.a_path) for d in repo.index.diff("HEAD")} if repo.head.is_valid() else set()
            )

            path_to_save = set(paths) - staged_files

            if path_to_save:
                add_to_git(repo.git, *path_to_save)

            saved_paths = [d.b_path for d in repo.index.diff("HEAD")]

            if not message:
                # Show saved files in message
                max_len = 100
                message = "Saved changes to: "
                paths_with_lens = reduce(
                    lambda c, x: c + [(x, c[-1][1] + len(x))], saved_paths, [(None, len(message))]
                )[1:]
                # limit first line to max_len characters
                message += " ".join(p if l < max_len else "\n\t" + p for p, l in paths_with_lens)

            repo.index.commit(message)
        except git.exc.GitCommandError as e:
            raise errors.GitError("Cannot commit changes") from e

    try:
        # NOTE: Push local changes to remote branch.
        merge_conflict = False
        if origin.refs and repo.active_branch.tracking_branch() and repo.active_branch.tracking_branch() in origin.refs:
            origin.fetch()
            try:
                origin.pull(repo.active_branch)
            except git.exc.GitCommandError:
                # NOTE: Couldn't pull, probably due to conflicts, try a merge.
                # NOTE: the error sadly doesn't tell any details.
                unmerged_blobs = repo.index.unmerged_blobs().values()
                conflicts = (stage != 0 for blobs in unmerged_blobs for stage, _ in blobs)
                if any(conflicts):
                    merge_conflict = True

                    if communication.confirm(
                        "There were conflicts when updating the local data with remote changes,"
                        " do you want to resolve them (if not, a new remote branch will be created)?",
                        warning=True,
                    ):
                        repo.git.mergetool("-g")
                        repo.git.commit("--no-edit")
                        merge_conflict = False
                    else:
                        repo.head.reset(index=True, working_tree=True)
                else:
                    raise

        result = None
        failed_push = None

        if not merge_conflict:
            result = origin.push(repo.active_branch)
            failed_push = [
                push_info
                for push_info in result
                if push_info.flags & git.PushInfo.ERROR
                or push_info.flags & git.PushInfo.REJECTED
                or push_info.flags & git.PushInfo.REMOTE_REJECTED
                or push_info.flags & git.PushInfo.REMOTE_FAILURE
            ]

        if merge_conflict or (result and "[remote rejected] (pre-receive hook declined)" in result[0].summary):
            # NOTE: Push to new remote branch if original one is protected and reset the cache.
            old_pushed_branch = pushed_branch
            old_active_branch = repo.active_branch
            pushed_branch = uuid4().hex
            try:
                repo.create_head(pushed_branch)
                result = repo.remote().push(pushed_branch)
                failed_push = [
                    push_info
                    for push_info in result
                    if push_info.flags & git.PushInfo.ERROR
                    or push_info.flags & git.PushInfo.REJECTED
                    or push_info.flags & git.PushInfo.REMOTE_REJECTED
                    or push_info.flags & git.PushInfo.REMOTE_FAILURE
                ]
            finally:
                # Reset cache
                repo.git.checkout(old_active_branch)
                ref = f"{origin}/{old_pushed_branch}"
                repo.index.reset(commit=ref, head=True, working_tree=True)

        if result and failed_push:
            # NOTE: Couldn't push for some reason
            msg = "\n".join(info.summary for info in failed_push)
            raise errors.GitError(f"Couldn't push changes. Reason:\n{msg}")

    except git.exc.GitCommandError as e:
        raise errors.GitError("Cannot push changes") from e

    return saved_paths, pushed_branch
