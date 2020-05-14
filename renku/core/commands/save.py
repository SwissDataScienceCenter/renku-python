# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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

from uuid import uuid4

import git

from renku.core import errors

from .client import pass_local_client


@pass_local_client
def save_and_push(client, message, remote=None, paths=None):
    """Save and push local changes."""
    client.setup_credential_helper()
    if not paths:
        paths = client.dirty_paths

    origin = None

    if remote:
        if client.repo.remotes:
            existing = next(
                (r for r in client.repo.remotes if r.url == remote), None
            )
            origin = next(
                (r for r in client.repo.remotes if r.name == 'origin'), None
            )
            if existing:
                origin = existing
            elif origin:
                origin = client.repo.create_remote(str(uuid4()), remote)
        if not origin:
            origin = client.repo.create_remote('origin', remote)
    elif not client.repo.active_branch.tracking_branch():
        if len(client.repo.remotes) == 1:
            origin = client.repo.remotes[0]
        else:
            raise errors.ConfigurationError(
                'No remote has been set up for the current branch'
            )
    else:
        origin = client.repo.remotes[
            client.repo.active_branch.tracking_branch().remote_name]

    try:
        client.track_paths_in_storage(*paths)
        client.repo.git.add(*paths)
        saved_paths = [d.b_path for d in client.repo.index.diff('HEAD')]
        client.repo.index.commit(message)
    except git.exc.GitCommandError as e:
        raise errors.GitError('Cannot commit changes') from e

    try:
        origin.fetch()

        if origin.refs and client.repo.active_branch in origin.refs:
            origin.pull(client.repo.active_branch)

        origin.push(client.repo.active_branch)
    except git.exc.GitCommandError as e:
        raise errors.GitError('Cannot push changes') from e

    return saved_paths
